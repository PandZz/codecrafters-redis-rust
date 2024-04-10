// Uncomment this block to pass the first stage
use anyhow::{anyhow, Result};
use redis_starter_rust::{cmd::Cmd, frame::RESP, Config};
use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

type ShardedDb = Arc<Vec<Mutex<HashMap<String, (RESP, u128)>>>>;

static EMPTY_RDB_HEX: &'static str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

fn hash(s: &str) -> usize {
    const MOD: usize = 1e9 as usize + 7;
    const P: usize = 26;
    s.chars().fold(0, |acc, x| (acc * P + x as usize) % MOD)
}

fn new_sharded_db(num_shards: usize) -> ShardedDb {
    let mut db = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }
    Arc::new(db)
}

fn hex2bytes(src: &str) -> Vec<u8> {
    let mut hex_bytes: Vec<_> = src
        .as_bytes()
        .iter()
        .map(|&c| {
            if b'0' <= c && c <= b'9' {
                c - b'0'
            } else {
                c - b'a' + 10
            }
        })
        .collect();
    if hex_bytes.len() % 2 != 0 {
        hex_bytes.push(0);
    }
    hex_bytes
        .chunks(2)
        .map(|pair| match pair.len() {
            2 => pair[0] * 16 + pair[1],
            1 => pair[0] * 16,
            _ => 0,
        })
        .collect()
}

async fn handle_client(mut stream: TcpStream, db: ShardedDb, config: Arc<Mutex<Config>>) {
    let resp_null_bytes = RESP::Null.to_string();
    let resp_null_bytes = resp_null_bytes.as_bytes();
    let empty_rdb_bytes = hex2bytes(EMPTY_RDB_HEX);
    let front = format!("${}\r\n", empty_rdb_bytes.len());
    let empty_rdb_bytes = vec![front.into_bytes(), empty_rdb_bytes].concat();
    let mut buf = [0; 512];
    loop {
        let count = stream.read(&mut buf).await.unwrap();
        if count == 0 {
            break;
        }
        if let Some((_, resp)) = RESP::read_next_resp(&buf) {
            if let Some(cmd) = Cmd::from(&resp) {
                let res;
                let response = match cmd {
                    Cmd::Ping => "+PONG\r\n".as_bytes(),
                    Cmd::Echo(s) => {
                        res = RESP::new_bulk(s).to_string();
                        res.as_bytes()
                    }
                    Cmd::Set(key, value, mut expire_time) => {
                        let shard = hash(&key) % db.len();
                        let now_millis = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        let mut db = db[shard].lock().await;
                        if expire_time != u128::MAX {
                            expire_time += now_millis
                        }
                        db.insert(key, (RESP::new_bulk(value), expire_time));
                        res = RESP::new_simple("OK".to_string()).to_string();
                        res.as_bytes()
                    }
                    Cmd::Get(key) => {
                        let shard = hash(&key) % db.len();
                        let now_millis = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        let mut db = db[shard].lock().await;
                        if let Some((value, expire_time)) = db.get(&key) {
                            if now_millis < *expire_time {
                                res = value.to_string();
                                res.as_bytes()
                            } else {
                                db.remove(&key);
                                resp_null_bytes
                            }
                        } else {
                            resp_null_bytes
                        }
                    }
                    Cmd::Info(rep) => {
                        if rep == "replication" {
                            let read_config = config.lock().await;
                            println!("info config:{:?}", read_config);
                            res = RESP::new_bulk(format!(
                                "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                                read_config.role,
                                read_config.master_replid,
                                read_config.master_repl_offset
                            ))
                            .to_string();
                            res.as_bytes()
                        } else {
                            resp_null_bytes
                        }
                    }
                    Cmd::ReplConf => "+OK\r\n".as_bytes(),
                    Cmd::Psync(repl_id, offset) => {
                        if repl_id.as_str() == "?" && offset == -1 {
                            let read_config = config.lock().await;
                            stream
                                .write_all(
                                    format!("+FULLRESYNC {} 0\r\n", read_config.master_replid)
                                        .as_bytes(),
                                )
                                .await
                                .unwrap();
                            &empty_rdb_bytes
                        } else {
                            resp_null_bytes
                        }
                    }
                    _ => resp_null_bytes,
                };
                stream.write_all(response).await.unwrap();
            }
        }
    }
}

async fn handshake(config: Arc<Mutex<Config>>) -> Result<()> {
    let mut config = config.lock().await;
    // handshake
    let mut stream =
        TcpStream::connect(format!("{}:{}", config.master_host, config.master_port)).await?;
    let mut buf = [0; 512];
    println!("Begin handshake");
    // 1.1 send "PING" to master
    stream
        .write_all(Cmd::new_ping_resp().to_string().as_bytes())
        .await?;
    // 1.2 receive "PONG" from master
    stream.read(&mut buf).await?;
    assert_eq!(
        RESP::read_next_resp(&buf).unwrap().1,
        RESP::new_simple("pong".to_string())
    );
    println!("handshake: PING finished");
    // 2.1 send "REPLCONF listening-port <PORT>" to master
    stream
        .write_all(
            format!(
                "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
                config.port
            )
            .as_bytes(),
        )
        .await?;
    // 2.2 receive "OK" from master
    stream.read(&mut buf).await?;
    assert_eq!(
        RESP::read_next_resp(&buf).unwrap().1,
        RESP::new_simple("ok".to_string())
    );
    // 2.3 send "REPLCONF capa psync2" to master
    stream
        .write_all("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".as_bytes())
        .await?;
    // 2.4 receive "OK" from master
    stream.read(&mut buf).await?;
    assert_eq!(
        RESP::read_next_resp(&buf).unwrap().1,
        RESP::new_simple("ok".to_string())
    );
    println!("handshake: REPLCONF finished");
    // 3.1 send "PSYNC ? -1" to master
    stream
        .write_all("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bytes())
        .await?;
    // 3.2 receive "+FULLRESYNC <REPL_ID> 0\r\n" from master
    stream.read(&mut buf).await?;
    let res = RESP::read_next_resp(&buf).unwrap().1;
    if let Some(Cmd::FullSync(repl_id, offset)) = Cmd::from(&res) {
        config.master_replid = repl_id;
        config.master_repl_offset = offset;
        println!("handshake: REPLCONF finished");
        Ok(())
    } else {
        Err(anyhow!(
            "failed in 2.6 receive \"+FULLRESYNC <REPL_ID> 0\r\n\" from master"
        ))
    }
}

async fn handle_master(config: Arc<Mutex<Config>>) -> Result<()> {
    if config.lock().await.role.as_str() == "slave" {
        handshake(config.clone()).await?;
        println!("slave: handshake finished");
    } else {
        println!("master: no need for handshaking");
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = env::args();
    let config = Arc::new(Mutex::new(Config::from_args(args)));

    tokio::spawn(handle_master(config.clone()));

    let listener = {
        let read_config = config.lock().await;
        TcpListener::bind(format!("127.0.0.1:{}", read_config.port))
            .await
            .unwrap()
    };

    let db = new_sharded_db(32);

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        println!("accepted new connection");
        let db = db.clone();
        let config = config.clone();
        tokio::spawn(handle_client(stream, db, config));
    }
}
