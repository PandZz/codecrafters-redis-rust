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

async fn handle_client(mut stream: TcpStream, db: ShardedDb, config: Arc<Mutex<Config>>) {
    let mut buf = [0; 512];
    loop {
        let count = stream.read(&mut buf).await.unwrap();
        if count == 0 {
            break;
        }
        if let Some((_, resp)) = RESP::read_next_resp(&buf) {
            if let Some(cmd) = Cmd::from(&resp) {
                let response = match cmd {
                    Cmd::Ping => "+PONG\r\n".to_string(),
                    Cmd::Echo(s) => RESP::new_bulk(s).to_string(),
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
                        RESP::new_simple("OK".to_string()).to_string()
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
                                value.to_string()
                            } else {
                                db.remove(&key);
                                RESP::new_null().to_string()
                            }
                        } else {
                            RESP::new_null().to_string()
                        }
                    }
                    Cmd::Info(rep) => {
                        if rep == "replication" {
                            let read_config = config.lock().await;
                            println!("info config:{:?}", read_config);
                            RESP::new_bulk(format!(
                                "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                                read_config.role,
                                read_config.master_replid,
                                read_config.master_repl_offset
                            ))
                            .to_string()
                        } else {
                            RESP::new_null().to_string()
                        }
                    }
                    Cmd::ReplConf => "+OK\r\n".to_string(),
                    Cmd::Psync(repl_id, offset) => {
                        if repl_id.as_str() == "?" && offset == -1 {
                            let read_config = config.lock().await;
                            format!("+FULLRESYNC {} 0\r\n", read_config.master_replid)
                        } else {
                            RESP::new_null().to_string()
                        }
                    }
                    _ => RESP::new_null().to_string(),
                };
                stream.write_all(response.as_bytes()).await.unwrap();
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
