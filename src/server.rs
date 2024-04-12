use crate::{cmd::Cmd, frame::RESP, Config};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{
        mpsc::{self, Receiver, Sender},
        OnceCell, RwLock,
    },
};

pub type ShardedDb = Arc<Vec<RwLock<HashMap<String, (RESP, u128)>>>>;
pub type CmdSender = Sender<RESP>;
pub type CmdReceiver = Receiver<RESP>;
pub type ReplicaSender = Sender<Vec<u8>>;
pub type ReplicaRecevier = Receiver<Vec<u8>>;
type ShardedT<T> = Arc<RwLock<T>>;
pub type ShardedTxList = ShardedT<Vec<ReplicaSender>>;
pub type ShardedConfig = ShardedT<Config>;

static EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
static EMPTY_RDB_BYTES: OnceCell<Bytes> = OnceCell::const_new();
static RESP_NULL_BYTES: OnceCell<Bytes> = OnceCell::const_new();

fn hash(s: &str) -> usize {
    const MOD: usize = 1e9 as usize + 7;
    const P: usize = 26;
    s.chars().fold(0, |acc, x| (acc * P + x as usize) % MOD)
}

pub fn new_sharded_db(num_shards: usize) -> ShardedDb {
    let mut db = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        db.push(RwLock::new(HashMap::new()));
    }
    Arc::new(db)
}

fn hex2bytes(src: &str) -> Vec<u8> {
    let mut hex_bytes: Vec<_> = src
        .as_bytes()
        .iter()
        .map(|&c| {
            if c.is_ascii_digit() {
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

pub async fn handle_replica(mut stream: TcpStream, mut rx: ReplicaRecevier) {
    loop {
        if let Some(val) = rx.recv().await {
            stream.write_all(&val).await.unwrap()
        }
    }
}

pub async fn trans_write_cmd(mut cmd_rx: CmdReceiver, tx_list: ShardedTxList) {
    loop {
        if let Some(cmd) = cmd_rx.recv().await {
            let read_tx_list = tx_list.read().await;
            let cmd = cmd.to_string().into_bytes();
            for tx in read_tx_list.iter() {
                tx.send(cmd.clone()).await.unwrap();
            }
        }
    }
}

pub async fn handle_client(
    mut stream: TcpStream,
    db: ShardedDb,
    config: ShardedConfig,
    tx_list: ShardedTxList,
    write_cmd_tx: CmdSender,
    num_replica: Arc<RwLock<usize>>,
) {
    let resp_null_bytes = RESP_NULL_BYTES
        .get_or_init(|| async { Bytes::from(RESP::Null.to_string()) })
        .await;
    // let resp_null_bytes = RESP::Null.to_string();
    // let resp_null_bytes = resp_null_bytes.as_bytes();
    let mut buf = [0; 1024];
    loop {
        let mut i = 0;
        let count = stream.read(&mut buf).await.unwrap();
        if count == 0 {
            break;
        }
        while let Some((j, resp)) = RESP::read_next_resp(&buf[i..]) {
            i += j;
            if let Some(cmd) = Cmd::from(&resp) {
                let mut is_write_cmd = false;
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
                        let mut write_db = db[shard].write().await;
                        if expire_time != u128::MAX {
                            expire_time += now_millis
                        }
                        write_db.insert(key, (RESP::new_bulk(value), expire_time));
                        res = RESP::new_simple("OK".to_string()).to_string();
                        is_write_cmd = true;
                        res.as_bytes()
                    }
                    Cmd::Get(key) => {
                        let shard = hash(&key) % db.len();
                        let now_millis = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        let mut write_db = db[shard].write().await;
                        // println!("handle_client get db:{:?}", db);
                        if let Some((value, expire_time)) = write_db.get(&key) {
                            if now_millis < *expire_time {
                                res = value.to_string();
                                res.as_bytes()
                            } else {
                                write_db.remove(&key);
                                resp_null_bytes
                            }
                        } else {
                            resp_null_bytes
                        }
                    }
                    Cmd::Info(rep) => {
                        if rep == "replication" {
                            let read_config = config.read().await;
                            // println!("info config:{:?}", read_config);
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
                    Cmd::ReplConf(_, _) => "+OK\r\n".as_bytes(),
                    Cmd::Psync(repl_id, offset) => {
                        if repl_id.as_str() == "?" && offset == -1 {
                            let read_config = config.read().await;
                            stream
                                .write_all(
                                    format!("+FULLRESYNC {} 0\r\n", read_config.master_replid)
                                        .as_bytes(),
                                )
                                .await
                                .unwrap();
                            let empty_rdb_bytes = EMPTY_RDB_BYTES
                                .get_or_init(|| async {
                                    let empty_rdb_bytes = hex2bytes(EMPTY_RDB_HEX);
                                    let front = format!("${}\r\n", empty_rdb_bytes.len());
                                    Bytes::from([front.into_bytes(), empty_rdb_bytes].concat())
                                })
                                .await;
                            stream.write_all(empty_rdb_bytes).await.unwrap();
                        }
                        let (tx, rx) = mpsc::channel(32);
                        tx_list.write().await.push(tx);
                        let mut num = num_replica.write().await;
                        *num += 1;
                        tokio::spawn(handle_replica(stream, rx));
                        return;
                    }
                    Cmd::Wait(_numreplicas, _timeout) => {
                        res = RESP::Integer(*num_replica.read().await as i64).to_string();
                        res.as_bytes()
                    }
                    _ => resp_null_bytes,
                };
                stream.write_all(response).await.unwrap();
                if is_write_cmd {
                    write_cmd_tx.send(resp).await.unwrap();
                }
            }
            if i > count {
                break;
            }
        }
    }
}

async fn handshake(
    config: &mut Config,
    stream: &mut TcpStream,
    buf: &mut [u8],
) -> Result<(usize, usize)> {
    // handshake
    println!("Begin handshake");
    // 1.1 send "PING" to master
    stream
        .write_all(Cmd::new_ping_resp().to_string().as_bytes())
        .await?;
    // 1.2 receive "PONG" from master
    if stream.read(buf).await? == 0 {
        return Err(anyhow!(
            "handshake: 1.2 receive \"PONG\" from master failed"
        ));
    }
    assert_eq!(
        RESP::read_next_resp(buf).unwrap().1,
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
    if stream.read(buf).await? == 0 {
        return Err(anyhow!("handshake: 2.2 receive \"OK\" from master failed"));
    }
    assert_eq!(
        RESP::read_next_resp(buf).unwrap().1,
        RESP::new_simple("ok".to_string())
    );
    // 2.3 send "REPLCONF capa psync2" to master
    stream
        .write_all("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".as_bytes())
        .await?;
    // 2.4 receive "OK" from master
    if stream.read(buf).await? == 0 {
        return Err(anyhow!("handshake: 2.4 receive \"OK\" from master failed"));
    }
    assert_eq!(
        RESP::read_next_resp(buf).unwrap().1,
        RESP::new_simple("ok".to_string())
    );
    println!("handshake: REPLCONF finished");
    // 3.1 send "PSYNC ? -1" to master
    stream
        .write_all("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bytes())
        .await?;
    // 3.2 receive "+FULLRESYNC <REPL_ID> 0\r\n" and RDB file from master
    let final_count = stream.read(buf).await?;
    if final_count == 0 {
        return Err(anyhow!(
            "3.2 receive \"+FULLRESYNC <REPL_ID> 0\r\n\" and RDB file from master failed"
        ));
    }
    let (len, res) = RESP::read_next_resp(buf).unwrap();
    println!("handshake FULLRESYNC resp:{}", res);
    if let Some(Cmd::FullReSync(repl_id, offset)) = Cmd::from(&res) {
        config.master_replid = repl_id;
        config.master_repl_offset = offset;
        println!("handshake: receive FULLRESYNC and RDB file finished");
        Ok((len, final_count))
    } else {
        Err(anyhow!(
            "failed in 2.6 receive \"+FULLRESYNC <REPL_ID> 0\r\n\" from master"
        ))
    }
}

// master slave 握手后master可能连续发送命令使得上一次stream.read后残留了需要同步的命令
async fn handle_master_loop(
    mut stream: TcpStream,
    mut buf: [u8; 1024],
    mut offset: usize,
    mut count: usize,
    db: ShardedDb,
) -> Result<()> {
    let mut total_len = 0;
    loop {
        while let Some((len, resp)) = RESP::read_next_resp(&buf[offset..]) {
            offset += len;
            if let Some(cmd) = Cmd::from(&resp) {
                // println!("handle_master_loop: receive cmd:{:?}", &cmd);
                match cmd {
                    // 这里只处理需要回显的命令
                    Cmd::Set(key, value, mut expire_time) => {
                        let shard = hash(&key) % db.len();
                        let now_millis = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        let mut write_db = db[shard].write().await;
                        if expire_time != u128::MAX {
                            expire_time += now_millis
                        }
                        write_db.insert(key, (RESP::new_bulk(value), expire_time));
                        // println!("handle_master db:{:?}", &write_db);
                    }
                    Cmd::ReplConf(r#type, arg) => {
                        if &r#type == "getack" && &arg == "*" {
                            let res = RESP::new_cmd_array(vec![
                                "REPLCONF".to_string(),
                                "ACK".to_string(),
                                total_len.to_string(),
                            ]);
                            stream.write_all(res.to_string().as_bytes()).await?;
                        }
                    }
                    _ => (),
                };
                // 即使不回显的命令也需要记录其长度
                total_len += len;
            }
            if offset > count {
                break;
            }
        }
        count = stream.read(&mut buf).await.unwrap();
        if count == 0 {
            break Ok(());
        }
        offset = 0;
    }
}

pub async fn handle_master(config: ShardedConfig, db: ShardedDb) -> Result<()> {
    let mut write_config = config.write().await;
    if write_config.role.as_str() == "slave" {
        let mut stream = TcpStream::connect(format!(
            "{}:{}",
            write_config.master_host, write_config.master_port
        ))
        .await?;
        let mut buf = [0; 1024];
        let (offset, count) = handshake(&mut write_config, &mut stream, &mut buf).await?;
        println!("slave: handshake has finished, listening from master begins");
        tokio::spawn(handle_master_loop(stream, buf, offset, count, db));
    } else {
        println!("master: no need for handshaking");
    }
    Ok(())
}
