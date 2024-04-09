// Uncomment this block to pass the first stage
use redis_starter_rust::{cmd::Cmd, frame::RESP};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

type ShardedDb = Arc<Vec<Mutex<HashMap<String, RESP>>>>;

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

fn handle_client(mut stream: TcpStream, db: ShardedDb) {
    let mut buf = [0; 512];
    let pong_res = RESP::new_simple("PONG".to_string()).to_string();
    loop {
        let count = stream.read(&mut buf).unwrap();
        if count == 0 {
            break;
        }
        if let Some((_, resp)) = RESP::read_next_resp(&buf) {
            if let Some(cmd) = Cmd::from(&resp) {
                match cmd {
                    Cmd::Ping => {
                        stream.write_all(pong_res.as_bytes()).unwrap();
                    }
                    Cmd::Echo(s) => {
                        stream
                            .write_all(RESP::new_bulk(s).to_string().as_bytes())
                            .unwrap();
                    }
                    Cmd::Set(key, value) => {
                        let shard = hash(&key) % db.len();
                        let mut db = db[shard].lock().unwrap();
                        db.insert(key, RESP::new_bulk(value));
                        stream
                            .write_all(RESP::new_simple("OK".to_string()).to_string().as_bytes())
                            .unwrap();
                    }
                    Cmd::Get(key) => {
                        let shard = hash(&key) % db.len();
                        let db = db[shard].lock().unwrap();
                        if let Some(value) = db.get(&key) {
                            stream.write_all(value.to_string().as_bytes()).unwrap();
                        } else {
                            stream
                                .write_all(RESP::new_null().to_string().as_bytes())
                                .unwrap();
                        }
                    }
                }
            }
        }
    }
}

fn main() {
    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let db = new_sharded_db(32);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let db = db.clone();
                thread::spawn(move || handle_client(stream, db));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
