// Uncomment this block to pass the first stage
use redis_starter_rust::{server::*, Config};
use std::{env, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{mpsc, Mutex},
};

#[tokio::main]
async fn main() {
    let args = env::args();
    let config = Arc::new(Mutex::new(Config::from_args(args)));
    let db = new_sharded_db(32);

    // 只有当前服务器为slave时, 这里能连接到1个master服务器, 在这里接收到的"write"命令只需静默执行
    tokio::spawn(handle_master(config.clone(), db.clone()));

    let listener = {
        let read_config = config.lock().await;
        TcpListener::bind(format!("127.0.0.1:{}", read_config.port))
            .await
            .unwrap()
    };

    let (cmd_tx, cmd_rx) = mpsc::channel(512);
    let replica_tx_list: ShardedTxList = Arc::new(Mutex::new(Vec::new()));

    tokio::spawn(handle_trans_write_cmd(cmd_rx, replica_tx_list.clone()));
    loop {
        // 若当前服务器为master, 则: 在n个stream中有m个是客户端, n - m个是slave服务器, 需要将客户端发来的"write"命令转发到slave服务器
        // 若当前服务器为slave, 则: 在此处的stream全都是客户端, 无需特殊处理
        let (stream, _) = listener.accept().await.unwrap();
        println!("accepted new connection");
        let db = db.clone();
        let config = config.clone();
        let replica_tx_list = replica_tx_list.clone();
        let cmd_tx = cmd_tx.clone();
        tokio::spawn(handle_client(stream, db, config, replica_tx_list, cmd_tx));
    }
}
