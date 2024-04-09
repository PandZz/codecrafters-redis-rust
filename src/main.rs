// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use redis_starter_rust::{cmd::Cmd, frame::RESP};

fn handle_client(mut stream: TcpStream) {
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
                }
            }
        }
    }
}

fn main() {
    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    // println!("{}", RESP::new_simple("PONG".to_string()).to_string());
    // println!("{}", RESP::new_bulk("hay".to_string()).to_string());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || handle_client(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
