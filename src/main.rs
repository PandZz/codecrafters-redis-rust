// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 512];
    let res = String::from("+PONG\r\n");
    loop {
        let count = stream.read(&mut buf).unwrap();
        if count == 0 {
            break;
        }
        stream.write(res.as_bytes()).unwrap();
    }
}

fn main() {
    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

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
