pub mod cmd;
pub mod frame;

pub struct Config {
    pub port: u32,
    pub master_host: String,
    pub master_port: u32,
    pub role: String,
    pub master_replid: String,
    pub master_repl_offset: usize,
}

impl Config {
    pub fn new() -> Self {
        Config {
            port: 6379,
            master_host: "".to_string(),
            master_port: 0,
            role: "master".to_string(),
            // 40 character alphanumeric string
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            // starts from 0
            master_repl_offset: 0,
        }
    }
    pub fn from_args(mut args: std::env::Args) -> Self {
        let mut config = Config::new();
        while let Some(x) = args.next() {
            match x.as_str() {
                "--port" => {
                    let port = args.next().and_then(|s| s.parse().ok());
                    match port {
                        Some(port) => config.port = port,
                        None => (),
                    }
                }
                "--replicaof" => {
                    config.master_host = match args.next() {
                        Some(host) => host,
                        None => config.master_host,
                    };
                    config.master_port = match args.next().and_then(|s| s.parse().ok()) {
                        Some(port) => port,
                        None => 0,
                    };
                    if config.master_port != 0 {
                        config.role = "slave".to_string();
                    }
                }
                _ => (),
            }
        }
        config
    }
}
