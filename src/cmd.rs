use crate::frame::RESP;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    Echo(String),
    Set(String, String, u128),
    Get(String),
    Info(String),
    ReplConf(String, String),
    Psync(String, i64),
    FullReSync(String, usize),
    Incomplete,
}

impl Cmd {
    pub fn from(frame: &RESP) -> Option<Self> {
        match frame {
            RESP::Array(arr) => {
                if let RESP::Bulk(s) = &arr[0] {
                    match s.as_str() {
                        "ping" => Some(Cmd::Ping),
                        "echo" => {
                            if let RESP::Bulk(s) = &arr[1] {
                                Some(Cmd::Echo(s.clone()))
                            } else {
                                None
                            }
                        }
                        "set" => {
                            if let (RESP::Bulk(key), RESP::Bulk(value)) = (&arr[1], &arr[2]) {
                                if let (Some(RESP::Bulk(px)), Some(RESP::Bulk(millis_str))) =
                                    (arr.get(3), arr.get(4))
                                {
                                    if px == "px" {
                                        match millis_str.parse() {
                                            Ok(millis) => {
                                                Some(Cmd::Set(key.clone(), value.clone(), millis))
                                            }
                                            Err(_) => None,
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    Some(Cmd::Set(key.clone(), value.clone(), u128::MAX))
                                }
                            } else {
                                None
                            }
                        }
                        "get" => arr.get(1).and_then(|resp| {
                            if let RESP::Bulk(key) = resp {
                                Some(Cmd::Get(key.clone()))
                            } else {
                                None
                            }
                        }),
                        "info" => arr.get(1).map_or(Some(Cmd::Info("".to_string())), |resp| {
                            if let RESP::Bulk(rep) = resp {
                                Some(Cmd::Info(rep.clone()))
                            } else {
                                Some(Cmd::Info("".to_string()))
                            }
                        }),
                        "replconf" => {
                            if let (Some(RESP::Bulk(a)), Some(RESP::Bulk(b))) =
                                (arr.get(1), arr.get(2))
                            {
                                Some(Cmd::ReplConf(a.clone(), b.clone()))
                            } else {
                                None
                            }
                        }
                        "psync" => {
                            if let (
                                Some(RESP::Bulk(master_replid)),
                                Some(RESP::Bulk(master_repl_offset)),
                            ) = (arr.get(1), arr.get(2))
                            {
                                match master_repl_offset.parse() {
                                    Ok(offset) => Some(Cmd::Psync(master_replid.clone(), offset)),
                                    Err(_) => None,
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            RESP::Simple(s) => {
                // FULLRESYNC
                // println!("Cmd from RESP::Simple:{}", s);
                if s.starts_with("fullresync") {
                    let mut iter = s.split_whitespace();
                    iter.next();
                    if let (Some(repl_id), Some(off_str)) = (iter.next(), iter.next()) {
                        // println!("repl_id:{}, off_str:{}", repl_id, off_str);
                        match off_str.parse() {
                            Ok(off) => Some(Cmd::FullReSync(repl_id.to_string(), off)),
                            Err(_) => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    pub fn new_ping_resp() -> RESP {
        RESP::Array(vec![RESP::Bulk("ping".to_string())])
    }
}

#[cfg(test)]
mod cmd_test {
    use super::*;

    #[test]
    fn test_ping() {
        let frame = RESP::Array(vec![RESP::Bulk("ping".to_string())]);
        assert_eq!(Cmd::from(&frame), Some(Cmd::Ping));
    }

    #[test]
    fn test_echo() {
        let frame = RESP::Array(vec![
            RESP::Bulk("echo".to_string()),
            RESP::Bulk("hello".to_string()),
        ]);
        assert_eq!(Cmd::from(&frame), Some(Cmd::Echo("hello".to_string())));
    }

    #[test]
    fn test_fullresync() {
        let frame =
            RESP::Simple("fullresync 75cd7bc10c49047e0d163660f3b90625b1af31dc 0".to_string());
        assert_eq!(
            Cmd::from(&frame),
            Some(Cmd::FullReSync(
                "75cd7bc10c49047e0d163660f3b90625b1af31dc".to_string(),
                0
            ))
        );
    }
}
