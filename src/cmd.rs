use crate::frame::RESP;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    Echo(String),
    Set(String, String, u128),
    Get(String),
}

impl Cmd {
    pub fn from(frame: &RESP) -> Option<Self> {
        if let RESP::Array(arr) = frame {
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
                        if let RESP::Bulk(key) = &arr[1] {
                            if let RESP::Bulk(value) = &arr[2] {
                                if let (Some(RESP::Bulk(px)), Some(RESP::Bulk(millis_str))) =
                                    (arr.get(3), arr.get(4))
                                {
                                    if px == "px" {
                                        Some(Cmd::Set(
                                            key.clone(),
                                            value.clone(),
                                            millis_str.parse().unwrap(),
                                        ))
                                    } else {
                                        None
                                    }
                                } else {
                                    Some(Cmd::Set(key.clone(), value.clone(), u128::MAX))
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    "get" => {
                        if let RESP::Bulk(key) = &arr[1] {
                            Some(Cmd::Get(key.clone()))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        }
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
}
