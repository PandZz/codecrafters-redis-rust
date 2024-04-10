use crate::frame::RESP;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    Echo(String),
    Set(String, String, u128),
    Get(String),
    Info(String),
    Incomplete,
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
                        if let (RESP::Bulk(key), RESP::Bulk(value)) = (&arr[1], &arr[2]) {
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
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
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
}
