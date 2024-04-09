use crate::frame::RESP;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    Echo(String),
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
