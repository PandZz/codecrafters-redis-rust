use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum RESP {
    Integer(i64),
    Simple(String),
    Error(String),
    Bulk(String),
    Array(Vec<RESP>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(i128),
    Verbatim(String),
}

impl RESP {
    pub fn read_next_resp(src: &[u8]) -> Option<(usize, RESP)> {
        let mut i = 1;
        match src[0] {
            b'+' => {
                while src[i] != b'\r' {
                    i += 1;
                }
                i += 2;
                Some((
                    i,
                    RESP::Simple(
                        String::from_utf8_lossy(&src[1..i - 2])
                            .to_string()
                            .to_lowercase(),
                    ),
                ))
            }
            b'$' => {
                let mut len = 0;
                let is_negative = if src[1] == b'-' {
                    i += 1;
                    true
                } else {
                    false
                };
                while src[i] != b'\r' {
                    len = len * 10 + (src[i] - b'0') as usize;
                    i += 1;
                }
                i += 2;
                if is_negative {
                    Some((i, RESP::Null))
                } else {
                    Some((
                        i + len + 2,
                        RESP::Bulk(
                            String::from_utf8_lossy(&src[i..i + len])
                                .to_string()
                                .to_lowercase(),
                        ),
                    ))
                }
            }
            b'*' => {
                // println!("frame arr:{}", String::from_utf8_lossy(src));
                let mut len = 0;
                while src[i] != b'\r' {
                    len = len * 10 + (src[i] - b'0') as usize;
                    i += 1;
                }
                i += 2;
                let mut arr = Vec::with_capacity(len);
                for _ in 0..len {
                    if let Some((j, resp)) = RESP::read_next_resp(&src[i..]) {
                        i += j;
                        arr.push(resp);
                    } else {
                        return None;
                    }
                }
                Some((i, RESP::Array(arr)))
            }
            _ => None,
        }
    }

    pub fn new_bulk(str: String) -> Self {
        RESP::Bulk(str)
    }

    pub fn new_simple(str: String) -> Self {
        RESP::Simple(str)
    }

    pub fn new_null() -> &'static Self {
        &(RESP::Null)
    }
}

impl Display for RESP {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RESP::Integer(i) => write!(f, ":{}", i),
            RESP::Simple(s) => write!(f, "+{}\r\n", s),
            RESP::Error(e) => write!(f, "-{}\r\n", e),
            RESP::Bulk(b) => write!(f, "${}\r\n{}\r\n", b.len(), b),
            RESP::Array(arr) => {
                write!(f, "*{}\r\n", arr.len())?;
                for resp in arr {
                    write!(f, "{}", resp)?;
                }
                Ok(())
            }
            RESP::Null => write!(f, "$-1\r\n"),
            RESP::Boolean(b) => write!(f, ":{}\r\n", if *b { 't' } else { 'f' }),
            RESP::Double(d) => write!(f, ":{}\r\n", d),
            RESP::BigNumber(n) => write!(f, ":{}\r\n", n),
            RESP::Verbatim(v) => write!(f, "+{}\r\n", v),
        }
    }
}

#[cfg(test)]
mod resp_test {
    use super::*;

    #[test]
    fn test_simple_string() {
        let src = b"+OK\r\n";
        let (len, resp) = RESP::read_next_resp(src).unwrap();
        assert_eq!(len, 5);
        assert_eq!(resp, RESP::Simple("OK".to_string()));
    }

    #[test]
    fn test_bulk_string() {
        let src = b"$6\r\nfoobar\r\n";
        let (len, resp) = RESP::read_next_resp(src).unwrap();
        assert_eq!(len, 12);
        assert_eq!(resp, RESP::Bulk("foobar".to_string()));
    }

    #[test]
    fn test_null_bulk_string() {
        let src = b"$-1\r\n";
        let (len, resp) = RESP::read_next_resp(src).unwrap();
        assert_eq!(len, 5);
        assert_eq!(resp, RESP::Null);
    }

    #[test]
    fn test_array() {
        let src = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (len, resp) = RESP::read_next_resp(src).unwrap();
        assert_eq!(len, 22);
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::Bulk("foo".to_string()),
                RESP::Bulk("bar".to_string())
            ])
        );
    }

    #[test]
    fn test_another_array() {
        let src = b"*4\r\n$5\r\napple\r\n$6\r\nbanana\r\n$2\r\npx\r\n$3\r\n123\r\n";
        let (_, resp) = RESP::read_next_resp(src).unwrap();
        // println!("resp: {}", resp);
        assert_eq!(
            resp,
            RESP::Array(vec![
                RESP::Bulk("apple".to_string()),
                RESP::Bulk("banana".to_string()),
                RESP::Bulk("px".to_string()),
                RESP::Bulk("123".to_string()),
            ])
        )
    }
}
