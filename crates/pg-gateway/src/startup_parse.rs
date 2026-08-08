use std::collections::HashMap;

use bytes::Bytes;

/// Parses `user`, `database`, and other keys from a PostgreSQL StartupMessage body.
pub fn parse_startup_params(startup: &Bytes) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if startup.len() < 8 {
        return out;
    }

    let mut idx = 8;
    while idx < startup.len() {
        if startup[idx] == 0 {
            break;
        }
        let Some(key_end) = startup[idx..].iter().position(|&b| b == 0) else {
            break;
        };
        let key = String::from_utf8_lossy(&startup[idx..idx + key_end]).into_owned();
        idx += key_end + 1;
        if idx >= startup.len() {
            break;
        }
        let Some(val_end) = startup[idx..].iter().position(|&b| b == 0) else {
            break;
        };
        let value = String::from_utf8_lossy(&startup[idx..idx + val_end]).into_owned();
        idx += val_end + 1;
        out.insert(key, value);
    }
    out
}

pub fn build_startup_packet(user: &str, database: &str) -> Bytes {
    let mut body = Vec::new();
    body.extend_from_slice(&(3_i32 << 16).to_be_bytes());
    body.extend_from_slice(format!("user\0{user}\0database\0{database}\0\0").as_bytes());
    let len = body.len() as i32 + 4;
    let mut out = Vec::with_capacity(len as usize);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&body);
    Bytes::from(out)
}
