use pg_gateway::{build_startup_packet, parse_startup_params};

#[test]
fn parse_startup_extracts_user_and_database() {
    let raw = build_startup_packet("alice", "appdb");
    let params = parse_startup_params(&raw);
    assert_eq!(params.get("user").map(String::as_str), Some("alice"));
    assert_eq!(params.get("database").map(String::as_str), Some("appdb"));
}

#[test]
fn build_startup_has_protocol_version() {
    let raw = build_startup_packet("u", "d");
    assert!(raw.len() >= 8);
    let version = i32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);
    assert_eq!(version, 3 << 16);
}
