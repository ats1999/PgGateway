//! Gateway-initiated upstream login against an in-process mock that asks for a password.

use std::collections::HashMap;

use pg_protocol::{read_frontend, read_startup_request, StartupRequest};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::*;
use crate::startup_parse::parse_startup_params;

const USER: &str = "svc";
const PASSWORD: &str = "s3cret";
const SALT: [u8; 4] = [1, 2, 3, 4];

/// What the mock asks the gateway for after startup.
#[derive(Clone, Copy)]
enum Ask {
    Trust,
    Cleartext,
    Md5,
    /// SASL offering only this (unsupported) mechanism.
    SaslOnly(&'static str),
}

/// What the mock saw on one login.
#[derive(Debug)]
struct Seen {
    startup: HashMap<String, String>,
    accepted: bool,
}

fn backend(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_request(code: i32, extra: &[u8]) -> Vec<u8> {
    let mut body = code.to_be_bytes().to_vec();
    body.extend_from_slice(extra);
    backend(b'R', &body)
}

/// The password a correct client sends for `ask`.
fn expected_password(ask: Ask) -> Vec<u8> {
    match ask {
        Ask::Md5 => md5_hash(USER.as_bytes(), PASSWORD.as_bytes(), SALT).into_bytes(),
        _ => PASSWORD.as_bytes().to_vec(),
    }
}

/// One-shot mock upstream: sends `ask`, checks the reply, then finishes login (AuthenticationOk,
/// ParameterStatus, ReadyForQuery) or rejects it with an ErrorResponse.
async fn mock(ask: Ask) -> (String, JoinHandle<Seen>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let StartupRequest::Startup(raw) = read_startup_request(&mut socket).await.unwrap() else {
            panic!("expected a startup packet");
        };
        let startup = parse_startup_params(&raw);
        let accepted = match ask {
            Ask::Trust => true,
            Ask::SaslOnly(mechanism) => {
                let mut list = mechanism.as_bytes().to_vec();
                list.extend_from_slice(b"\0\0");
                socket.write_all(&auth_request(AUTH_SASL, &list)).await.unwrap();
                return Seen { startup, accepted: false };
            }
            Ask::Cleartext | Ask::Md5 => {
                let request = match ask {
                    Ask::Md5 => auth_request(AUTH_MD5_PASSWORD, &SALT),
                    _ => auth_request(AUTH_CLEARTEXT_PASSWORD, &[]),
                };
                socket.write_all(&request).await.unwrap();
                match read_frontend(&mut socket).await {
                    Ok(reply) => {
                        assert_eq!(reply.tag(), b'p');
                        let sent = &reply.raw[5..reply.raw.len() - 1];
                        sent == expected_password(ask).as_slice()
                    }
                    // The gateway gave up (no password configured).
                    Err(_) => return Seen { startup, accepted: false },
                }
            }
        };
        let reply = if accepted {
            let mut ok = auth_request(AUTH_OK, &[]);
            ok.extend(backend(b'S', b"server_version\x0016\x00"));
            ok.extend(backend(b'Z', b"I"));
            ok
        } else {
            backend(b'E', b"SFATAL\0C28P01\0Mpassword authentication failed\0\0")
        };
        let _ = socket.write_all(&reply).await;
        Seen { startup, accepted }
    });
    (addr, task)
}

async fn login_to(addr: &str, password: Option<&str>) -> anyhow::Result<Bytes> {
    ServerConnection::connect_and_authenticate(
        addr,
        USER,
        "actual_db",
        password,
        Some(Duration::from_secs(5)),
        None,
    )
    .await
    .map(|(_, flight)| flight)
}

/// Tags of the backend messages in a replay flight.
fn flight_tags(flight: &[u8]) -> Vec<u8> {
    let mut tags = Vec::new();
    let mut idx = 0;
    while idx + 5 <= flight.len() {
        tags.push(flight[idx]);
        let len = i32::from_be_bytes([flight[idx + 1], flight[idx + 2], flight[idx + 3], flight[idx + 4]]);
        idx += 1 + len as usize;
    }
    tags
}

#[tokio::test]
async fn trust_login_needs_no_password() {
    let (addr, upstream) = mock(Ask::Trust).await;
    let flight = login_to(&addr, None).await.expect("trust login");
    assert!(upstream.await.unwrap().accepted);
    assert_eq!(flight_tags(&flight), b"RSZ");
}

#[tokio::test]
async fn cleartext_login_sends_userlist_password() {
    let (addr, upstream) = mock(Ask::Cleartext).await;
    login_to(&addr, Some(PASSWORD)).await.expect("cleartext login");
    assert!(upstream.await.unwrap().accepted);
}

#[tokio::test]
async fn md5_login_sends_hashed_password() {
    let (addr, upstream) = mock(Ask::Md5).await;
    login_to(&addr, Some(PASSWORD)).await.expect("md5 login");
    assert!(upstream.await.unwrap().accepted);
}

#[tokio::test]
async fn replay_flight_starts_at_authentication_ok() {
    let (addr, _upstream) = mock(Ask::Md5).await;
    let flight = login_to(&addr, Some(PASSWORD)).await.expect("md5 login");
    // The md5 challenge must not be replayed to clients that reuse this connection.
    assert_eq!(flight_tags(&flight), b"RSZ");
    assert_eq!(auth_code(&flight), Some(AUTH_OK));
}

#[tokio::test]
async fn login_sends_actual_database_name() {
    let (addr, upstream) = mock(Ask::Trust).await;
    login_to(&addr, None).await.expect("login");
    let seen = upstream.await.unwrap();
    assert_eq!(seen.startup.get("database").map(String::as_str), Some("actual_db"));
    assert_eq!(seen.startup.get("user").map(String::as_str), Some(USER));
}

#[tokio::test]
async fn password_request_without_userlist_password_fails_with_hint() {
    let (addr, upstream) = mock(Ask::Md5).await;
    let error = login_to(&addr, None).await.expect_err("must fail without a password");
    assert!(error.to_string().contains("set `password`"), "{error:#}");
    assert!(!upstream.await.unwrap().accepted);
}

#[tokio::test]
async fn wrong_password_is_rejected() {
    let (addr, upstream) = mock(Ask::Md5).await;
    let error = login_to(&addr, Some("wrong")).await.expect_err("wrong password");
    assert!(error.to_string().contains("rejected"), "{error:#}");
    assert!(!upstream.await.unwrap().accepted);
}

#[tokio::test]
async fn sasl_without_scram_sha_256_is_refused() {
    let (addr, _upstream) = mock(Ask::SaslOnly("SCRAM-SHA-256-PLUS")).await;
    let error = login_to(&addr, Some(PASSWORD)).await.expect_err("no usable mechanism");
    assert!(error.to_string().contains("no supported SASL mechanism"), "{error:#}");
}
