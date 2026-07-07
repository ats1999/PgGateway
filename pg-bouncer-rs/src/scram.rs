use base64::{engine::general_purpose::STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub fn client_first() -> String {
    let nonce = random_nonce();
    format!("n,,n=,r={nonce}")
}

pub fn client_final(password: &str, client_first: &str, server_first: &str) -> anyhow::Result<String> {
    let r = extract(server_first, "r=")?;
    let s = extract(server_first, "s=")?;
    let i: u32 = extract(server_first, "i=")?.parse()?;

    let salt = STANDARD.decode(s)?;
    let salted_password = hi(password, &salt, i)?;

    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = sha256(&client_key);
    let client_first_bare = client_first
        .strip_prefix("n,,")
        .unwrap_or(client_first);
    let client_final_no_proof = format!("c=biws,r={r}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_no_proof}");
    let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
    let client_proof: Vec<u8> = client_key
        .iter()
        .zip(client_signature.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    let proof_b64 = STANDARD.encode(client_proof);
    Ok(format!("{client_final_no_proof},p={proof_b64}"))
}

fn extract<'a>(s: &'a str, prefix: &str) -> anyhow::Result<&'a str> {
    s.split(',')
        .find_map(|p| p.strip_prefix(prefix))
        .ok_or_else(|| anyhow::anyhow!("missing {prefix} in {s}"))
}

fn hi(password: &str, salt: &[u8], iterations: u32) -> anyhow::Result<Vec<u8>> {
    let mut msg = salt.to_vec();
    msg.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac_sha256(password.as_bytes(), &msg);
    let mut out = u.clone();
    for _ in 1..iterations {
        u = hmac_sha256(password.as_bytes(), &u);
        for (a, b) in out.iter_mut().zip(u.iter()) {
            *a ^= b;
        }
    }
    Ok(out)
}

fn sha256(data: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().to_vec()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn random_nonce() -> String {
    let mut bytes = [0u8; 18];
    rand::thread_rng().fill_bytes(&mut bytes);
    STANDARD.encode(bytes)
}
