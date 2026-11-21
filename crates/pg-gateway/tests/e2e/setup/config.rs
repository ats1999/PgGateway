use anyhow::{Context, Result};
use hcl::{from_str, to_string, Value};

pub fn update_config(config: &str, host: &str, port: u16) -> Result<String> {
    let mut config: Value = from_str(config).context("Failed to parse HCL configuration")?;

    let root = config.as_object_mut().context("Expected root HCL object")?;

    // Traverse databases.pg_gateway.primary
    let primary = root
        .get_mut("databases")
        .and_then(Value::as_object_mut)
        .and_then(|databases| databases.get_mut("pg_gateway"))
        .and_then(Value::as_object_mut)
        .and_then(|database| database.get_mut("primary"))
        .and_then(Value::as_object_mut)
        .context("Missing databases.pg_gateway.primary configuration")?;

    primary.insert("host".into(), Value::from(host));
    primary.insert("port".into(), Value::from(port));

    to_string(&config).context("Failed to serialize HCL configuration")
}
