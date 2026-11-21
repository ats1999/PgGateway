# Rendered by docker/integration-restart-gateway.sh (envsubst). Tests inject extra pool settings
# through DEFAULTS_POOL_CONFIG, DB_POOL_CONFIG, and USER_POOL_CONFIG (one `key = value` per line).
listen = "127.0.0.1:${LISTEN_PORT}"

pool_defaults = {
  ${DEFAULTS_POOL_CONFIG}
}

databases = {
  postgres = {
    primary = { host = "127.0.0.1", port = ${PG_PORT} }
    userlist = [{ name = "postgres", password = "postgres" }]
  }
  # Logical name for database gw_actual; gw_secure needs a SCRAM password upstream, which the
  # gateway takes from the userlist for min_pool_size warm-up and re-checkout logins.
  "gw-logical" = {
    dbname  = "gw_actual"
    primary = { host = "127.0.0.1", port = ${PG_PORT} }
    pool_config = {
      pool_mode     = "${POOL_MODE}"
      pool_size     = 10
      min_pool_size = 2
    }
    userlist = [{ name = "gw_secure", password = "gw_secret" }]
  }
  "pg-gateway" = {
    primary = { host = "127.0.0.1", port = ${PG_PORT} }
    # Same server as the primary; clients are not routed here (no read routing yet), it exists
    # so the admin database shows replica-level overrides.
    replicas = [{ host = "127.0.0.1", port = ${PG_PORT}, pool_config = { pool_size = 7 } }]
    pool_config = {
      pool_mode = "${POOL_MODE}"
      ${DB_POOL_CONFIG}
    }
    userlist = [{
      name     = "postgres"
      password = "postgres"
      pool_config = {
        ${USER_POOL_CONFIG}
      }
    }]
  }
}
