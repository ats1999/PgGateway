listen = "127.0.0.1:6432"

# Virtual admin DB: psql -d pg_gateway (override with admin_database)
# admin_database = "pg_gateway"

# Merge order (later wins): built-in default → pool_defaults → database pool_config
#   → primary / replicas[i] pool_config → userlist pool_config.
# See which layer won for every setting: psql -d pg_gateway -c "SHOW POOL_CONFIG"
pool_defaults = {
  pool_size              = 100
  server_reset_query     = "DISCARD ALL"
  server_idle_timeout_seconds = 7200
}

databases = {
  rocketdb = {
    # Actual database on the primary and every replica; clients use the logical name (the key
    # above). Unset = same as the logical name (PgBouncer `dbname`).
    dbname = "postgres"

    primary = {
      host = "127.0.0.1"
      port = 5432
      # Optional: tighten connect time for this host only
      # pool_config = { server_connect_timeout_seconds = 5 }
    }

    replicas = [{
      host = "127.0.0.1"
      port = 5433
      # Overrides for pools on this replica (no read routing yet; see docs/configuration.md)
      # pool_config = { pool_size = 30 }
    }]

    # pool_mode: "session" (default) | "transaction" | "statement"
    pool_config = {
      pool_mode                   = "transaction"
      pool_size                   = 2      # max_connections is an alias
      max_idle                    = 1
      min_pool_size               = 2       # warm-up logs in with the userlist password
      max_db_connections          = 200
      max_db_client_connections   = 500
      server_idle_timeout_seconds = 3600
      connect_query               = "SET application_name = 'pg_gateway'"

      # Clients wait for a free connection; unset = wait forever.
      query_wait_timeout_seconds       = 30
      idle_transaction_timeout_seconds = 60
      # transaction_timeout_seconds    = 300
      # client_idle_timeout_seconds    = 3600
      # client_login_timeout_seconds   = 15
      # server_check_query             = "SELECT 1"
      # server_check_delay_seconds     = 30
      # server_connect_timeout_seconds = 5
      # server_login_retry_seconds     = 15
    }

    userlist = [
      {
        name     = "postgres"
        password = "postgres"
        # Per-user overrides (every instance). Database-wide keys such as
        # max_db_connections are not allowed here.
        # pool_config = { pool_size = 10 }
      },
    ]
  }
}