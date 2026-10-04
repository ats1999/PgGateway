listen = "127.0.0.1:6432"

databases = {
  postgres = {
    primary = {
      host = "127.0.0.1"
      port = 5432
    }

    replicas = [{ host = "127.0.0.1", port = 5433 }]

    # Pool configuration with explicit pooling mode
    # pool_mode options: "session" (default), "transaction", "statement"
    #   - session: Pin for entire session (never release to pool)
    #   - transaction: Pin until COMMIT/ROLLBACK
    #   - statement: Unpin after each statement completes
    pool_config = {
      max_connections = 50
      pool_mode = "session"
    }

    userlist = [{
      name     = "postgres"
      password = "postgres"
    }]
  }
}


