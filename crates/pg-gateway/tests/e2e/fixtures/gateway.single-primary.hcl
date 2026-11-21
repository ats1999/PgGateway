listen = "0.0.0.0:6432"

pool_defaults = {
  pool_size = 10
  server_reset_query = "DISCARD ALL"
  server_idle_timeout_seconds = 7200
}

databases = {
  pg_gateway = {
    dbname = "pg_gateway"

    primary = {
      host = "127.0.0.1"
      port = 5432
    }

    pool_config = {
      pool_mode = "session"
      pool_size = 2
      max_idle = 1
      max_db_connections = 10
      max_db_client_connections = 100
    }

    userlist = [
      { name = "ats1999", password = "badboy" }
    ]
  }
}