listen = "127.0.0.1:6432"

databases = {
  postgres = {
    primary = { host = "127.0.0.1", port = 5432 }
    pool_config = { pool_mode = "transaction" }
    userlist = [{ name = "postgres" }]
  }
}