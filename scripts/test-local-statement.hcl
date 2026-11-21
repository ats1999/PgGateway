listen = "127.0.0.1:6432"

databases = {
  postgres = {
    primary = { host = "127.0.0.1", port = 5432 }
    pool_config = { pool_mode = "statement" }
    userlist = [{ name = "postgres", password = "postgres" }]  # gateway checks every login; set the real password
  }
}