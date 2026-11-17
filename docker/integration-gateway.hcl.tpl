# Rendered by docker/integration-restart-gateway.sh (envsubst: LISTEN_PORT, PG_PORT, POOL_MODE).
listen = "127.0.0.1:${LISTEN_PORT}"

databases = {
  postgres = {
    primary = { host = "127.0.0.1", port = ${PG_PORT} }
    userlist = [{ name = "postgres" }]
  }
  "pg-gateway" = {
    primary = { host = "127.0.0.1", port = ${PG_PORT} }
    pool_config = { pool_mode = "${POOL_MODE}" }
    userlist = [{ name = "postgres" }]
  }
}