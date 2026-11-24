//! Parse Postgres' `log_statement = all` output into `(backend pid, sql)` records.
//!
//! Lines look like `2026-11-25 10:00:00.123 UTC [57] LOG:  statement: SELECT 1` (simple query)
//! or `... [57] LOG:  execute s0: SELECT 1` (extended protocol).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedStatement {
    pub pid: i32,
    pub sql: String,
}

pub fn parse(log: &str) -> Vec<LoggedStatement> {
    log.lines().filter_map(parse_line).collect()
}

fn parse_line(line: &str) -> Option<LoggedStatement> {
    let open = line.find(" [")?;
    let close = open + line[open..].find("] ")?;
    let pid = line[open + 2..close].parse().ok()?;
    let message = line[close + 2..].strip_prefix("LOG:  ")?;
    let sql = if let Some(sql) = message.strip_prefix("statement: ") {
        sql
    } else {
        // `execute <name>: <sql>` / `execute <unnamed>: <sql>`
        let rest = message.strip_prefix("execute ")?;
        &rest[rest.find(": ")? + 2..]
    };
    Some(LoggedStatement {
        pid,
        sql: sql.trim().to_string(),
    })
}

#[test]
fn parses_simple_and_extended_statements() {
    let log = "\
2026-11-25 10:00:00.123 UTC [57] LOG:  statement: SELECT 1
2026-11-25 10:00:00.124 UTC [58] LOG:  execute s0: SELECT pg_backend_pid()
2026-11-25 10:00:00.125 UTC [58] LOG:  execute <unnamed>: SELECT 2
2026-11-25 10:00:00.126 UTC [1] LOG:  database system is ready to accept connections
2026-11-25 10:00:00.127 UTC [59] ERROR:  division by zero";
    assert_eq!(
        parse(log),
        vec![
            LoggedStatement { pid: 57, sql: "SELECT 1".into() },
            LoggedStatement { pid: 58, sql: "SELECT pg_backend_pid()".into() },
            LoggedStatement { pid: 58, sql: "SELECT 2".into() },
        ]
    );
}
