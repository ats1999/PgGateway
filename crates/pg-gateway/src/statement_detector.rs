/// Detects SQL statement types and protocol-level transaction status.
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    /// Transaction control: BEGIN, COMMIT, ROLLBACK
    TransactionControl(TransactionOp),
    /// Simple query: SELECT, INSERT, UPDATE, DELETE
    SimpleQuery,
    /// Other statements (SET, CREATE TEMP, SHOW, EXPLAIN, etc.)
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionOp {
    Begin,
    Commit,
    Rollback,
}

/// PostgreSQL transaction status from ReadyForQuery message (Z message type).
/// See: https://www.postgresql.org/docs/current/protocol-message-formats.html
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// 'I' = Not in a transaction (Idle)
    Idle,
    /// 'T' = In a transaction block
    InTransaction,
    /// 'E' = In a failed transaction block (error state)
    FailedTransaction,
}

/// Detect PostgreSQL transaction status from ReadyForQuery message (backend message).
///
/// ReadyForQuery message format:
/// - 1 byte: tag 'Z'
/// - 4 bytes: message length (5 for this message)
/// - 1 byte: transaction status ('I', 'T', or 'E')
///
/// This is the authoritative source for transaction state from the server.
pub fn detect_transaction_status(message: &Bytes) -> Option<TransactionStatus> {
    // Message must have at least: tag (1) + length (4) + status (1) = 6 bytes
    if message.len() < 6 {
        return None;
    }

    // Check for ReadyForQuery message tag 'Z'
    if message[0] != b'Z' {
        return None;
    }

    // Transaction status is at byte 5 (after tag + 4-byte length)
    match message[5] {
        b'I' => Some(TransactionStatus::Idle),
        b'T' => Some(TransactionStatus::InTransaction),
        b'E' => Some(TransactionStatus::FailedTransaction),
        _ => None,
    }
}

/// Detect the type of SQL statement from a frontend query message.
///
/// The message format for simple Query (tag 'Q') is:
/// - 1 byte: tag 'Q'
/// - 4 bytes: message length (including length field but not tag)
/// - N bytes: SQL string (null-terminated)
///
/// Note: This is a secondary check. The authoritative transaction status
/// comes from the server's ReadyForQuery message via detect_transaction_status().
pub fn detect_statement_type(message: &Bytes) -> StatementType {
    if message.is_empty() {
        return StatementType::Other;
    }

    // Extract the SQL from the message
    // For Query messages (tag 'Q'), skip the 5-byte header (tag + length)
    // and find the null-terminated string
    let sql_bytes = if message.len() > 5 && message[0] == b'Q' {
        // Skip tag and length fields
        &message[5..]
    } else {
        return StatementType::Other;
    };

    // Null-terminate handling - find the string
    let sql_str = match std::str::from_utf8(sql_bytes) {
        Ok(s) => {
            // Remove null terminator if present
            s.trim_end_matches('\0')
        }
        Err(_) => return StatementType::Other,
    };

    detect_from_sql(sql_str)
}

fn detect_from_sql(sql: &str) -> StatementType {
    let normalized = sql.trim().to_uppercase();

    // Split by whitespace to get the first word
    let first_word = match normalized.split_whitespace().next() {
        Some(word) => word,
        None => return StatementType::Other,
    };

    match first_word {
        // Transaction control
        "BEGIN" => StatementType::TransactionControl(TransactionOp::Begin),
        "START" => {
            // START TRANSACTION
            if normalized.contains("TRANSACTION") {
                StatementType::TransactionControl(TransactionOp::Begin)
            } else {
                StatementType::Other
            }
        }
        "COMMIT" => StatementType::TransactionControl(TransactionOp::Commit),
        "END" => {
            // END is equivalent to COMMIT in Postgres
            if normalized.split_whitespace().nth(1).is_none() {
                StatementType::TransactionControl(TransactionOp::Commit)
            } else {
                StatementType::Other
            }
        }
        "ROLLBACK" => StatementType::TransactionControl(TransactionOp::Rollback),
        "SAVEPOINT" | "RELEASE" => {
            // SAVEPOINT and RELEASE SAVEPOINT don't affect pinning
            // (connection is already pinned by BEGIN)
            StatementType::Other
        }
        // Explicit pool modes only (no auto-detection)
        "SET" | "RESET" | "CREATE" | "ALTER" => StatementType::Other,
        // Simple queries
        "SELECT" => StatementType::SimpleQuery,
        "INSERT" => StatementType::SimpleQuery,
        "UPDATE" => StatementType::SimpleQuery,
        "DELETE" => StatementType::SimpleQuery,
        "WITH" => {
            // Common Table Expressions (CTE) - usually SELECT
            StatementType::SimpleQuery
        }
        // Everything else
        _ => StatementType::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_from_sql() {
        assert_eq!(
            detect_from_sql("BEGIN"),
            StatementType::TransactionControl(TransactionOp::Begin)
        );
        assert_eq!(
            detect_from_sql("COMMIT"),
            StatementType::TransactionControl(TransactionOp::Commit)
        );
        assert_eq!(
            detect_from_sql("ROLLBACK"),
            StatementType::TransactionControl(TransactionOp::Rollback)
        );
        assert_eq!(
            detect_from_sql("SAVEPOINT sp1"),
            StatementType::Other
        );
        assert_eq!(
            detect_from_sql("RELEASE SAVEPOINT sp1"),
            StatementType::Other
        );
        assert_eq!(detect_from_sql("SET work_mem = '256MB'"), StatementType::Other);
        assert_eq!(
            detect_from_sql("CREATE TEMP TABLE t (id INT)"),
            StatementType::Other
        );
        assert_eq!(
            detect_from_sql("CREATE TEMPORARY TABLE t (id INT)"),
            StatementType::Other
        );
        assert_eq!(detect_from_sql("SELECT * FROM users"), StatementType::SimpleQuery);
        assert_eq!(
            detect_from_sql("INSERT INTO users VALUES (1)"),
            StatementType::SimpleQuery
        );
        assert_eq!(
            detect_from_sql("UPDATE users SET name = 'test'"),
            StatementType::SimpleQuery
        );
        assert_eq!(detect_from_sql("DELETE FROM users"), StatementType::SimpleQuery);
    }

    #[test]
    fn test_whitespace_handling() {
        assert_eq!(
            detect_from_sql("  \n  BEGIN  \n  "),
            StatementType::TransactionControl(TransactionOp::Begin)
        );
        assert_eq!(
            detect_from_sql("\t\tSELECT\t*\t"),
            StatementType::SimpleQuery
        );
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(
            detect_from_sql("begin"),
            StatementType::TransactionControl(TransactionOp::Begin)
        );
        assert_eq!(detect_from_sql("select * from t"), StatementType::SimpleQuery);
    }

    #[test]
    fn test_protocol_level_transaction_status() {
        // ReadyForQuery message with status 'I' (Idle)
        let idle_msg = Bytes::from_static(b"Z\x00\x00\x00\x05I");
        assert_eq!(
            detect_transaction_status(&idle_msg),
            Some(TransactionStatus::Idle)
        );

        // ReadyForQuery message with status 'T' (In Transaction)
        let in_tx_msg = Bytes::from_static(b"Z\x00\x00\x00\x05T");
        assert_eq!(
            detect_transaction_status(&in_tx_msg),
            Some(TransactionStatus::InTransaction)
        );

        // ReadyForQuery message with status 'E' (Failed Transaction)
        let failed_msg = Bytes::from_static(b"Z\x00\x00\x00\x05E");
        assert_eq!(
            detect_transaction_status(&failed_msg),
            Some(TransactionStatus::FailedTransaction)
        );

        // Invalid message (wrong tag)
        let invalid_msg = Bytes::from_static(b"Q\x00\x00\x00\x05I");
        assert_eq!(detect_transaction_status(&invalid_msg), None);

        // Message too short
        let short_msg = Bytes::from_static(b"Z\x00\x00");
        assert_eq!(detect_transaction_status(&short_msg), None);
    }
}
