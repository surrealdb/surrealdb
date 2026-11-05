//! Observability facilities for SQL front-ends:
//! metrics counters, safe query logging, and mismatch reporting.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use regex::Regex;

use crate::dialect::SqlDialect;

/// A snapshot of current SQL planner and protocol metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPlanMetricsSnapshot {
	pub pgsql_queries: u64,
	pub mysql_queries: u64,
	pub prepared_statements_active: u64,
	pub prepared_bytes_active: u64,
	pub mismatches_detected: u64,
	pub unsupported_constructs: HashMap<String, u64>,
}

/// Global observability counters for SQL front-ends.
pub struct SqlPlanMetrics {
	pgsql_queries: AtomicU64,
	mysql_queries: AtomicU64,
	prepared_statements_active: AtomicU64,
	prepared_bytes_active: AtomicU64,
	mismatches_detected: AtomicU64,
	unsupported_constructs: RwLock<HashMap<String, u64>>,
}

impl Default for SqlPlanMetrics {
	fn default() -> Self {
		Self {
			pgsql_queries: AtomicU64::new(0),
			mysql_queries: AtomicU64::new(0),
			prepared_statements_active: AtomicU64::new(0),
			prepared_bytes_active: AtomicU64::new(0),
			mismatches_detected: AtomicU64::new(0),
			unsupported_constructs: RwLock::new(HashMap::new()),
		}
	}
}

static GLOBAL_METRICS: std::sync::OnceLock<SqlPlanMetrics> = std::sync::OnceLock::new();

/// The most distinct unsupported constructs the registry holds.
pub const MAX_CONSTRUCTS: usize = 256;

/// The most bytes of a construct the registry keeps.
pub const MAX_CONSTRUCT_LEN: usize = 128;

/// The key that counts the unsupported constructs past [`MAX_CONSTRUCTS`].
pub const OTHER_CONSTRUCTS: &str = "other";

/// `construct` cut to at most [`MAX_CONSTRUCT_LEN`] bytes, at a character boundary.
fn truncate_construct(construct: &str) -> &str {
	if construct.len() <= MAX_CONSTRUCT_LEN {
		return construct;
	}
	let mut end = MAX_CONSTRUCT_LEN;
	while !construct.is_char_boundary(end) {
		end -= 1;
	}
	&construct[..end]
}

/// Access the global metrics singleton.
pub fn global_metrics() -> &'static SqlPlanMetrics {
	GLOBAL_METRICS.get_or_init(SqlPlanMetrics::default)
}

impl SqlPlanMetrics {
	/// Record a query executed under a specific SQL dialect.
	pub fn record_query(&self, dialect: SqlDialect) {
		match dialect {
			SqlDialect::Postgres => {
				self.pgsql_queries.fetch_add(1, Ordering::Relaxed);
			}
			SqlDialect::MySql => {
				self.mysql_queries.fetch_add(1, Ordering::Relaxed);
			}
		}
	}

	/// Record an unsupported-feature error encountered for a specific construct.
	///
	/// The registry is bounded: a construct is kept to [`MAX_CONSTRUCT_LEN`] bytes, and once
	/// [`MAX_CONSTRUCTS`] distinct constructs are held any other is counted under
	/// [`OTHER_CONSTRUCTS`]. The caller names the kind of construct and not the client's text.
	pub fn record_unsupported_feature(&self, construct: &str) {
		let construct = truncate_construct(construct);
		tracing::warn!(construct = %construct, "unsupported SQL construct encountered");
		if let Ok(mut map) = self.unsupported_constructs.write() {
			let key = if map.contains_key(construct) || map.len() < MAX_CONSTRUCTS {
				construct
			} else {
				OTHER_CONSTRUCTS
			};
			let entry = map.entry(key.to_string()).or_insert(0);
			*entry = entry.saturating_add(1);
		}
	}

	/// Record a result equivalence mismatch detected against oracle.
	pub fn record_mismatch(&self) {
		tracing::error!("result equivalence mismatch detected against oracle");
		self.mismatches_detected.fetch_add(1, Ordering::Relaxed);
	}

	/// Record the creation/caching of a prepared statement with size in bytes.
	pub fn record_prepared_stmt(&self, bytes: usize) {
		self.prepared_statements_active.fetch_add(1, Ordering::Relaxed);
		self.prepared_bytes_active.fetch_add(bytes as u64, Ordering::Relaxed);
	}

	/// Record the eviction/closing of a prepared statement with size in bytes.
	pub fn record_closed_stmt(&self, bytes: usize) {
		self.prepared_statements_active.fetch_sub(1, Ordering::Relaxed);
		self.prepared_bytes_active.fetch_sub(bytes as u64, Ordering::Relaxed);
	}

	/// Obtain an instantaneous snapshot of the metrics counters.
	pub fn snapshot(&self) -> SqlPlanMetricsSnapshot {
		let unsupported =
			self.unsupported_constructs.read().map(|map| map.clone()).unwrap_or_default();

		SqlPlanMetricsSnapshot {
			pgsql_queries: self.pgsql_queries.load(Ordering::Relaxed),
			mysql_queries: self.mysql_queries.load(Ordering::Relaxed),
			prepared_statements_active: self.prepared_statements_active.load(Ordering::Relaxed),
			prepared_bytes_active: self.prepared_bytes_active.load(Ordering::Relaxed),
			mismatches_detected: self.mismatches_detected.load(Ordering::Relaxed),
			unsupported_constructs: unsupported,
		}
	}

	/// Reset all counters (primarily for testing).
	pub fn reset(&self) {
		self.pgsql_queries.store(0, Ordering::Relaxed);
		self.mysql_queries.store(0, Ordering::Relaxed);
		self.prepared_statements_active.store(0, Ordering::Relaxed);
		self.prepared_bytes_active.store(0, Ordering::Relaxed);
		self.mismatches_detected.store(0, Ordering::Relaxed);
		if let Ok(mut map) = self.unsupported_constructs.write() {
			map.clear();
		}
	}
}

/// Redact credentials, passwords, and tokens from SQL query strings for safe logging.
///
/// Ensures password literals in `CREATE USER`, `ALTER USER`, `IDENTIFIED BY`,
/// `PASSWORD '...'`, and `SECRET '...'` clauses are replaced with `'[REDACTED]'`.
pub fn redact_sql_credentials(sql: &str) -> String {
	// Patterns matching password literals:
	// 1. (IDENTIFIED\s+BY\s+)(['"][^'"]*['"]|\S+)
	// 2. (PASSWORD\s+)(['"][^'"]*['"]|\S+)
	// 3. (SECRET\s+)(['"][^'"]*['"]|\S+)
	let re = Regex::new(r#"(?i)\b(IDENTIFIED\s+BY|PASSWORD|SECRET)\s+(['"][^'"]*['"]|\S+)"#)
		.expect("valid credential regex");

	re.replace_all(sql, "$1 '[REDACTED]'").to_string()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_metrics_query_counting() {
		let metrics = SqlPlanMetrics::default();
		metrics.record_query(SqlDialect::Postgres);
		metrics.record_query(SqlDialect::Postgres);
		metrics.record_query(SqlDialect::MySql);

		let snap = metrics.snapshot();
		assert_eq!(snap.pgsql_queries, 2);
		assert_eq!(snap.mysql_queries, 1);
	}

	#[test]
	fn test_metrics_prepared_and_unsupported() {
		let metrics = SqlPlanMetrics::default();
		metrics.record_prepared_stmt(1024);
		metrics.record_unsupported_feature("WINDOW");
		metrics.record_unsupported_feature("WINDOW");
		metrics.record_mismatch();

		let snap = metrics.snapshot();
		assert_eq!(snap.prepared_statements_active, 1);
		assert_eq!(snap.prepared_bytes_active, 1024);
		assert_eq!(snap.mismatches_detected, 1);
		assert_eq!(snap.unsupported_constructs.get("WINDOW"), Some(&2));

		metrics.record_closed_stmt(1024);
		let snap2 = metrics.snapshot();
		assert_eq!(snap2.prepared_statements_active, 0);
		assert_eq!(snap2.prepared_bytes_active, 0);
	}

	#[test]
	fn test_unsupported_constructs_are_bounded() {
		let metrics = SqlPlanMetrics::default();
		let long = "é".repeat(MAX_CONSTRUCT_LEN);
		metrics.record_unsupported_feature(&long);
		for n in 0..(MAX_CONSTRUCTS * 2) {
			metrics.record_unsupported_feature(&format!("construct {n}"));
		}
		metrics.record_unsupported_feature("construct 0");

		let snap = metrics.snapshot();
		assert_eq!(snap.unsupported_constructs.len(), MAX_CONSTRUCTS + 1);
		assert!(snap.unsupported_constructs.keys().all(|key| key.len() <= MAX_CONSTRUCT_LEN));
		assert_eq!(snap.unsupported_constructs.get("construct 0"), Some(&2));
		assert_eq!(
			snap.unsupported_constructs.get(OTHER_CONSTRUCTS),
			Some(&(MAX_CONSTRUCTS as u64 + 1))
		);
		assert_eq!(
			snap.unsupported_constructs.values().sum::<u64>(),
			2 * MAX_CONSTRUCTS as u64 + 2
		);
	}

	#[test]
	fn test_redact_sql_credentials() {
		let pg_sql = "CREATE ROLE admin WITH PASSWORD 'super_secret_pw123';";
		let redacted_pg = redact_sql_credentials(pg_sql);
		assert!(!redacted_pg.contains("super_secret_pw123"));
		assert!(redacted_pg.contains("[REDACTED]"));

		let my_sql = "CREATE USER 'user'@'localhost' IDENTIFIED BY 'mysql_secret_456';";
		let redacted_my = redact_sql_credentials(my_sql);
		assert!(!redacted_my.contains("mysql_secret_456"));
		assert!(redacted_my.contains("[REDACTED]"));

		let plain_sql = "SELECT id, name FROM users WHERE active = true;";
		assert_eq!(redact_sql_credentials(plain_sql), plain_sql);
	}
}
