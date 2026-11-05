//! The transaction layer's own knobs.
//!
//! Both bound a single transaction rather than the datastore that opens it: the
//! entry cache is per-transaction state, and every write a transaction issues
//! reserves a slot against the write-cardinality guard. A [`TransactionConfig`]
//! is handed to [`Transaction::new`] at construction, so every transaction one
//! factory opens is bounded identically. The guard itself is armed only on the
//! statement-execution paths, which read the limit through
//! `Datastore::transaction_max_write_keys`.
//!
//! [`Transaction::new`]: crate::Transaction::new

use std::str::FromStr;
use std::time::Duration;

use surrealdb_cnf as cnf;

/// What the datastore does with a read-only transaction whose staleness bound
/// no published snapshot meets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FallbackPolicy {
	/// Read the current state in place, unless the request is `STRICT`.
	Allow,
	/// Never read the current state; fail the transaction instead.
	Deny,
	/// Read the current state in place while fewer than this many such reads
	/// opened in the current second; fail the transaction beyond that.
	RateLimited(u32),
}

impl FromStr for FallbackPolicy {
	type Err = ();

	/// `allow`, `deny` or `rate:<per second>`.
	fn from_str(s: &str) -> Result<Self, ()> {
		let s = s.trim();
		if s.eq_ignore_ascii_case("allow") {
			Ok(Self::Allow)
		} else if s.eq_ignore_ascii_case("deny") {
			Ok(Self::Deny)
		} else if s.get(..5).is_some_and(|p| p.eq_ignore_ascii_case("rate:")) {
			s[5..].trim().parse().map(Self::RateLimited).map_err(|_| ())
		} else {
			Err(())
		}
	}
}

/// Limits applied to a single transaction.
#[derive(Clone, Debug)]
pub struct TransactionConfig {
	/// Specifies the number of items which can be cached within a single
	/// transaction (default: 512)
	pub transaction_cache_size: usize,
	/// Maximum number of write operations a single statement transaction may
	/// buffer before it is aborted with an error; 0 disables the guard
	/// (default: 0).
	///
	/// A statement's physical write count can vastly exceed its logical row
	/// count: cascaded deletes, full-text term maintenance, and graph-edge
	/// cleanup all multiply per-record work, and on distributed backends every
	/// written key is reserved on every replica at prepare time while staying
	/// far below byte-based write-set limits. The guard reserves one slot per
	/// write before it is issued, so fan-out stops accumulating at the limit
	/// and the transaction rolls back atomically. Tripping the guard poisons
	/// the transaction: an explicit COMMIT (client-owned RPC/SDK
	/// transactions) is refused and rolls back, so the partial statement can
	/// never be persisted.
	///
	/// Accounting (canonical; the `Transaction` API docs defer here): per-key
	/// writes count individually. Each range delete counts as one write — on
	/// backends that expand a range delete into per-key writes inside the
	/// same transaction (TiKV, bounded by `SURREAL_TIKV_DELR_MAX_KEYS`), the
	/// effective key bound is therefore this limit multiplied by that
	/// per-operation cap, and operators sizing distributed clusters need both
	/// numbers. Commit-time changefeed and live-query event writes count like
	/// any other write. Reservations are never refunded — neither by failed
	/// writes nor by savepoint rollbacks — so a statement retried through
	/// savepoints (e.g. under UPSERT contention) can fail earlier than its
	/// final buffered size.
	///
	/// Applies to statement execution wherever it happens: executor-created
	/// transactions, statements executed on explicit client-owned (RPC/SDK)
	/// transactions, and record-access clause evaluation
	/// (SIGNUP/SIGNIN/AUTHENTICATE). Internal maintenance transactions (index
	/// builds, compaction, garbage collection) are not affected
	/// (`SURREAL_TRANSACTION_MAX_WRITE_KEYS`).
	pub transaction_max_write_keys: u64,
	/// The largest `MAX STALENESS` a read-only transaction may request. A
	/// larger request fails when the transaction opens. It bounds how late a
	/// permission revocation can reach a stale transaction
	/// (default: one hour).
	pub read_snapshot_max_staleness: Duration,
	/// What happens when no published snapshot meets a request's bound
	/// (default: `allow`).
	pub read_snapshot_fallback: FallbackPolicy,
}

impl Default for TransactionConfig {
	fn default() -> Self {
		Self {
			transaction_cache_size: 512,
			transaction_max_write_keys: 0,
			read_snapshot_max_staleness: Duration::from_secs(3600),
			read_snapshot_fallback: FallbackPolicy::Allow,
		}
	}
}

impl cnf::Config for TransactionConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("transaction_cache_size", &mut self.transaction_cache_size)
			.parse_key("transaction_max_write_keys", &mut self.transaction_max_write_keys)
			.parse_key_with(
				"read_snapshot_max_staleness_secs",
				&mut self.read_snapshot_max_staleness,
				|x| x.trim().parse().ok().map(Duration::from_secs),
			)
			.parse_key("read_snapshot_fallback", &mut self.read_snapshot_fallback);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_fallback_policy_parses_case_insensitively() {
		assert_eq!("ALLOW".parse(), Ok(FallbackPolicy::Allow));
		assert_eq!(" deny ".parse(), Ok(FallbackPolicy::Deny));
		assert_eq!("rate:10".parse(), Ok(FallbackPolicy::RateLimited(10)));
		assert_eq!("RATE: 10".parse(), Ok(FallbackPolicy::RateLimited(10)));
		assert_eq!("rate:x".parse::<FallbackPolicy>(), Err(()));
		assert_eq!("ra".parse::<FallbackPolicy>(), Err(()));
		assert_eq!("é".parse::<FallbackPolicy>(), Err(()));
	}
}
