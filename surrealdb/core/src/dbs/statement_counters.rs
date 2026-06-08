//! Per-statement side-channel counters tracked by the document path and read
//! by the executor when emitting [`crate::observe::StatementEvent`]s.
//!
//! The post-RETURN [`crate::val::Value`] is not a reliable source for the
//! number of records affected by a DML statement: `RETURN NONE` causes the
//! iterator to drop every touched document (see `IgnoreError::Ignore` in
//! `crate::dbs::iterator`) so the executor sees an empty array, while
//! `RETURN BEFORE` on a fresh `CREATE` collapses to `Value::None`. To honour
//! the documented contract on
//! [`crate::observe::StatementEventSafe::result_rows`] -- "rows returned
//! (SELECT) or affected (CREATE / UPDATE / UPSERT / DELETE / RELATE / INSERT)"
//! -- the document path increments this counter only after a real KV write
//! has succeeded (`store_record_data` / `purge` set
//! [`crate::doc::Document::mutated`], which is then consumed once per row by
//! [`crate::doc::Document::process`]). The executor reads the snapshot at
//! statement completion.
//!
//! Pre-mutation `IgnoreError::Ignore` paths -- `check_record_exists`,
//! `check_where_condition`, permission gates, `ctx.is_done` short-circuits --
//! and `set_record` no-ops suppressed by `!self.changed()` therefore never
//! inflate the count, even when the visited row would otherwise have been
//! counted by the iterator.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic per-statement counter set, shared between the iterator and the
/// executor for the lifetime of a single top-level statement.
///
/// `Relaxed` ordering is sufficient because the counter is never used to
/// gate other state: the executor only reads it after the statement has
/// returned, by which point all writes to the counter have happened-before
/// the read through the executor's await.
#[derive(Debug, Default)]
pub(crate) struct StatementCounters {
	/// Records that DML statements (CREATE / UPDATE / UPSERT / DELETE /
	/// RELATE / INSERT) successfully mutated during the statement.
	/// Incremented from [`crate::doc::Document::process`] once the
	/// document path has signalled a real KV write via
	/// [`crate::doc::Document::mutated`], so the value is unaffected
	/// by the configured RETURN mode and by pre-mutation `Ignore`
	/// gates that filter rows before any storage write.
	///
	/// SELECT row counts are derived from the post-RETURN [`crate::val::Value`]
	/// shape inside [`crate::dbs::executor`], not from this counter.
	affected: AtomicU64,
}

impl StatementCounters {
	/// Construct an empty counter set wrapped in `Arc` for sharing between
	/// the executor and any contexts derived from it.
	pub(crate) fn new() -> Arc<Self> {
		Arc::new(Self::default())
	}

	/// Bump the affected-row counter by one.
	pub(crate) fn record_affected(&self) {
		self.affected.fetch_add(1, Ordering::Relaxed);
	}

	/// Read the current affected-row count. Cheap; safe to call from the
	/// executor immediately after the statement has returned.
	pub(crate) fn affected(&self) -> u64 {
		self.affected.load(Ordering::Relaxed)
	}
}
