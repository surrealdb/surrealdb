//! A zero `LIMIT` reads no record.
//!
//! A statement whose limit evaluates to zero returns no row, and reads none
//! from storage to find that out: a scan of a table, an index, a record id or
//! a record range beneath it checks what it checks before reading — that its
//! table exists, that the statement may select from it — and then reads no
//! record, so no record is judged by the table's permissions, its computed
//! fields, the statement's predicates or its projection. A source that is not
//! a read of storage — an array, a parameter, a subquery — yields its rows as
//! it always does, and the clauses above judge them. `START` alone reads as
//! usual: the rows it skips are judged before they are skipped.
//!
//! A limit that the planner pushes into the scan reaches it as the scan's own
//! limit. One it keeps above the scan, because rows are filtered between the
//! two, reaches it through a binding: [`bind_scans`] ties the storage scans of
//! the limit's own input to the limit's id when the plan is built, and the
//! limit, having evaluated to zero, runs its input in a context holding its id
//! ([`ExecutionContext::with_zero_limit`]). A scan honours only the id it is
//! bound to, so the context reaching a nested statement — a subquery in a
//! predicate or a source — changes nothing there: that statement's scans are
//! bound to its own limits, if any.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

#[cfg(test)]
use crate::exec::ExecutionContext;
use crate::exec::{AccessMode, ExecOperator};

/// The limit a storage scan is bound to, if any.
#[derive(Debug, Clone, Default)]
pub(crate) struct ZeroLimitBinding(OnceLock<u64>);

impl ZeroLimitBinding {
	/// Bind the scan to limit `id`. A scan is bound to the innermost limit
	/// above it, which binds first.
	fn bind(&self, id: u64) {
		let _ = self.0.set(id);
	}

	#[cfg(test)]
	/// Whether the limit the scan is bound to evaluated to zero in `ctx`.
	pub(crate) fn applies(&self, ctx: &ExecutionContext) -> bool {
		self.0.get().is_some_and(|id| ctx.zero_limited(*id))
	}
}

/// A fresh limit id.
pub(crate) fn next_id() -> u64 {
	static NEXT: AtomicU64 = AtomicU64::new(1);
	NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Bind the storage scans of `input`, the input of limit `id`, to it.
///
/// The walk follows the operators' children, which never reach into an
/// expression's subquery. It stops at another limit, whose scans are its own,
/// and at an operator that writes, whose reads the statement owes whatever
/// its limit.
pub(crate) fn bind_scans(input: &Arc<dyn ExecOperator>, id: u64) {
	if let Some(binding) = input.zero_limit_binding() {
		binding.bind(id);
		return;
	}
	if input.name() == "Limit" || input.access_mode() != AccessMode::ReadOnly {
		return;
	}
	for child in input.children() {
		bind_scans(child, id);
	}
}
