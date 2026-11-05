//! [`SeenSet`] — the set of rows a `DISTINCT` has already emitted.
//!
//! Two rows are the same row when they compare equal, so the seen set is
//! **ordered** (`Value: Ord`): one comparison decides it for every row, including
//! one whose hash may disagree with its equality (see
//! [`Value::hash_agrees_with_eq`]).
//!
//! The set grows with the number of *distinct* rows; its size is bounded by
//! `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` (the shared GQL in-memory-build budget), and
//! exceeding it fails the query with an error that names the knob.

use std::collections::BTreeSet;

use crate::expr::ControlFlow;
use crate::val::Value;

/// The error a distinct raises past `max_rows` distinct rows.
pub(crate) fn too_many_distinct(max_rows: usize) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(format!(
		"GQL MATCH RETURN DISTINCT exceeded the maximum of {max_rows} distinct rows \
		 (configurable via SURREAL_GQL_MAX_JOIN_BUILD_ROWS)"
	))))
}

/// Set of seen rows, keyed by `Value: Ord` so membership is exactly the equality
/// the language uses — see the module docs for why a hash-bucketed set is not
/// equivalent. Only distinct rows are retained, so the stored count is bounded by
/// the configured build-row budget.
pub(crate) struct SeenSet {
	seen: BTreeSet<Value>,
}

impl SeenSet {
	pub(crate) fn new() -> Self {
		Self {
			seen: BTreeSet::new(),
		}
	}

	/// Record `value` if not already present. Returns `Ok(true)` when the value
	/// is newly inserted (caller should emit it), `Ok(false)` when it is a
	/// duplicate (caller should drop it). Fails when inserting would exceed
	/// `max_rows`.
	///
	/// The duplicate case — the common one for a dedup — probes without cloning;
	/// only a newly seen row is cloned into the set.
	pub(crate) fn insert(&mut self, value: &Value, max_rows: usize) -> Result<bool, ControlFlow> {
		if self.seen.contains(value) {
			return Ok(false);
		}
		if self.seen.len() >= max_rows {
			return Err(too_many_distinct(max_rows));
		}
		self.seen.insert(value.clone());
		Ok(true)
	}

	/// Every row seen, in order, leaving the set empty.
	pub(crate) fn take_sorted(&mut self) -> Vec<Value> {
		std::mem::take(&mut self.seen).into_iter().collect()
	}

	/// How many rows the set holds.
	pub(crate) fn len(&self) -> usize {
		self.seen.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn seen_set_guard_names_the_knob() {
		let mut seen = SeenSet::new();
		// First insert under a budget of 1 succeeds.
		assert_eq!(seen.insert(&Value::from(1), 1).unwrap(), true);
		// Re-inserting the same value is a duplicate, not a budget failure.
		assert_eq!(seen.insert(&Value::from(1), 1).unwrap(), false);
		// A new distinct value past the budget errors and names the knob.
		let err = seen.insert(&Value::from(2), 1).unwrap_err();
		let ControlFlow::Err(e) = err else {
			panic!("expected error, got {err:?}");
		};
		let msg = e.to_string();
		assert!(
			msg.contains("SURREAL_GQL_MAX_JOIN_BUILD_ROWS"),
			"guard error must name the knob, got: {msg}"
		);
	}
}
