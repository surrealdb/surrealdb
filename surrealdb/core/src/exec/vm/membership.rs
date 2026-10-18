//! The rows of a statement kept across evaluations for membership tests: the
//! `x IN (SELECT ...)` and `x NOT IN (SELECT ...)` whose statement runs once.
//!
//! The answer is `Value::Array(rows).contains(probe)`, which tests each row
//! with [`Value::equal`]. That is not [`Value::cmp`]: a string equals a regex
//! that matches it, and the order of a value holding a geometry, range or
//! closure does not agree with its equality. So only the rows whose order agrees with their
//! equality ([`Value::ord_agrees_with_equal`]) are indexed, sorted and
//! deduplicated by `cmp`, where a probe that agrees too is found by a binary
//! search. The other rows are kept aside and tested one by one with `equal`,
//! as is any probe that does not agree.

use std::cmp::Ordering;

use crate::exec::memory::size::values_bytes;
use crate::exec::memory::{Reservation, Verdict};
use crate::exec::{ExecutionContext, FlowResult};
use crate::val::Value;

/// The rows of a statement, held for membership tests.
///
/// The bytes of both vectors are charged to the query's lease, as bytes it
/// cannot spill, for as long as the rows are kept.
#[derive(Debug)]
pub(super) struct Members {
	/// The rows whose order agrees with their equality, sorted and
	/// deduplicated by `Value::cmp`.
	sorted: Vec<Value>,
	/// Every other row, in arrival order.
	aside: Vec<Value>,
	/// What the two vectors hold, released when the rows are dropped.
	_account: Reservation,
}

impl Members {
	/// Keep `rows`, moved rather than copied, for membership tests, charging
	/// what they weigh to the query's lease. The query waits or overcommits,
	/// and fails only where its pinned bytes would pass its ceiling
	/// ([`Reservation::insist`]).
	pub(super) async fn keep(rows: Vec<Value>, ctx: &ExecutionContext) -> FlowResult<Self> {
		let (mut sorted, aside): (Vec<_>, Vec<_>) =
			rows.into_iter().partition(Value::ord_agrees_with_equal);
		sorted.sort_unstable_by(Value::cmp);
		// Among values whose order agrees with their equality, `cmp` is `Equal`
		// exactly where `equal` holds, and a membership test asks only whether a
		// match exists, so a repeat changes no answer.
		sorted.dedup_by(|a, b| Value::cmp(a, b) == Ordering::Equal);
		let mut account = match ctx.ctx().memory() {
			Some(memory) => memory.pinned_reservation(),
			None => Reservation::unbounded(),
		};
		if account.charge(values_bytes(&sorted) + values_bytes(&aside)) == Verdict::Spill {
			account.insist(ctx).await?;
		}
		Ok(Self {
			sorted,
			aside,
			_account: account,
		})
	}

	/// Whether `Value::Array(rows).contains(probe)` holds of the rows kept.
	pub(super) fn contains(&self, probe: &Value) -> bool {
		if !probe.ord_agrees_with_equal() {
			return self.sorted.iter().chain(&self.aside).any(|row| row.equal(probe));
		}
		self.sorted.binary_search_by(|row| row.cmp(probe)).is_ok()
			|| self.aside.iter().any(|row| row.equal(probe))
	}
}
