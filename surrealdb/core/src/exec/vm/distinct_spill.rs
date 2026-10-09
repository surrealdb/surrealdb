//! A GQL `DISTINCT` that spills: the rows it has seen held in the query's
//! temporary store once its lease no longer covers them, with its output in
//! the order the rows arrived.
//!
//! In memory, a row is handed on the moment it is first seen. Once the seen
//! set outgrows its lease it is written out, sorted, as a run, and every row
//! that arrives after is deferred rather than judged: whether it was seen
//! cannot be answered without reading the run, and handing on a later row
//! before an earlier deferred one would change the order. A deferred row is
//! kept with its arrival, and deferred rows are spilled sorted by row then
//! arrival as they outgrow the lease.
//!
//! When the input is finished, the deferred rows are merged in row order
//! against the rows seen before the spill: a row seen before is dropped, and
//! of deferred rows that are equal only the earliest is kept. The rows kept
//! are then put back in arrival order, by an external sort on their arrival,
//! and handed on. The output is the output the in-memory set gives.
//!
//! The distinct rows counted against `gql_max_join_build_rows` are the rows
//! seen before the spill and the deferred rows kept, so a statement past the
//! limit fails with the error the in-memory set raises.

use std::cmp::Ordering;
use std::sync::Arc;

use super::spill::{RecordOrder, Runs, write_run};
use crate::exec::memory::runs::Record;
use crate::exec::memory::{QueryMemory, Reservation, Verdict};
use crate::exec::operators::distinct::{SeenSet, too_many_distinct};
use crate::exec::{ExecutionContext, FlowResult};
use crate::val::Value;

/// The order of records by their keys: a seen row, a deferred row and its
/// arrival, or a kept row's arrival.
fn by_keys() -> RecordOrder {
	Arc::new(|(a, _), (b, _)| a.cmp(b))
}

/// What a distinct holds once it has spilled.
pub(super) struct Spilled {
	memory: Arc<QueryMemory>,
	/// The rows seen before the spill, one run in row order.
	seen: Runs,
	seen_count: usize,
	/// Deferred rows still held, each keyed by the row and its arrival.
	deferred: Vec<Record>,
	deferred_runs: Runs,
	arrivals: i64,
}

/// A distinct's seen rows and, once it has spilled, its deferred rows.
pub(super) struct Distinct {
	pub(super) seen: SeenSet,
	pub(super) max_rows: usize,
	account: Reservation,
	spilled: Option<Box<Spilled>>,
}

impl Distinct {
	/// A distinct before any row, charging `ctx`'s query where it has one.
	pub(super) fn start(max_rows: usize, ctx: &ExecutionContext) -> Self {
		Self {
			seen: SeenSet::new(),
			max_rows,
			account: match ctx.ctx().memory() {
				Some(memory) => memory.reservation(),
				None => Reservation::unbounded(),
			},
			spilled: None,
		}
	}

	/// Whether the distinct has spilled, so rows it defers are handed on
	/// when its input is finished ([`Self::finish`]).
	pub(super) fn has_spilled(&self) -> bool {
		self.spilled.is_some()
	}

	/// Take `row`: answer it when it is the first of its rows to arrive and
	/// the distinct judges in memory, or nothing when it is a repeat or is
	/// deferred.
	pub(super) async fn take(
		&mut self,
		row: Value,
		ctx: &ExecutionContext,
	) -> FlowResult<Option<Value>> {
		if let Some(spilled) = &mut self.spilled {
			let arrival = spilled.arrivals;
			spilled.arrivals += 1;
			let bytes = crate::exec::memory::size::value_bytes(&row);
			spilled.deferred.push((vec![row, Value::from(arrival)], Value::None));
			if self.account.charge(bytes) == Verdict::Spill {
				let mut held = std::mem::take(&mut spilled.deferred);
				held.sort_by(|(a, _), (b, _)| a.cmp(b));
				let run = write_run(&spilled.memory, held, ctx).await?;
				spilled.deferred_runs.push(run);
				self.account.clear();
			}
			return Ok(None);
		}
		if !self.seen.insert(&row, self.max_rows)? {
			return Ok(None);
		}
		let bytes = crate::exec::memory::size::value_bytes(&row);
		if self.account.charge(bytes) == Verdict::Spill {
			match self.account.memory().filter(|m| m.can_spill()).cloned() {
				Some(memory) => {
					let seen_count = self.seen.len();
					let rows = self.seen.take_sorted().into_iter().map(|v| (vec![v], Value::None));
					let mut seen = Runs::default();
					seen.push(write_run(&memory, rows, ctx).await?);
					self.account.clear();
					self.spilled = Some(Box::new(Spilled {
						memory,
						seen,
						seen_count,
						deferred: Vec::new(),
						deferred_runs: Runs::default(),
						arrivals: 0,
					}));
				}
				// With no store to spill to the seen rows stay in memory, held
				// as bytes the query cannot spill.
				None => self.account.insist(ctx).await?,
			}
		}
		Ok(Some(row))
	}

	/// The deferred rows to hand on, first seen first, once the input is
	/// finished: a merge of the rows kept, in arrival order. `None` where the
	/// distinct never spilled.
	pub(super) async fn finish(
		&mut self,
		ctx: &ExecutionContext,
	) -> FlowResult<Option<super::spill::Merge>> {
		let Some(spilled) = self.spilled.take() else {
			return Ok(None);
		};
		let Spilled {
			memory,
			seen,
			seen_count,
			mut deferred,
			deferred_runs,
			..
		} = *spilled;
		self.account.clear();
		deferred.sort_by(|(a, _), (b, _)| a.cmp(b));
		// The seen run is read beside the deferred rows' merge, and a run of
		// the rows kept may be written while both are open: one reader fewer
		// for the merge keeps the three within the query's spill headroom.
		// Every pass is made before either is opened.
		let order = by_keys();
		let deferred_runs = deferred_runs.reduce(&memory, &order, memory.fan_in() - 1, ctx).await?;
		let seen = seen.reduce(&memory, &order, 1, ctx).await?;
		let mut before = seen.open(Vec::new(), Arc::clone(&order)).await?;
		let mut rows = deferred_runs.open(deferred, order).await?;
		let mut kept_runs = Runs::default();
		let mut kept: Vec<Record> = Vec::new();
		let mut kept_count = 0usize;
		let mut head = before.next(ctx).await?;
		let mut last: Option<Value> = None;
		while let Some((mut keys, _)) = rows.next(ctx).await? {
			let arrival = keys.pop().unwrap_or(Value::None);
			let row = keys.pop().unwrap_or(Value::None);
			// Rows seen before the spill that order before this one are
			// passed; one equal to it drops it.
			while let Some((seen_keys, _)) = &head
				&& seen_keys[0].cmp(&row) == Ordering::Less
			{
				head = before.next(ctx).await?;
			}
			let seen_before = head.as_ref().is_some_and(|(k, _)| k[0].cmp(&row) == Ordering::Equal);
			let repeat = last.as_ref().is_some_and(|l| l.cmp(&row) == Ordering::Equal);
			if seen_before || repeat {
				continue;
			}
			kept_count += 1;
			if seen_count + kept_count > self.max_rows {
				return Err(too_many_distinct(self.max_rows));
			}
			last = Some(row.clone());
			let bytes = crate::exec::memory::size::value_bytes(&row);
			kept.push((vec![arrival], row));
			if self.account.charge(bytes) == Verdict::Spill {
				let mut held = std::mem::take(&mut kept);
				held.sort_by(|(a, _), (b, _)| a.cmp(b));
				kept_runs.push(write_run(&memory, held, ctx).await?);
				self.account.clear();
			}
		}
		drop(rows);
		drop(before);
		kept.sort_by(|(a, _), (b, _)| a.cmp(b));
		self.account.clear();
		let merge = kept_runs.merge(&memory, kept, by_keys(), ctx).await?;
		Ok(Some(merge))
	}
}
