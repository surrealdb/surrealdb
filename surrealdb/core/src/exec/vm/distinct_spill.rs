//! A GQL `DISTINCT` that spills: the rows it has seen held in the query's
//! temporary store once its lease does not cover them, with its output in
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
//!
//! A distinct whose rows several producers take ([`DistinctProducer`]) defers
//! every row: each is kept as `[row, producer, arrival]`, and the producers'
//! partials merge ([`merge_distinct`]) into the first arrival of every row, in
//! the order a serial distinct over the producers' rows in producer order
//! answers them.

// Runtime the IR lowering emits.
#![allow(dead_code)]

use std::cmp::Ordering;
use std::sync::Arc;

use super::partial::{Partial, PartialCounts, concat};
use super::spill::{RecordOrder, Runs, write_run};
use crate::exec::memory::runs::{BreakerKind, Record, RunOrder, RunSpec};
use crate::exec::memory::{QueryMemory, Reservation, Verdict};
use crate::exec::operators::distinct::{SeenSet, too_many_distinct};
use crate::exec::{ExecutionContext, FlowResult};
use crate::val::Value;

/// The order of records by their keys: a seen row, a deferred row and its
/// arrival, or a kept row's arrival.
pub(super) fn by_keys() -> RecordOrder {
	Arc::new(|(a, _), (b, _)| a.cmp(b))
}

/// The spec of a distinct's run of `kind` with `arity` keys. A distinct is
/// one step of its program, so its runs have no index of their own.
fn spec(kind: BreakerKind, arity: usize) -> RunSpec {
	RunSpec::new(kind, 0, arity, RunOrder::Plain)
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
				let spec = spec(BreakerKind::DistinctDeferred, 2);
				let run = write_run(&spilled.memory, spec, held, ctx).await?;
				spilled.deferred_runs.push(run)?;
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
					seen.push(
						write_run(&memory, spec(BreakerKind::DistinctSeen, 1), rows, ctx).await?,
					)?;
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
		let deferred_runs =
			deferred_runs.reduce(&memory, &order, memory.fan_in() - 1, None, ctx).await?;
		let seen = seen.reduce(&memory, &order, 1, None, ctx).await?;
		let mut before = seen.open(Vec::new(), Arc::clone(&order), None).await?;
		let mut rows = deferred_runs.open(deferred, order, None).await?;
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
				let spec = spec(BreakerKind::DistinctKept, 1);
				kept_runs.push(write_run(&memory, spec, held, ctx).await?)?;
				self.account.clear();
			}
		}
		drop(rows);
		drop(before);
		kept.sort_by(|(a, _), (b, _)| a.cmp(b));
		self.account.clear();
		let merge = kept_runs.merge(&memory, kept, by_keys(), None, ctx).await?;
		Ok(Some(merge))
	}
}

/// One producer's side of a distinct whose rows several producers take: each
/// row it takes is kept as `[row, producer, arrival]`; runs are written
/// sorted by that key with all but the first arrival of each row dropped,
/// once its lease does not cover what it holds.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct DistinctProducer {
	producer: i64,
	arrivals: i64,
	held: Vec<Record>,
	runs: Runs,
	account: Reservation,
	/// `ctx`'s memory where it can spill.
	memory: Option<Arc<QueryMemory>>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl DistinctProducer {
	/// Producer `producer`, charging `ctx`'s query where it can spill.
	pub(super) fn new(producer: u32, ctx: &ExecutionContext) -> Self {
		let memory = ctx.ctx().memory().filter(|memory| memory.can_spill()).cloned();
		Self {
			producer: i64::from(producer),
			arrivals: 0,
			held: Vec::new(),
			runs: Runs::default(),
			account: match &memory {
				Some(memory) => memory.reservation(),
				None => Reservation::unbounded(),
			},
			memory,
		}
	}

	/// Take `row`, which arrives after every row this producer has taken.
	pub(super) async fn take(&mut self, row: Value, ctx: &ExecutionContext) -> FlowResult<()> {
		let arrival = self.arrivals;
		self.arrivals += 1;
		let bytes = crate::exec::memory::size::value_bytes(&row);
		self.held.push((vec![row, Value::from(self.producer), Value::from(arrival)], Value::None));
		if self.account.charge(bytes) == Verdict::Spill && self.memory.is_some() {
			self.spill(ctx).await?;
		}
		Ok(())
	}

	/// Write the rows held as the next run and release what they were charged.
	async fn spill(&mut self, ctx: &ExecutionContext) -> FlowResult<()> {
		let Some(memory) = self.memory.clone() else {
			return Ok(());
		};
		let held = first_arrivals(std::mem::take(&mut self.held));
		let run = write_run(&memory, spec(BreakerKind::DistinctDeferred, 3), held, ctx).await?;
		self.runs.push(run)?;
		self.account.clear();
		Ok(())
	}

	/// The rows taken as a partial: its runs, and the rows still held sorted
	/// with all but the first arrival of each row dropped.
	pub(super) fn into_partial(mut self) -> Partial {
		let held = first_arrivals(std::mem::take(&mut self.held));
		self.account.clear();
		Partial {
			spec: spec(BreakerKind::DistinctDeferred, 3),
			runs: self.runs,
			held,
			side: Vec::new(),
			counts: PartialCounts {
				taken: self.arrivals as u64,
			},
		}
	}
}

/// `records`, keyed `[row, producer, arrival]`, sorted by key with every
/// record dropped whose row equals the row before it: the first of each equal
/// run is the earliest arrival.
#[cfg_attr(not(test), allow(dead_code))]
fn first_arrivals(mut records: Vec<Record>) -> Vec<Record> {
	records.sort_by(|(a, _), (b, _)| a.cmp(b));
	let mut last: Option<Value> = None;
	records.retain(|(keys, _)| {
		if last.as_ref().is_some_and(|l| l.cmp(&keys[0]) == Ordering::Equal) {
			return false;
		}
		last = Some(keys[0].clone());
		true
	});
	records
}

/// The first arrival of every row across `partials`, in arrival order: the
/// rows a serial distinct over the producers' rows in producer order
/// answers. Fails past `max_rows` kept rows as the in-memory set does. The
/// merge's records are `([producer, arrival], row)`.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) async fn merge_distinct(
	memory: &Arc<QueryMemory>,
	partials: Vec<Partial>,
	max_rows: usize,
	ctx: &ExecutionContext,
) -> FlowResult<super::spill::Merge> {
	merge_distinct_counted(memory, partials, max_rows, ctx).await.map(|(merge, _)| merge)
}

/// [`merge_distinct`], also answering how many rows it kept.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) async fn merge_distinct_counted(
	memory: &Arc<QueryMemory>,
	partials: Vec<Partial>,
	max_rows: usize,
	ctx: &ExecutionContext,
) -> FlowResult<(super::spill::Merge, usize)> {
	let (runs, held, _) = concat(memory, partials, None, false, ctx).await?;
	// One reader is left free for the run of kept rows being written.
	let order = by_keys();
	let runs = runs.reduce(memory, &order, memory.fan_in() - 1, None, ctx).await?;
	let mut input = runs.open(held, order, None).await?;
	let mut account = memory.reservation();
	let mut kept_runs = Runs::default();
	let mut kept: Vec<Record> = Vec::new();
	let mut kept_count = 0usize;
	let mut last: Option<Value> = None;
	while let Some((keys, _)) = input.next(ctx).await? {
		let mut keys = keys.into_iter();
		let (Some(row), Some(producer), Some(arrival)) = (keys.next(), keys.next(), keys.next())
		else {
			return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
				"Internal error: a distinct partial's record has too few keys"
			)));
		};
		if last.as_ref().is_some_and(|l| l.cmp(&row) == Ordering::Equal) {
			continue;
		}
		kept_count += 1;
		if kept_count > max_rows {
			return Err(too_many_distinct(max_rows));
		}
		last = Some(row.clone());
		let bytes = crate::exec::memory::size::value_bytes(&row);
		kept.push((vec![producer, arrival], row));
		if account.charge(bytes) == Verdict::Spill {
			let mut held = std::mem::take(&mut kept);
			held.sort_by(|(a, _), (b, _)| a.cmp(b));
			let run = write_run(memory, spec(BreakerKind::DistinctKept, 2), held, ctx).await?;
			kept_runs.push(run)?;
			account.clear();
		}
	}
	drop(input);
	kept.sort_by(|(a, _), (b, _)| a.cmp(b));
	account.clear();
	let merge = kept_runs.merge(memory, kept, by_keys(), None, ctx).await?;
	Ok((merge, kept_count))
}
