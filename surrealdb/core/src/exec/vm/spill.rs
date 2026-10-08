//! External sort and merge: a breaker's rows held in the query's temporary
//! store once they outgrow its memory.
//!
//! A breaker takes its rows in arrival order. Each time its query's lease no
//! longer covers them (`crate::exec::memory`), or — for a sort under
//! `TEMPFILES` — each time it holds the external sorting buffer limit's rows,
//! the rows are sorted, stably, and written out as one run. When the breaker
//! hands its rows on, the runs and the rows still held are merged: at each
//! step the row that orders first is taken, and of rows that tie, the one
//! from the earlier run, the rows still held last. The merged order is the
//! order sorting every row at once gives.
//!
//! A merge reads each run through one part buffer, and reads no more runs at
//! once than its query's fan-in ([`QueryMemory::fan_in`]). With more runs than
//! that, consecutive runs are merged into one, a fan-in at a time, until few
//! enough remain; merging consecutive runs keeps every tie in arrival order.
//! The merge is a loser tree, `log k` comparisons a row taken over `k`
//! sources, and charges the thread's fuel a unit a row.

use std::cmp::Ordering;
use std::sync::Arc;

use surrealdb_runtime::fuel::LoserTree;

use super::program::KeyOrder;
use super::sort::{Sorted, compare_rows};
use crate::exec::memory::QueryMemory;
use crate::exec::memory::runs::{Record, Run, RunSink, RunSource};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;

/// How many runs this process has written, for tests that must know a
/// breaker spilled.
#[cfg(test)]
pub(super) static RUNS_WRITTEN: std::sync::atomic::AtomicUsize =
	std::sync::atomic::AtomicUsize::new(0);

/// How many merge passes this process has made before a final merge, for
/// tests that must know a merge took several.
#[cfg(test)]
pub(super) static MERGE_PASSES: std::sync::atomic::AtomicUsize =
	std::sync::atomic::AtomicUsize::new(0);

/// The order two records take in a merge.
pub(crate) type RecordOrder = Arc<dyn Fn(&Record, &Record) -> Ordering + Send + Sync>;

/// The order of a sort's records: by their keys under `keys`, then their
/// arrival where a sort stamps one.
pub(crate) fn sort_order(keys: Vec<KeyOrder>) -> RecordOrder {
	Arc::new(move |(a, _), (b, _)| compare_rows(a, b, &keys))
}

fn err(e: anyhow::Error) -> ControlFlow {
	ControlFlow::Err(e)
}

/// Write `records`, already in order, as one run.
pub(crate) async fn write_run(
	memory: &Arc<QueryMemory>,
	records: impl IntoIterator<Item = Record>,
	ctx: &ExecutionContext,
) -> FlowResult<Run> {
	let mut sink = RunSink::open(memory).await.map_err(err)?;
	let mut taken = 0;
	for (keys, row) in records {
		sink.push(&keys, &row).await.map_err(err)?;
		taken += 1;
		if taken >= super::fuel::left().max(1) {
			super::fuel::tick(ctx, taken).await?;
			taken = 0;
		}
	}
	super::fuel::tick(ctx, taken).await?;
	let run = sink.finish().await.map_err(err)?;
	#[cfg(test)]
	RUNS_WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	Ok(run)
}

/// The runs one breaker has written, in the order it wrote them.
#[derive(Debug, Default)]
pub(crate) struct Runs {
	runs: Vec<Run>,
}

impl Runs {
	/// Whether no run has been written.
	pub(crate) fn is_empty(&self) -> bool {
		self.runs.is_empty()
	}

	/// Add `run`, written after every run held.
	pub(crate) fn push(&mut self, run: Run) {
		self.runs.push(run);
	}

	/// Sort a sort's `rows` stably by `keys` and write them as the next run;
	/// `slab` is the sort's slab its slab rows lie in.
	pub(super) async fn spill_sorted(
		&mut self,
		memory: &Arc<QueryMemory>,
		rows: Vec<Sorted>,
		slab: &[u8],
		keys: &[KeyOrder],
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		let rows = super::sort::sorted(rows, keys, usize::MAX, ctx).await?;
		let records = records_of(rows, slab)?;
		self.runs.push(write_run(memory, records, ctx).await?);
		Ok(())
	}

	/// The merge of every run with `held`, the records still in memory, which
	/// arrived after every run, already in `order`.
	pub(crate) async fn merge(
		self,
		memory: &Arc<QueryMemory>,
		held: Vec<Record>,
		order: RecordOrder,
		ctx: &ExecutionContext,
	) -> FlowResult<Merge> {
		let fan_in = memory.fan_in();
		self.reduce(memory, &order, fan_in, ctx).await?.open(held, order).await
	}

	/// These runs merged, in passes of the query's fan-in, until no more
	/// than `readers` (at least one) remain. Merging consecutive runs keeps
	/// every tie in order. A caller that will read several merges at once
	/// reduces each before it opens any, so that no pass runs beside them.
	pub(crate) async fn reduce(
		self,
		memory: &Arc<QueryMemory>,
		order: &RecordOrder,
		readers: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<Self> {
		let fan_in = memory.fan_in();
		let readers = readers.clamp(1, fan_in);
		let mut runs = self.runs;
		while runs.len() > fan_in {
			let mut merged = Vec::with_capacity(runs.len().div_ceil(fan_in));
			let mut rest = runs.into_iter().peekable();
			while rest.peek().is_some() {
				let group: Vec<Run> = rest.by_ref().take(fan_in).collect();
				merged.push(merge_pass(memory, group, order, ctx).await?);
			}
			runs = merged;
		}
		if runs.len() > readers {
			let rest = runs.split_off(runs.len() - readers + 1);
			let first = merge_pass(memory, runs, order, ctx).await?;
			runs = std::iter::once(first).chain(rest).collect();
		}
		Ok(Self {
			runs,
		})
	}

	/// The merge of these runs with `held`, the records still in memory,
	/// every run read at once.
	pub(crate) async fn open(self, held: Vec<Record>, order: RecordOrder) -> FlowResult<Merge> {
		Merge::open(self.runs, held, order).await
	}
}

/// Merge `group`, consecutive runs, into one run; a single run is answered
/// as it is.
async fn merge_pass(
	memory: &Arc<QueryMemory>,
	mut group: Vec<Run>,
	order: &RecordOrder,
	ctx: &ExecutionContext,
) -> FlowResult<Run> {
	if group.len() == 1 {
		return Ok(group.remove(0));
	}
	#[cfg(test)]
	MERGE_PASSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let mut pass = Merge::open(group, Vec::new(), Arc::clone(order)).await?;
	let mut sink = RunSink::open(memory).await.map_err(err)?;
	while let Some((keys, row)) = pass.next(ctx).await? {
		sink.push(&keys, &row).await.map_err(err)?;
	}
	sink.finish().await.map_err(err)
}

/// Where a merge reads records from: a run, or the records held in memory.
enum Source {
	Run(RunSource),
	Held(std::vec::IntoIter<Record>),
}

impl Source {
	async fn next(&mut self) -> FlowResult<Option<Record>> {
		match self {
			Source::Run(run) => run.next().await.map_err(err),
			Source::Held(rows) => Ok(rows.next()),
		}
	}
}

/// The merged records of a breaker's runs and the records it still held.
pub(crate) struct Merge {
	/// Held so the runs are deleted only once the merge is done with them.
	_runs: Vec<Run>,
	sources: Vec<Source>,
	/// Each source's next record, `None` once the source is spent.
	heads: Vec<Option<Record>>,
	tree: LoserTree,
	order: RecordOrder,
}

impl std::fmt::Debug for Merge {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Merge").field("sources", &self.sources.len()).finish()
	}
}

/// Whether source `a`'s head orders before source `b`'s: a spent source
/// orders after every other, and the earlier source wins a tie.
fn beats(heads: &[Option<Record>], order: &RecordOrder, a: usize, b: usize) -> bool {
	match (&heads[a], &heads[b]) {
		(Some(x), Some(y)) => match order(x, y) {
			Ordering::Less => true,
			Ordering::Greater => false,
			Ordering::Equal => a < b,
		},
		(Some(_), None) => true,
		(None, _) => false,
	}
}

impl Merge {
	async fn open(runs: Vec<Run>, held: Vec<Record>, order: RecordOrder) -> FlowResult<Self> {
		let mut sources = Vec::with_capacity(runs.len() + 1);
		for run in &runs {
			sources.push(Source::Run(run.read().await.map_err(err)?));
		}
		if !held.is_empty() {
			sources.push(Source::Held(held.into_iter()));
		}
		let mut heads = Vec::with_capacity(sources.len());
		for source in &mut sources {
			heads.push(source.next().await?);
		}
		let tree = LoserTree::new(heads.len(), |a, b| beats(&heads, &order, a, b));
		Ok(Self {
			_runs: runs,
			sources,
			heads,
			tree,
			order,
		})
	}

	/// The next record in order, or `None` once every record is taken.
	pub(crate) async fn next(&mut self, ctx: &ExecutionContext) -> FlowResult<Option<Record>> {
		if self.heads.is_empty() {
			return Ok(None);
		}
		let winner = self.tree.winner();
		let Some(record) = self.heads[winner].take() else {
			return Ok(None);
		};
		self.heads[winner] = self.sources[winner].next().await?;
		let (heads, order) = (&self.heads, &self.order);
		self.tree.replay(winner, heads.len(), |a, b| beats(heads, order, a, b));
		super::fuel::tick(ctx, 1).await?;
		Ok(Some(record))
	}

	/// Up to `count` further records in order; empty once every record is
	/// taken.
	pub(crate) async fn next_chunk(
		&mut self,
		count: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<Vec<Record>> {
		let mut chunk = Vec::with_capacity(count.min(1024));
		while chunk.len() < count {
			match self.next(ctx).await? {
				Some(record) => chunk.push(record),
				None => break,
			}
		}
		Ok(chunk)
	}
}

/// A sort's rows as records, each decoded; a slab row is read from `slab`,
/// its sort's slab.
pub(super) fn records_of(rows: Vec<Sorted>, slab: &[u8]) -> FlowResult<Vec<Record>> {
	rows.into_iter().map(|(keys, row)| Ok((keys, row.into_value_in(slab).map_err(err)?))).collect()
}
