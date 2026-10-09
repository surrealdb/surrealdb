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
//!
//! A merge with a limit answers the first `limit` records of the merge
//! without one. Every pass and every run is cut to the limit, which is exact:
//! any record after a run's first `limit` has `limit` records of its own run
//! at or before it, each of which wins a tie by source order, so it is never
//! among the first `limit`. The earlier source wins every tie, and passes
//! merge consecutive runs, so a cut pass keeps what the whole merge would.

use std::cmp::Ordering;
use std::sync::Arc;

use surrealdb_runtime::fuel::LoserTree;

use super::program::KeyOrder;
use super::sort::{Sorted, compare_rows};
use crate::exec::memory::runs::{Record, Run, RunSink, RunSource, RunSpec};
use crate::exec::memory::{QueryMemory, Sharing};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

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

#[cfg(test)]
thread_local! {
	/// The most records a merge pass has written on this thread, for tests
	/// that must know a pass was cut to a limit.
	pub(super) static LARGEST_PASS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
	/// How many times a spilled floor has been set on this thread, for tests
	/// that must know a sort's runs gave it one.
	pub(super) static FLOORS_SET: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

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

/// The order bytes a run's header carries for `keys`.
pub(crate) fn key_order_bytes(keys: &[KeyOrder]) -> Vec<u8> {
	keys.iter()
		.map(|key| {
			u8::from(key.ascending) | (u8::from(key.collate) << 1) | (u8::from(key.numeric) << 2)
		})
		.collect()
}

/// Write `records`, already in order, as one run written for `spec`.
pub(crate) async fn write_run(
	memory: &Arc<QueryMemory>,
	spec: RunSpec,
	records: impl IntoIterator<Item = Record>,
	ctx: &ExecutionContext,
) -> FlowResult<Run> {
	let mut sink = RunSink::open(memory, spec, Sharing::Private).await.map_err(err)?;
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

	/// Add `run`, written after every run held. A run written for another
	/// breaker than the runs held is an internal error.
	pub(crate) fn push(&mut self, run: Run) -> FlowResult<()> {
		if let Some(first) = self.runs.first()
			&& first.spec() != run.spec()
		{
			return Err(err(anyhow::anyhow!("Internal error: runs of two breakers were merged")));
		}
		self.runs.push(run);
		Ok(())
	}

	/// Sort a sort's `rows` stably by `keys`, keep the first `limit`, and write
	/// them as the next run, written for `spec`; `slab` is the sort's slab its
	/// slab rows lie in. Answers the keys of the run's last record when the
	/// run holds exactly `limit` records.
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn spill_sorted(
		&mut self,
		memory: &Arc<QueryMemory>,
		spec: RunSpec,
		rows: Vec<Sorted>,
		slab: &[u8],
		keys: &[KeyOrder],
		limit: Option<usize>,
		ctx: &ExecutionContext,
	) -> FlowResult<Option<Vec<Value>>> {
		let rows = super::sort::sorted(rows, keys, limit.unwrap_or(usize::MAX), ctx).await?;
		let records = records_of(rows, slab)?;
		let floor = match records.last() {
			Some((last, _)) if limit == Some(records.len()) => Some(last.clone()),
			_ => None,
		};
		self.push(write_run(memory, spec, records, ctx).await?)?;
		Ok(floor)
	}

	/// The records of every run, in the order the runs were written, as each
	/// run counts them.
	#[cfg(test)]
	pub(crate) fn records(&self) -> Vec<u64> {
		self.runs.iter().map(Run::records).collect()
	}

	/// The merge of every run with `held`, the records still in memory, which
	/// arrived after every run, already in `order`; the first `limit` records
	/// of it where there is a limit.
	pub(crate) async fn merge(
		self,
		memory: &Arc<QueryMemory>,
		held: Vec<Record>,
		order: RecordOrder,
		limit: Option<usize>,
		ctx: &ExecutionContext,
	) -> FlowResult<Merge> {
		let fan_in = memory.fan_in();
		self.reduce(memory, &order, fan_in, limit, ctx).await?.open(held, order, limit).await
	}

	/// These runs merged, in passes of the query's fan-in, until no more
	/// than `readers` (at least one) remain. Merging consecutive runs keeps
	/// every tie in order. A caller that will read several merges at once
	/// reduces each before it opens any, so that no pass runs beside them.
	/// Each pass writes at most `limit` records where there is a limit.
	pub(crate) async fn reduce(
		self,
		memory: &Arc<QueryMemory>,
		order: &RecordOrder,
		readers: usize,
		limit: Option<usize>,
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
				merged.push(merge_pass(memory, group, order, limit, ctx).await?);
			}
			runs = merged;
		}
		if runs.len() > readers {
			let rest = runs.split_off(runs.len() - readers + 1);
			let first = merge_pass(memory, runs, order, limit, ctx).await?;
			runs = std::iter::once(first).chain(rest).collect();
		}
		Ok(Self {
			runs,
		})
	}

	/// The merge of these runs with `held`, the records still in memory,
	/// every run read at once, cut to its first `limit` records where there is
	/// a limit.
	pub(crate) async fn open(
		self,
		held: Vec<Record>,
		order: RecordOrder,
		limit: Option<usize>,
	) -> FlowResult<Merge> {
		Merge::open(self.runs, held, order, limit).await
	}
}

/// Merge `group`, consecutive runs, into one run of the runs' spec, cut to
/// `limit` records; a single run is answered as it is.
async fn merge_pass(
	memory: &Arc<QueryMemory>,
	mut group: Vec<Run>,
	order: &RecordOrder,
	limit: Option<usize>,
	ctx: &ExecutionContext,
) -> FlowResult<Run> {
	if group.len() == 1 {
		return Ok(group.remove(0));
	}
	#[cfg(test)]
	MERGE_PASSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let spec = group[0].spec().clone();
	let mut pass = Merge::open(group, Vec::new(), Arc::clone(order), limit).await?;
	let mut sink = RunSink::open(memory, spec, Sharing::Private).await.map_err(err)?;
	while let Some((keys, row)) = pass.next(ctx).await? {
		sink.push(&keys, &row).await.map_err(err)?;
	}
	let run = sink.finish().await.map_err(err)?;
	#[cfg(test)]
	LARGEST_PASS.with(|largest| largest.set(largest.get().max(run.records())));
	Ok(run)
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
	/// The records the merge may still answer.
	left: usize,
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
	async fn open(
		runs: Vec<Run>,
		held: Vec<Record>,
		order: RecordOrder,
		limit: Option<usize>,
	) -> FlowResult<Self> {
		let left = limit.unwrap_or(usize::MAX);
		let mut sources = Vec::with_capacity(runs.len() + 1);
		if left > 0 {
			for run in &runs {
				sources.push(Source::Run(run.read().await.map_err(err)?));
			}
			if !held.is_empty() {
				sources.push(Source::Held(held.into_iter()));
			}
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
			left,
		})
	}

	/// The next record in order, or `None` once every record is taken.
	pub(crate) async fn next(&mut self, ctx: &ExecutionContext) -> FlowResult<Option<Record>> {
		if self.heads.is_empty() || self.left == 0 {
			return Ok(None);
		}
		let winner = self.tree.winner();
		let Some(record) = self.heads[winner].take() else {
			return Ok(None);
		};
		self.left -= 1;
		if self.left > 0 {
			self.heads[winner] = self.sources[winner].next().await?;
			let (heads, order) = (&self.heads, &self.order);
			self.tree.replay(winner, heads.len(), |a, b| beats(heads, order, a, b));
		}
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

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::*;
	use crate::exec::memory::runs::{BreakerKind, RunOrder};
	use crate::exec::memory::{MemTempStore, MemoryHost, MemorySettings, TempStore, Unlimited};
	use crate::exec::operators::test_util::TestDb;

	fn memory_over(store: Arc<dyn TempStore>, fan_in: usize) -> Arc<QueryMemory> {
		MemoryHost {
			governor: Arc::new(Unlimited),
			store: Some(store),
			settings: MemorySettings {
				floor: 1 << 20,
				fan_in,
				chunk: 4096,
				wait: std::time::Duration::ZERO,
				max: usize::MAX,
			},
		}
		.query()
	}

	fn spec(kind: BreakerKind) -> RunSpec {
		RunSpec::new(kind, 0, 1, RunOrder::Plain)
	}

	fn order() -> RecordOrder {
		Arc::new(|(a, _), (b, _)| a.cmp(b))
	}

	/// `runs` runs of `per_run` records each, sorted by key and stable, the
	/// rows telling the run and the place.
	async fn runs_of(
		memory: &Arc<QueryMemory>,
		runs: usize,
		per_run: usize,
		ctx: &ExecutionContext,
	) -> Runs {
		let mut out = Runs::default();
		for r in 0..runs {
			let mut records: Vec<Record> = (0..per_run)
				.map(|i| (vec![Value::from((i % 5) as i64)], Value::from((r * 1000 + i) as i64)))
				.collect();
			records.sort_by(|(a, _), (b, _)| a.cmp(b));
			let run =
				write_run(memory, spec(BreakerKind::Sort), records, ctx).await.expect("a run");
			out.push(run).expect("one breaker");
		}
		out
	}

	fn held_of(count: usize) -> Vec<Record> {
		let mut held: Vec<Record> = (0..count)
			.map(|i| (vec![Value::from((i % 5) as i64)], Value::from(-(i as i64))))
			.collect();
		held.sort_by(|(a, _), (b, _)| a.cmp(b));
		held
	}

	async fn drained(mut merge: Merge, ctx: &ExecutionContext) -> Vec<Record> {
		let mut all = Vec::new();
		loop {
			let chunk = merge.next_chunk(64, ctx).await.expect("a chunk");
			if chunk.is_empty() {
				return all;
			}
			all.extend(chunk);
		}
	}

	#[tokio::test]
	async fn a_merge_with_a_limit_answers_the_first_records_of_the_full_merge() {
		let db = TestDb::new_with_config("", surrealdb_cnf::ConfigMap::empty()).await;
		let ctx = db.exec_ctx().await;
		let store = Arc::new(MemTempStore::new(64, None));
		let memory = memory_over(Arc::clone(&store) as Arc<dyn TempStore>, 4);
		let runs = runs_of(&memory, 10, 50, &ctx).await;
		let merge = runs.merge(&memory, held_of(7), order(), None, &ctx).await.expect("a merge");
		let full = drained(merge, &ctx).await;
		assert_eq!(full.len(), 507);
		for limit in [0, 1, 7, 49, 50, 51, 120, 499, 500, 507, 10_000] {
			let runs = runs_of(&memory, 10, 50, &ctx).await;
			let merge =
				runs.merge(&memory, held_of(7), order(), Some(limit), &ctx).await.expect("a merge");
			let got = drained(merge, &ctx).await;
			let want: Vec<Record> = full.iter().take(limit).cloned().collect();
			assert_eq!(got, want, "limit {limit}");
		}
		assert_eq!(store.runs(), 0);
	}

	#[tokio::test]
	async fn a_pass_with_a_limit_writes_no_more_than_the_limit() {
		let db = TestDb::new_with_config("", surrealdb_cnf::ConfigMap::empty()).await;
		let ctx = db.exec_ctx().await;
		let store = Arc::new(MemTempStore::new(64, None));
		let memory = memory_over(Arc::clone(&store) as Arc<dyn TempStore>, 4);
		let runs = runs_of(&memory, 10, 50, &ctx).await;
		let runs = runs.reduce(&memory, &order(), 1, Some(30), &ctx).await.expect("reduced");
		let counts = runs.records();
		assert_eq!(counts.len(), 1);
		assert!(counts[0] <= 30, "{counts:?}");
	}

	#[tokio::test]
	async fn a_merge_with_a_limit_reads_only_the_parts_it_needs() {
		let db = TestDb::new_with_config("", surrealdb_cnf::ConfigMap::empty()).await;
		let ctx = db.exec_ctx().await;
		let store = Arc::new(MemTempStore::new(256, None));
		let memory = memory_over(Arc::clone(&store) as Arc<dyn TempStore>, 4);
		let runs = runs_of(&memory, 4, 1000, &ctx).await;
		let written = store.parts_written();
		let before = store.parts_read();
		let merge =
			runs.merge(&memory, Vec::new(), order(), Some(10), &ctx).await.expect("a merge");
		let got = drained(merge, &ctx).await;
		assert_eq!(got.len(), 10);
		assert!(store.parts_read() - before <= 12, "{}", store.parts_read() - before);
		assert!(written >= 100, "{written}");
	}

	#[tokio::test]
	async fn runs_of_two_breakers_do_not_merge() {
		let db = TestDb::new_with_config("", surrealdb_cnf::ConfigMap::empty()).await;
		let ctx = db.exec_ctx().await;
		let store = Arc::new(MemTempStore::new(64, None));
		let memory = memory_over(store as Arc<dyn TempStore>, 4);
		let mut runs = Runs::default();
		let sort =
			write_run(&memory, spec(BreakerKind::Sort), Vec::new(), &ctx).await.expect("a run");
		runs.push(sort).expect("the first run");
		let groups =
			write_run(&memory, spec(BreakerKind::Groups), Vec::new(), &ctx).await.expect("a run");
		let Err(ControlFlow::Err(error)) = runs.push(groups) else {
			panic!("two breakers' runs were taken");
		};
		assert!(error.to_string().contains("Internal error"), "{error}");
	}
}
