//! Range partitions of a keyed breaker's partials. The key space is cut into
//! `N` ranges. Each range is finished on its own, and the finished ranges are
//! put together in key order.
//!
//! The unit that is partitioned is a [`Partial`], never a live breaker. A
//! producer finishes its breaker into a partial. [`Partial::split`] cuts that
//! partial into one partial per partition. Partition `i` is then finished by
//! the code that finishes unpartitioned partials, using the `i`-th share of
//! every producer, in producer order:
//! - an aggregate uses [`merge_partials`] as a keyed fold;
//! - a distinct uses [`merge_distinct_counted`];
//! - a join uses [`SpilledJoin::from_partials`], then `defer` and `finish`.
//!
//! Partitioning is opt-in. A caller that holds partials picks the
//! [`Splitters`] and the partition count itself.
//!
//! The route of a key is the number of splitters that order strictly before it
//! under `Ord for [Value]`. Splitters are strictly ascending under that order.
//! Keys that compare `Equal` therefore route to the same partition. Groups,
//! distinct and joins all compare keys with `Value`'s `Ord`, so equal keys
//! always meet in one partition. The route is monotone: a key that orders
//! before another never routes after it. Splitters only affect balance. They
//! never change the answers.
//!
//! Only the columns that a breaker's equality compares are routed on
//! ([`route_width`]): a group's whole key, a distinct's row, a join's key
//! columns. A producer index or a record's arrival never decides the
//! partition. So every arrival of a key meets in one partition, and that
//! partition's merge breaks ties by source order, as an unpartitioned merge
//! does. A sort is not partitioned.
//!
//! A partial is split by reading each of its runs once, in order. The records
//! for each partition are written as a piece of that partition's run. A
//! source run is deleted as soon as it has been read. A query has one run
//! writer, so only one piece is open at a time. A piece stays open while
//! consecutive records route to the same partition. When the key ranges are
//! contiguous in a run, that means the whole run. Records that a partial still
//! holds in memory are routed in memory. A join's `side` records are build rows
//! whose key holds a regex. They can match across key ranges, so they are copied
//! to every partition.
//!
//! Partitions of an aggregate finish in index order, which is key order. Their
//! groups concatenate into the same result as the unpartitioned aggregate
//! ([`PartitionedFold`]). `GROUP ALL` has the empty key. That key orders before
//! or equal to every splitter, so its one group routes to partition 0, and the
//! other partitions finish with no rows. Partitions of a distinct and of a join
//! finish into runs keyed `[producer, arrival]` and `[probe arrival, place]`.
//! Each key is unique per record, so one merge by key restores the order of the
//! unpartitioned finish.
//!
//! A distinct's `max_rows` is checked against each partition's kept rows and
//! against their sum. It fails exactly when the unpartitioned distinct fails,
//! with the same error. A join's build-row cap is checked on the producers'
//! counts before anything is split. Probe rows are numbered across all
//! partitions, so the output cap counts the same rows as the unpartitioned join.
//! A probe row with no key yields at once and is deferred to partition 0. A
//! probe row whose key holds a regex matches build rows in every range. So the
//! join's partitions are united ([`SpilledJoin::union`]) and finished as one.
//!
//! `Value::cmp` is a total preorder over every value: `partial_cmp` always has
//! an answer, since numbers of every kind compare exactly and a geometry is
//! totally ordered, NaN coordinates included. So two keys that compare `Equal`
//! always route to the same partition, and a partition groups, deduplicates and
//! joins its keys as the unpartitioned breaker does. A closure compares `Equal`
//! to every other closure, whatever either holds, so closures always route
//! together and form one group, one distinct row or one join key, as they do
//! unpartitioned.

// Items in this module may have no caller in the current build.
#![allow(dead_code)]

use std::cmp::Ordering;
use std::sync::Arc;

use super::distinct_spill::{by_keys, merge_distinct_counted};
use super::groups::SavedFold;
use super::join_spill::{Pairer, SpilledJoin};
use super::partial::{Merged, Partial, PartialCounts, PartialMerge, merge_partials};
use super::spill::{Merge, Runs, write_merge};
use crate::exec::function::AggregateFunction;
use crate::exec::memory::runs::{BreakerKind, RunOrder, RunSink, RunSpec};
use crate::exec::memory::{QueryMemory, Sharing};
use crate::exec::operators::distinct::too_many_distinct;
use crate::exec::operators::join::hash_join::{JoinKey, key_has_regex, too_many_build_rows};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

fn err(e: anyhow::Error) -> ControlFlow {
	ControlFlow::Err(e)
}

fn internal(message: &str) -> ControlFlow {
	err(anyhow::anyhow!("Internal error: {message}"))
}

/// The N−1 keys that divide a keyed breaker's key space into N ranges.
#[derive(Debug, Clone, Default)]
pub(crate) struct Splitters {
	/// Strictly ascending by `Ord for [Value]`; no two compare equal.
	keys: Vec<Vec<Value>>,
}

impl Splitters {
	/// One partition: every key routes to 0.
	pub(crate) fn single() -> Self {
		Self::default()
	}

	/// Splitters `keys`, sorted (stable) and with every key that compares
	/// equal to the one before it dropped.
	pub(crate) fn new(mut keys: Vec<Vec<Value>>) -> Self {
		keys.sort();
		keys.dedup_by(|later, earlier| (*later).cmp(&*earlier) == Ordering::Equal);
		Self {
			keys,
		}
	}

	/// At most `n` partitions drawn from producers' sorted samples: the
	/// samples' keys sorted and deduplicated, then the keys at `i * len / n`
	/// for `i` in `1..n`, skipping the first key and repeats.
	pub(crate) fn from_samples(samples: Vec<Vec<Vec<Value>>>, n: usize) -> Self {
		if n <= 1 {
			return Self::single();
		}
		let all = Self::new(samples.into_iter().flatten().collect()).keys;
		let len = all.len();
		let mut keys = Vec::new();
		let mut last = None;
		for i in 1..n {
			let at = (i as u128 * len as u128 / n as u128) as usize;
			if at == 0 || Some(at) == last {
				continue;
			}
			keys.push(all[at].clone());
			last = Some(at);
		}
		Self {
			keys,
		}
	}

	/// How many partitions: one more than there are splitters.
	pub(crate) fn partitions(&self) -> usize {
		self.keys.len() + 1
	}

	pub(crate) fn keys(&self) -> &[Vec<Value>] {
		&self.keys
	}

	/// The partition of `key`: how many splitters order strictly before it.
	pub(crate) fn route(&self, key: &[Value]) -> usize {
		self.keys.partition_point(|s| s.as_slice().cmp(key) == Ordering::Less)
	}
}

/// How many leading keys of a record of `spec` decide its partition: the
/// columns the breaker's equality compares, never a producer or arrival.
pub(crate) fn route_width(spec: &RunSpec) -> FlowResult<usize> {
	let arity = spec.arity as usize;
	match spec.kind {
		BreakerKind::Groups => Ok(arity),
		BreakerKind::DistinctDeferred if arity == 3 => Ok(1),
		BreakerKind::JoinBuild if arity >= 2 => Ok(arity - 2),
		BreakerKind::Sort
		| BreakerKind::DistinctSeen
		| BreakerKind::DistinctDeferred
		| BreakerKind::DistinctKept
		| BreakerKind::JoinBuild
		| BreakerKind::JoinProbe
		| BreakerKind::JoinOut
		| BreakerKind::WindowBuffer => Err(internal("a breaker of this kind is not partitioned")),
	}
}

/// The first `width` of a record's `keys`, which decide its partition.
fn prefix(keys: &[Value], width: usize) -> FlowResult<&[Value]> {
	keys.get(..width).ok_or_else(|| internal("a record has fewer keys than its route"))
}

/// Finish `open`, the piece being written, into its partition's runs.
async fn close(parts: &mut [Partial], open: &mut Option<(usize, RunSink)>) -> FlowResult<()> {
	if let Some((at, sink)) = open.take() {
		let run = sink.finish().await.map_err(err)?;
		#[cfg(test)]
		super::spill::RUNS_WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		parts[at].runs.push(run)?;
	}
	Ok(())
}

impl Partial {
	/// Every `stride`-th route key of this partial's first run and of its held
	/// records, sorted: the producer's sample for [`Splitters::from_samples`].
	pub(crate) async fn sample(
		&self,
		stride: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<Vec<Vec<Value>>> {
		let width = route_width(&self.spec)?;
		let stride = stride.max(1);
		let mut sample = Vec::new();
		if let Some(run) = self.runs.runs().first() {
			let mut source = run.read().await.map_err(err)?;
			let mut at = 0usize;
			while let Some((keys, _)) = source.next().await.map_err(err)? {
				if at.is_multiple_of(stride) {
					sample.push(prefix(&keys, width)?.to_vec());
				}
				at += 1;
				super::fuel::tick(ctx, 1).await?;
			}
		}
		for (at, (keys, _)) in self.held.iter().enumerate() {
			if at.is_multiple_of(stride) {
				sample.push(prefix(keys, width)?.to_vec());
			}
		}
		sample.sort();
		Ok(sample)
	}

	/// This partial cut by `splitters` into one partial per partition, in partition
	/// order. Partition `i` gets, in this partial's order, the records of its runs
	/// that route to `i`, written as runs through `memory` and readable as `sharing`
	/// says. It also gets the held records that route to `i`. Every partition gets
	/// every `side` record. A split partial's `counts.taken` is the records routed
	/// to it. Partition 0 also counts the side records, so the partitions' counts sum
	/// to this partial's records. For a distinct, these are the deduplicated records
	/// the partial holds, not every row it took. This consumes the partial. Its runs
	/// are deleted as they are read.
	pub(crate) async fn split(
		self,
		memory: &Arc<QueryMemory>,
		splitters: &Splitters,
		sharing: Sharing,
		ctx: &ExecutionContext,
	) -> FlowResult<Vec<Partial>> {
		let width = route_width(&self.spec)?;
		let n = splitters.partitions();
		let Partial {
			spec,
			runs,
			held,
			side,
			..
		} = self;
		let mut parts: Vec<Partial> = (0..n)
			.map(|_| Partial {
				spec: spec.clone(),
				runs: Runs::default(),
				held: Vec::new(),
				side: side.clone(),
				counts: PartialCounts::default(),
			})
			.collect();
		for run in runs.into_runs() {
			let mut source = run.read().await.map_err(err)?;
			let mut open: Option<(usize, RunSink)> = None;
			// The pieces a partition receives from one source run are in the run's
			// order, each a sorted subsequence of it. A route steps back to a
			// partition that already holds a piece of the run only where `Value::cmp`
			// is not transitive; the later piece is pushed after the earlier, so the
			// partition's merge, which breaks every tie by source order, keeps the
			// run's order among them.
			while let Some((keys, row)) = source.next().await.map_err(err)? {
				let to = splitters.route(prefix(&keys, width)?);
				if open.as_ref().is_some_and(|(at, _)| *at != to) {
					close(&mut parts, &mut open).await?;
				}
				if open.is_none() {
					let sink = RunSink::open(memory, spec.clone(), sharing).await.map_err(err)?;
					open = Some((to, sink));
				}
				if let Some((_, sink)) = &mut open {
					sink.push(&keys, &row).await.map_err(err)?;
				}
				parts[to].counts.taken += 1;
				super::fuel::tick(ctx, 1).await?;
			}
			close(&mut parts, &mut open).await?;
			// One reader and one writer are open at a time. Each source run's bytes are
			// released before the next run is read. Unread source runs, and pieces already
			// written for earlier runs, still count against the quota. The split adds at
			// most one source run's bytes above the unsplit partials' footprint: that run
			// plus its pieces. A source run is a whole spill, so this peak can exceed the
			// `part_size * (1 + fan_in)` headroom, as an unpartitioned merge pass can with
			// its input and output runs.
			drop(source);
			drop(run);
		}
		// Held records are routed in memory, in this partial's order.
		for (keys, row) in held {
			let to = splitters.route(prefix(&keys, width)?);
			parts[to].counts.taken += 1;
			parts[to].held.push((keys, row));
		}
		parts[0].counts.taken += side.len() as u64;
		Ok(parts)
	}
}

/// Every producer's partial of one breaker, divided into partitions.
pub(crate) struct Partitioned {
	splitters: Splitters,
	/// `parts[i][p]`: producer `p`'s share of partition `i`, producers in order.
	parts: Vec<Vec<Partial>>,
}

impl Partitioned {
	/// Split each of `producers`, in producer order, by `splitters`. Every
	/// producer's partial must be of one breaker.
	pub(crate) async fn split(
		memory: &Arc<QueryMemory>,
		splitters: Splitters,
		producers: Vec<Partial>,
		sharing: Sharing,
		ctx: &ExecutionContext,
	) -> FlowResult<Self> {
		let Some(first) = producers.first() else {
			return Err(internal("a partition of no partials"));
		};
		if producers.iter().any(|p| p.spec != first.spec) {
			return Err(internal("partials of two breakers were merged"));
		}
		route_width(&first.spec)?;
		let n = splitters.partitions();
		let count = producers.len();
		let mut parts: Vec<Vec<Partial>> = (0..n).map(|_| Vec::with_capacity(count)).collect();
		for producer in producers {
			let pieces = producer.split(memory, &splitters, sharing, ctx).await?;
			for (at, piece) in pieces.into_iter().enumerate() {
				parts[at].push(piece);
			}
		}
		Ok(Self {
			splitters,
			parts,
		})
	}

	pub(crate) fn partitions(&self) -> usize {
		self.parts.len()
	}

	pub(crate) fn route(&self, key: &[Value]) -> usize {
		self.splitters.route(key)
	}

	pub(crate) fn splitters(&self) -> &Splitters {
		&self.splitters
	}

	pub(crate) fn parts(&self) -> &[Vec<Partial>] {
		&self.parts
	}

	pub(crate) fn into_parts(self) -> Vec<Vec<Partial>> {
		self.parts
	}

	/// An aggregate's groups, partition by partition in index order, a chunk
	/// of keys at a time; see [`PartitionedFold`].
	pub(super) fn finish_in_order(
		self,
		memory: &Arc<QueryMemory>,
		keys: usize,
		functions: Vec<Vec<Arc<dyn AggregateFunction>>>,
		batch: usize,
		room: usize,
	) -> PartitionedFold {
		PartitionedFold {
			memory: Arc::clone(memory),
			parts: self.parts.into_iter(),
			current: None,
			keys,
			functions,
			batch,
			room,
		}
	}
}

/// Saved groups of a partitioned aggregate folded partition by partition:
/// every chunk holds keys of one partition, and partitions are finished in
/// index order, which is key order, so the chunks' finished rows in order
/// are the unpartitioned aggregate's.
pub(super) struct PartitionedFold {
	memory: Arc<QueryMemory>,
	parts: std::vec::IntoIter<Vec<Partial>>,
	current: Option<SavedFold>,
	keys: usize,
	functions: Vec<Vec<Arc<dyn AggregateFunction>>>,
	batch: usize,
	room: usize,
}

impl PartitionedFold {
	/// The next chunk of groups, every saved state of each key folded in
	/// source order; `None` once every partition is folded. At most one
	/// partition's merge is open at a time.
	pub(super) async fn next_chunk(
		&mut self,
		ctx: &ExecutionContext,
	) -> FlowResult<Option<super::groups::Groups>> {
		loop {
			if let Some(fold) = &mut self.current {
				if let Some(chunk) = fold.next_chunk(ctx).await? {
					return Ok(Some(chunk));
				}
				self.current = None;
			}
			let Some(part) = self.parts.next() else {
				return Ok(None);
			};
			match merge_partials(&self.memory, &PartialMerge::KeyedFold, part, ctx).await? {
				Merged::Records(merge) => {
					self.current = Some(SavedFold::new(
						merge,
						self.keys,
						self.functions.clone(),
						self.batch,
						self.room,
					));
				}
				Merged::Join(_) => return Err(internal("groups merged as a join")),
			}
		}
	}
}

/// Partition `part` of a distinct, one partial per producer in producer
/// order, finished on its own: the first arrival of each of its rows, as
/// one run keyed `[producer, arrival]` in that order, its `counts.taken` the
/// rows kept. Fails past `max_rows` kept rows of its own.
pub(crate) async fn finish_distinct_partition(
	memory: &Arc<QueryMemory>,
	part: Vec<Partial>,
	max_rows: usize,
	sharing: Sharing,
	ctx: &ExecutionContext,
) -> FlowResult<Partial> {
	let (mut merge, kept) = merge_distinct_counted(memory, part, max_rows, ctx).await?;
	let spec = RunSpec::new(BreakerKind::DistinctKept, 0, 2, RunOrder::Plain);
	let run = write_merge(memory, spec.clone(), &mut merge, sharing, ctx).await?;
	drop(merge);
	let mut runs = Runs::default();
	runs.push(run)?;
	Ok(Partial {
		spec,
		runs,
		held: Vec::new(),
		side: Vec::new(),
		counts: PartialCounts {
			taken: kept as u64,
		},
	})
}

/// The kept rows of every distinct partition, in arrival order: records
/// `([producer, arrival], row)`, as `merge_partials(DistinctFirst)` answers
/// for the unpartitioned producers. Fails past `max_rows` kept rows in all,
/// before any run is read.
pub(crate) async fn merge_distinct_outputs(
	memory: &Arc<QueryMemory>,
	outputs: Vec<Partial>,
	max_rows: usize,
	ctx: &ExecutionContext,
) -> FlowResult<Merge> {
	let kept: u64 = outputs.iter().map(|output| output.counts.taken).sum();
	if kept > max_rows as u64 {
		return Err(too_many_distinct(max_rows));
	}
	merge_outputs(memory, outputs, BreakerKind::DistinctKept, ctx).await
}

/// The k-way merge by key of partition outputs, each runs only, all of one
/// spec of `kind` with two keys.
async fn merge_outputs(
	memory: &Arc<QueryMemory>,
	outputs: Vec<Partial>,
	kind: BreakerKind,
	ctx: &ExecutionContext,
) -> FlowResult<Merge> {
	let Some(first) = outputs.first() else {
		return Err(internal("a merge of no partitions"));
	};
	if outputs.iter().any(|output| {
		output.spec != first.spec
			|| output.spec.kind != kind
			|| output.spec.arity != 2
			|| !output.held.is_empty()
			|| !output.side.is_empty()
	}) {
		return Err(internal("partition outputs of two breakers were merged"));
	}
	let mut runs = Runs::default();
	for output in outputs {
		runs.append(output.runs)?;
	}
	runs.merge(memory, Vec::new(), by_keys(), None, ctx).await
}

/// A join whose build side is divided into partitions: probe rows route by
/// key to the partition holding every build row of an equal key; see the
/// module documentation for rows with no key or a regex key.
pub(super) struct PartitionedJoin {
	memory: Arc<QueryMemory>,
	splitters: Splitters,
	index: u16,
	sharing: Sharing,
	parts: Vec<SpilledJoin>,
	/// The next probe's arrival, across every partition.
	arrivals: i64,
	needs_global: bool,
	/// The records each partition's build side held once split.
	#[cfg(test)]
	build_counts: Vec<u64>,
}

impl PartitionedJoin {
	/// Open from the unsplit build partials of every producer, in producer
	/// order. Fails past `max_rows` build rows in all, as the in-memory
	/// table does, before anything is split.
	pub(super) async fn open(
		memory: &Arc<QueryMemory>,
		splitters: Splitters,
		producers: Vec<Partial>,
		max_rows: usize,
		sharing: Sharing,
		ctx: &ExecutionContext,
	) -> FlowResult<Self> {
		let Some(first) = producers.first() else {
			return Err(internal("a join of no partials"));
		};
		let spec = &first.spec;
		if spec.kind != BreakerKind::JoinBuild || spec.arity < 2 {
			return Err(internal("partials merged as another breaker"));
		}
		if spec.arity == 2 {
			return Err(internal("a cross join is not partitioned"));
		}
		let index = spec.index;
		let taken: u64 = producers.iter().map(|producer| producer.counts.taken).sum();
		if taken > max_rows as u64 {
			return Err(too_many_build_rows(max_rows));
		}
		let split = Partitioned::split(memory, splitters.clone(), producers, sharing, ctx).await?;
		#[cfg(test)]
		let build_counts = split
			.parts()
			.iter()
			.map(|part| part.iter().map(|p| p.runs.total_records() + p.held.len() as u64).sum())
			.collect();
		let mut parts = Vec::with_capacity(split.partitions());
		for part in split.into_parts() {
			parts.push(*SpilledJoin::from_partials(memory, part, usize::MAX, ctx).await?);
		}
		Ok(Self {
			memory: Arc::clone(memory),
			splitters,
			index,
			sharing,
			parts,
			arrivals: 0,
			needs_global: false,
			#[cfg(test)]
			build_counts,
		})
	}

	pub(super) fn partitions(&self) -> usize {
		self.parts.len()
	}

	/// Whether a probe row whose key holds a regex has been deferred, so the
	/// join finishes unpartitioned.
	pub(super) fn needs_global(&self) -> bool {
		self.needs_global
	}

	/// The build records each partition held once split.
	#[cfg(test)]
	pub(super) fn build_counts(&self) -> &[u64] {
		&self.build_counts
	}

	/// Defer probe `row`, keyed `key`, in the partition of its key.
	pub(super) async fn defer(
		&mut self,
		key: Option<JoinKey>,
		row: Value,
		with: &mut dyn Pairer,
	) -> FlowResult<()> {
		let arrival = self.arrivals;
		self.arrivals += 1;
		let to = match &key {
			None => 0,
			Some(key) if key_has_regex(key) => {
				self.needs_global = true;
				0
			}
			Some(key) => self.splitters.route(key),
		};
		self.parts[to].defer_at(arrival, key, row, with).await
	}

	/// The rows every deferred probe yields, in the order the in-memory join
	/// yields them: records `([probe arrival, place], row)`.
	pub(super) async fn finish(self, with: &mut dyn Pairer) -> FlowResult<Merge> {
		let Self {
			memory,
			index,
			sharing,
			parts,
			needs_global,
			..
		} = self;
		let ctx = with.ctx().clone();
		if needs_global {
			return SpilledJoin::union(parts)?.finish(with).await;
		}
		let spec = RunSpec::new(BreakerKind::JoinOut, index, 2, RunOrder::Plain);
		let mut outputs = Vec::with_capacity(parts.len());
		for part in parts {
			let mut merge = part.finish(with).await?;
			let run = write_merge(&memory, spec.clone(), &mut merge, sharing, &ctx).await?;
			drop(merge);
			let taken = run.records();
			let mut runs = Runs::default();
			runs.push(run)?;
			outputs.push(Partial {
				spec: spec.clone(),
				runs,
				held: Vec::new(),
				side: Vec::new(),
				counts: PartialCounts {
					taken,
				},
			});
		}
		merge_outputs(&memory, outputs, BreakerKind::JoinOut, &ctx).await
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;
	use std::str::FromStr;

	use rust_decimal::Decimal;

	use super::*;
	use crate::val::{Array, Number, Object, RecordId, RecordIdKey, TableName};

	fn int(i: i64) -> Value {
		Value::from(i)
	}

	fn float(f: f64) -> Value {
		Value::Number(Number::Float(f))
	}

	fn dec(s: &str) -> Value {
		Value::Number(Number::Decimal(Decimal::from_str(s).expect("a decimal")))
	}

	fn arr(values: Vec<Value>) -> Value {
		Value::Array(Array(values))
	}

	fn obj(fields: &[(&str, Value)]) -> Value {
		let mut object = Object::default();
		for (name, value) in fields {
			object.insert((*name).to_owned(), value.clone());
		}
		Value::Object(object)
	}

	fn rid(values: Vec<Value>) -> Value {
		Value::RecordId(RecordId {
			table: TableName::new("r"),
			key: RecordIdKey::Array(Array(values)),
		})
	}

	fn s(text: &str) -> Value {
		Value::from(text)
	}

	/// Classes of key vectors that are equal under `cmp`, each in several
	/// representations.
	fn classes() -> Vec<Vec<Vec<Value>>> {
		vec![
			vec![vec![int(1)], vec![float(1.0)], vec![dec("1")], vec![dec("1.00")]],
			vec![vec![int(0)], vec![float(0.0)], vec![float(-0.0)], vec![dec("0")]],
			vec![
				vec![arr(vec![int(1), arr(vec![float(1.0)])])],
				vec![arr(vec![dec("1"), arr(vec![int(1)])])],
			],
			vec![
				vec![obj(&[("a", int(1)), ("b", arr(vec![float(2.0)]))])],
				vec![obj(&[("a", float(1.0)), ("b", arr(vec![dec("2")]))])],
			],
			vec![vec![rid(vec![int(1)])], vec![rid(vec![float(1.0)])], vec![rid(vec![dec("1")])]],
			vec![vec![int(1), s("a")], vec![float(1.0), s("a")], vec![dec("1"), s("a")]],
			vec![vec![Value::None, arr(vec![int(1)])], vec![Value::None, arr(vec![float(1.0)])]],
		]
	}

	fn of_width(width: usize, keys: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
		keys.into_iter().filter(|key| key.len() == width).collect()
	}

	fn pool1() -> Vec<Vec<Value>> {
		let mut pool: Vec<Vec<Value>> =
			classes().into_iter().flatten().filter(|key| key.len() == 1).collect();
		pool.extend([
			vec![int(2)],
			vec![float(0.5)],
			vec![dec("1.5")],
			vec![float(0.09)],
			vec![dec("0.11")],
			vec![float(0.1)],
			vec![dec("0.1")],
			vec![Value::None],
			vec![Value::Null],
			vec![s("a")],
			vec![arr(vec![])],
			vec![arr(vec![int(1)])],
			vec![obj(&[])],
			vec![rid(vec![int(0)])],
		]);
		pool
	}

	fn pool2() -> Vec<Vec<Value>> {
		let mut pool: Vec<Vec<Value>> =
			classes().into_iter().flatten().filter(|key| key.len() == 2).collect();
		pool.extend([
			vec![int(1), s("b")],
			vec![int(0), s("z")],
			vec![Value::None, arr(vec![int(0)])],
			vec![Value::Null, int(1)],
			vec![int(2), s("a")],
		]);
		pool
	}

	fn pool(width: usize) -> Vec<Vec<Value>> {
		match width {
			1 => pool1(),
			_ => pool2(),
		}
	}

	/// Every single pool key, every ordered pair of them and the whole pool,
	/// each as splitters.
	fn splitter_sets(pool: &[Vec<Value>]) -> Vec<Splitters> {
		let mut sets = Vec::new();
		for a in pool {
			sets.push(Splitters::new(vec![a.clone()]));
			for b in pool {
				sets.push(Splitters::new(vec![a.clone(), b.clone()]));
			}
		}
		sets.push(Splitters::new(pool.to_vec()));
		sets
	}

	#[test]
	fn c_class_members_compare_equal() {
		for class in classes() {
			for a in &class {
				for b in &class {
					assert_eq!(
						a.as_slice().cmp(b.as_slice()),
						Ordering::Equal,
						"{a:?} against {b:?}"
					);
				}
			}
		}
	}

	#[test]
	fn c_equal_keys_of_every_representation_route_together() {
		for width in [1, 2] {
			let pool = pool(width);
			let classes: Vec<Vec<Vec<Value>>> =
				classes().into_iter().map(|class| of_width(width, class)).collect();
			for splitters in splitter_sets(&pool) {
				for class in classes.iter().filter(|class| !class.is_empty()) {
					let routes: Vec<usize> = class.iter().map(|key| splitters.route(key)).collect();
					assert!(
						routes.iter().all(|route| *route == routes[0]),
						"{class:?} routed apart as {routes:?} by {:?}",
						splitters.keys()
					);
				}
			}
		}
	}

	#[test]
	fn c_routes_are_monotone_in_key_order() {
		for width in [1, 2] {
			let pool = pool(width);
			let mut keys = pool.clone();
			keys.sort();
			for splitters in splitter_sets(&pool) {
				let routes: Vec<usize> = keys.iter().map(|key| splitters.route(key)).collect();
				assert!(
					routes.windows(2).all(|pair| pair[0] <= pair[1]),
					"{routes:?} by {:?}",
					splitters.keys()
				);
			}
		}
	}

	/// Grouping, distinct and join never apply `collate` or `numeric`, so keys
	/// that differ under `Ord` are different groups in the unpartitioned
	/// breaker, and routing them apart is correct.
	#[test]
	fn c_keys_apart_under_ord_may_route_apart() {
		assert_ne!(Value::None.cmp(&Value::Null), Ordering::Equal);
		let splitters = Splitters::new(vec![vec![Value::None]]);
		assert_eq!(splitters.route(&[Value::None]), 0);
		assert_eq!(splitters.route(&[Value::Null]), 1);
		assert_ne!(s("Ab").cmp(&s("ab")), Ordering::Equal);
		let splitters = Splitters::new(vec![vec![s("Ab")]]);
		assert_eq!(splitters.route(&[s("Ab")]), 0);
		assert_eq!(splitters.route(&[s("ab")]), 1);
	}

	#[test]
	fn c_splitters_new_sorts_and_drops_equal_keys() {
		let splitters =
			Splitters::new(vec![vec![int(2)], vec![float(1.0)], vec![int(1)], vec![dec("1")]]);
		assert_eq!(splitters.keys().len(), 2);
		assert!(matches!(&splitters.keys()[0][0], Value::Number(Number::Float(_))));
		assert_eq!(splitters.keys()[1], vec![int(2)]);
	}

	#[test]
	fn c_splitters_from_samples_pick_at_most_n_partitions() {
		let ints: Vec<Vec<Value>> = (0..100).step_by(3).map(|i| vec![int(i)]).collect();
		let floats: Vec<Vec<Value>> = (1..100).step_by(3).map(|i| vec![float(i as f64)]).collect();
		let decimals: Vec<Vec<Value>> =
			(2..100).step_by(3).map(|i| vec![dec(&i.to_string())]).collect();
		for n in [1, 2, 7] {
			let splitters =
				Splitters::from_samples(vec![ints.clone(), floats.clone(), decimals.clone()], n);
			assert_eq!(splitters.partitions(), n);
			assert!(
				splitters.keys().windows(2).all(|pair| pair[0].cmp(&pair[1]) == Ordering::Less)
			);
		}
		let same = vec![vec![vec![int(1)]], vec![vec![float(1.0)]], vec![vec![dec("1")]]];
		assert_eq!(Splitters::from_samples(same, 7).partitions(), 1);
		assert_eq!(Splitters::from_samples(Vec::new(), 7).partitions(), 1);
	}

	#[test]
	fn c_group_all_keys_route_to_partition_zero() {
		for splitters in splitter_sets(&pool1()) {
			assert_eq!(splitters.route(&[]), 0);
		}
	}
}
