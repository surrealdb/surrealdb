//! What a producer hands its coordinator for one breaker. A producer is
//! either a morsel that spilled or a fan running the same program over
//! a disjoint, contiguous part of the source. The coordinator merges the
//! partials in producer order: the order the source yielded the batches, or
//! the order of the fans' key ranges. That is the arrival order of the serial
//! run. Within a partial, its runs come first, then its held records. The
//! merge's earlier-source-wins tie rule therefore gives every tie the serial
//! order.
//!
//! The records of a partial are those a single node writes for the same
//! breaker, with two exceptions that let the order of a whole record vector
//! stand for serial order across producers:
//!
//! - a sort's records carry the sort's keys alone: an arrival stamp is local to the state that
//!   wrote it, so ties are decided by source order;
//! - a distinct's and a join build's records carry their producer's index after their key, before
//!   their arrival.
//!
//! A partial is merged by [`merge_partials`], which first writes every
//! producer's held records but the last's as a run after that producer's
//! runs, so that what it merges is runs in producer order and the last
//! producer's held records, which arrived after every run.
//!
//! A partial whose runs are shared ([`Sharing::Shared`]) and that holds
//! nothing in memory travels to another process as bytes
//! ([`Partial::to_wire`], [`Partial::from_wire`]); its runs are named, not
//! copied.

// Lowered programs call the items in this module.
#![allow(dead_code)]

use std::sync::Arc;

use super::interp::{Arrivals, State};
use super::join_spill::SpilledJoin;
use super::program::{KeyOrder, Program};
use super::sort::{RANDOM_ORDER, sorted, with_random_keys};
use super::spill::{Merge, Runs, key_order_bytes, records_of, sort_order, write_run, write_run_as};
use crate::exec::memory::runs::{
	BUILD_KEY, BreakerKind, FORMAT, Record, Run, RunError, RunOrder, RunSpec, decode_record,
	encode_record, read_spec, write_spec,
};
use crate::exec::memory::{QueryMemory, RunRef, SessionName, Sharing, Tier};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;

/// The bytes a wire partial begins with.
const MAGIC: [u8; 8] = *b"SRLPARTL";

fn err(e: anyhow::Error) -> ControlFlow {
	ControlFlow::Err(e)
}

fn internal(message: &str) -> ControlFlow {
	err(anyhow::anyhow!("Internal error: {message}"))
}

/// One producer's share of one breaker.
pub(crate) struct Partial {
	/// The breaker and order every run of this partial was written under;
	/// `held` and `side` records have the same arity, but `side` keys are
	/// the join width only.
	pub spec: RunSpec,
	/// The producer's runs, in the order it wrote them.
	pub runs: Runs,
	/// Records the producer still held, after every run, in the spec's order.
	pub held: Vec<Record>,
	/// Records that travel beside the runs out of key order: a join build's
	/// rows whose key holds a regex, `(key, row)` in arrival order. Empty for
	/// every other breaker.
	pub side: Vec<Record>,
	pub counts: PartialCounts,
}

impl std::fmt::Debug for Partial {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Partial")
			.field("spec", &self.spec)
			.field("runs", &self.runs.len())
			.field("held", &self.held.len())
			.field("side", &self.side.len())
			.field("counts", &self.counts)
			.finish()
	}
}

/// What a producer counted while it filled a partial.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PartialCounts {
	/// For a join build, the build rows the producer read, regex rows
	/// included; for a distinct, the rows it took; otherwise the records the
	/// partial holds. A piece of a split partial counts the records routed to
	/// it; the duplicates a distinct dropped are not among them.
	pub taken: u64,
}

/// How a breaker's partials merge.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PartialMerge {
	/// A sort: merged by its keys, cut to `keep`.
	SortMerge {
		keys: Vec<KeyOrder>,
		keep: Option<usize>,
	},
	/// A keyed aggregate (or `GROUP ALL`): saved groups merged by key, each
	/// key's states folded in source order.
	KeyedFold,
	/// A GQL distinct: the first arrival of each row, in arrival order.
	DistinctFirst {
		max_rows: usize,
	},
	/// A join's build side, for the coordinator's spilled join.
	JoinBuild {
		max_rows: usize,
	},
}

/// What merging a breaker's partials answers.
pub(crate) enum Merged {
	/// Records in the merge's order: a sort's `(keys, row)`, a keyed
	/// aggregate's saved groups, or a distinct's kept rows keyed by their
	/// `[producer, arrival]`.
	Records(Merge),
	Join(Box<SpilledJoin>),
}

impl Partial {
	/// Sort `sort`'s runs and buffer as a partial: the buffer sorted stably
	/// and cut to the sort's `keep`, every record carrying the sort's keys
	/// alone. Leaves the sort empty, its account cleared.
	pub(super) async fn of_sort(
		state: &mut State,
		program: &Program,
		sort: u16,
		ctx: &ExecutionContext,
	) -> FlowResult<Partial> {
		let at = sort as usize;
		let spec = &program.sorts[at];
		if spec.knn_slot.is_some() {
			return Err(internal("a brute-force KNN's sort has no partial"));
		}
		let keys = match spec.random {
			true => vec![RANDOM_ORDER],
			false => spec.keys.clone(),
		};
		let mut rows = std::mem::take(&mut state.sorts[at]);
		let slab = std::mem::take(&mut state.sort_slabs[at]);
		if spec.random {
			with_random_keys(&mut rows);
		}
		let rows = sorted(rows, &keys, spec.keep.unwrap_or(usize::MAX), ctx).await?;
		let mut held = records_of(rows, &slab)?;
		for (record, _) in &mut held {
			record.truncate(keys.len());
		}
		let runs = state.sort_runs[at].take().unwrap_or_default();
		state.sort_accounts[at].clear();
		state.sort_arrivals[at] = Arrivals::default();
		if let Some(spill) = &mut state.sort_spill[at] {
			spill.floor = None;
		}
		let spec = RunSpec::new(
			BreakerKind::Sort,
			sort,
			keys.len(),
			RunOrder::Keys(key_order_bytes(&keys)),
		);
		let taken = runs.total_records() + held.len() as u64;
		Ok(Partial {
			spec,
			runs,
			held,
			side: Vec::new(),
			counts: PartialCounts {
				taken,
			},
		})
	}

	/// Aggregate `aggregate`'s runs and groups as a partial: its open groups
	/// saved in key order (`GROUP ALL`'s one group under an empty key). Takes
	/// the groups; fails where an accumulator's state cannot be saved.
	pub(super) fn of_groups(
		state: &mut State,
		program: &Program,
		aggregate: u16,
	) -> FlowResult<Partial> {
		let at = aggregate as usize;
		let groups = state.groups[at]
			.take()
			.ok_or_else(|| internal("an aggregate with no groups has no partial"))?;
		let runs = state.group_runs[at].take().unwrap_or_default();
		let held = groups.into_saved().ok_or_else(|| {
			err(anyhow::anyhow!(
				"An aggregate whose states cannot be saved cannot be merged from partials"
			))
		})?;
		state.group_accounts[at].clear();
		let spec = RunSpec::new(
			BreakerKind::Groups,
			aggregate,
			program.aggregates[at].keys,
			RunOrder::Plain,
		);
		let taken = runs.total_records() + held.len() as u64;
		Ok(Partial {
			spec,
			runs,
			held,
			side: Vec::new(),
			counts: PartialCounts {
				taken,
			},
		})
	}

	/// This partial with its held records written as its last run, through
	/// `memory`'s store with `sharing`; a partial holding nothing is answered
	/// as it is.
	pub(crate) async fn seal(
		mut self,
		memory: &Arc<QueryMemory>,
		sharing: Sharing,
		ctx: &ExecutionContext,
	) -> FlowResult<Partial> {
		if self.held.is_empty() {
			return Ok(self);
		}
		let held = std::mem::take(&mut self.held);
		let run = write_run_as(memory, self.spec.clone(), held, sharing, ctx).await?;
		self.runs.push(run)?;
		Ok(self)
	}
}

/// Merge `partials`, one per producer in producer order, as `how` merges its
/// breaker. Every partial must be of one breaker, the one `how` names.
pub(crate) async fn merge_partials(
	memory: &Arc<QueryMemory>,
	how: &PartialMerge,
	partials: Vec<Partial>,
	ctx: &ExecutionContext,
) -> FlowResult<Merged> {
	let Some(first) = partials.first() else {
		return Err(internal("a merge of no partials"));
	};
	if partials.iter().any(|p| p.spec != first.spec) {
		return Err(internal("partials of two breakers were merged"));
	}
	let (kind, arity) = (first.spec.kind, first.spec.arity as usize);
	let agrees = match how {
		PartialMerge::SortMerge {
			keys,
			..
		} => kind == BreakerKind::Sort && arity == keys.len(),
		PartialMerge::KeyedFold => kind == BreakerKind::Groups,
		PartialMerge::DistinctFirst {
			..
		} => kind == BreakerKind::DistinctDeferred && arity == 3,
		PartialMerge::JoinBuild {
			..
		} => kind == BreakerKind::JoinBuild && arity >= 2,
	};
	if !agrees {
		return Err(internal("partials merged as another breaker"));
	}
	match how {
		PartialMerge::SortMerge {
			keys,
			keep,
		} => {
			let (runs, held, _) = concat(memory, partials, *keep, false, ctx).await?;
			let merge = runs.merge(memory, held, sort_order(keys.clone()), *keep, ctx).await?;
			Ok(Merged::Records(merge))
		}
		PartialMerge::KeyedFold => {
			let (runs, held, _) = concat(memory, partials, None, false, ctx).await?;
			let order: super::spill::RecordOrder = Arc::new(|(a, _), (b, _)| a.cmp(b));
			Ok(Merged::Records(runs.merge(memory, held, order, None, ctx).await?))
		}
		PartialMerge::DistinctFirst {
			max_rows,
		} => {
			let merge =
				super::distinct_spill::merge_distinct(memory, partials, *max_rows, ctx).await?;
			Ok(Merged::Records(merge))
		}
		PartialMerge::JoinBuild {
			max_rows,
		} => {
			let join = SpilledJoin::from_partials(memory, partials, *max_rows, ctx).await?;
			Ok(Merged::Join(join))
		}
	}
}

/// The runs of `partials`, in producer order, with the held records of every
/// producer but the last written as a run after that producer's runs, or of
/// the last as well when `seal_last`; the last producer's held records where
/// they are not sealed, and every producer's side records in producer order.
/// Held records are cut to `limit` first where there is one: the first
/// `limit` records of a source are the only ones of it that can be among the
/// first `limit` of the merge.
pub(super) async fn concat(
	memory: &Arc<QueryMemory>,
	partials: Vec<Partial>,
	limit: Option<usize>,
	seal_last: bool,
	ctx: &ExecutionContext,
) -> FlowResult<(Runs, Vec<Record>, Vec<Record>)> {
	let count = partials.len();
	let mut runs = Runs::default();
	let mut last = Vec::new();
	let mut side = Vec::new();
	for (at, partial) in partials.into_iter().enumerate() {
		let Partial {
			spec,
			runs: theirs,
			mut held,
			side: theirs_side,
			..
		} = partial;
		runs.append(theirs)?;
		if let Some(limit) = limit {
			held.truncate(limit);
		}
		if !held.is_empty() && (at + 1 < count || seal_last) {
			runs.push(write_run(memory, spec, held, ctx).await?)?;
		} else {
			last = held;
		}
		side.extend(theirs_side);
	}
	Ok((runs, last, side))
}

/// A read position in a wire partial's bytes.
struct Cursor<'a> {
	bytes: &'a [u8],
	at: usize,
}

impl<'a> Cursor<'a> {
	/// The next `n` bytes; a malformed partial when fewer remain.
	fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
		let slice = self
			.at
			.checked_add(n)
			.and_then(|end| self.bytes.get(self.at..end).map(|slice| (slice, end)));
		let (slice, end) = slice.ok_or_else(|| anyhow::Error::new(RunError::Malformed))?;
		self.at = end;
		Ok(slice)
	}
}

impl Partial {
	/// The partial as bytes another process reads with [`Partial::from_wire`]:
	/// its spec, counts, runs by name and side records. Its runs are handed on
	/// ([`Run::into_ref`]). Refused for a partial that holds records in memory
	/// or a run another process cannot open.
	#[expect(
		clippy::wrong_self_convention,
		reason = "the partial hands its runs on, so it is consumed"
	)]
	pub(crate) fn to_wire(self) -> anyhow::Result<Vec<u8>> {
		anyhow::ensure!(
			self.held.is_empty(),
			"A partial that holds records in memory cannot be sent: seal it first"
		);
		anyhow::ensure!(
			self.runs.runs().iter().all(|run| run.tier() != Tier::Local),
			"A partial whose runs only this process can read cannot be sent"
		);
		let mut out = Vec::new();
		out.extend_from_slice(&MAGIC);
		out.extend_from_slice(&FORMAT.to_be_bytes());
		out.extend_from_slice(&BUILD_KEY);
		write_spec(&mut out, &self.spec);
		out.extend_from_slice(&self.counts.taken.to_be_bytes());
		let runs = self.runs.len();
		let mut sides = Vec::with_capacity(self.side.len());
		for (keys, row) in &self.side {
			let mut record = Vec::new();
			encode_record(&mut record, keys, row)?;
			sides.push(record);
		}
		out.extend_from_slice(&u32::try_from(runs)?.to_be_bytes());
		for run in self.runs.into_runs() {
			let (name, records) = run.into_ref();
			let session = name.session.as_str().as_bytes();
			out.extend_from_slice(&u32::try_from(session.len())?.to_be_bytes());
			out.extend_from_slice(session);
			out.extend_from_slice(&name.id.to_be_bytes());
			out.push(match name.tier {
				Tier::Local => 0,
				Tier::Shared => 1,
			});
			out.extend_from_slice(&name.bytes.to_be_bytes());
			out.extend_from_slice(&records.to_be_bytes());
		}
		out.extend_from_slice(&u32::try_from(sides.len())?.to_be_bytes());
		for record in sides {
			out.extend_from_slice(&(record.len() as u64).to_be_bytes());
			out.extend_from_slice(&record);
		}
		Ok(out)
	}

	/// A partial read from bytes [`Partial::to_wire`] wrote, its runs adopted
	/// by `memory` ([`Run::adopt`]).
	pub(crate) fn from_wire(bytes: &[u8], memory: &Arc<QueryMemory>) -> anyhow::Result<Partial> {
		let malformed = || anyhow::Error::new(RunError::Malformed);
		let mut cursor = Cursor {
			bytes,
			at: 0,
		};
		if cursor.take(MAGIC.len())? != MAGIC {
			return Err(anyhow::Error::new(RunError::NotARun));
		}
		let format = u16::from_be_bytes(cursor.take(2)?.try_into()?);
		if format != FORMAT {
			return Err(anyhow::Error::new(RunError::Format(format)));
		}
		if cursor.take(BUILD_KEY.len())? != BUILD_KEY {
			return Err(anyhow::Error::new(RunError::OtherBuild));
		}
		let (spec, end) = read_spec(bytes, cursor.at).ok_or_else(malformed)?;
		cursor.at = end;
		let taken = u64::from_be_bytes(cursor.take(8)?.try_into()?);
		let count = u32::from_be_bytes(cursor.take(4)?.try_into()?);
		let mut named = Vec::new();
		for _ in 0..count {
			let len = u32::from_be_bytes(cursor.take(4)?.try_into()?) as usize;
			let session = std::str::from_utf8(cursor.take(len)?).map_err(|_| malformed())?;
			let id = u64::from_be_bytes(cursor.take(8)?.try_into()?);
			let tier = match cursor.take(1)?[0] {
				0 => Tier::Local,
				1 => Tier::Shared,
				_ => return Err(malformed()),
			};
			let size = u64::from_be_bytes(cursor.take(8)?.try_into()?);
			let records = u64::from_be_bytes(cursor.take(8)?.try_into()?);
			let name = RunRef {
				session: SessionName::new(session),
				id,
				tier,
				bytes: size,
			};
			named.push((name, records));
		}
		let count = u32::from_be_bytes(cursor.take(4)?.try_into()?);
		let mut side = Vec::new();
		for _ in 0..count {
			let len = u64::from_be_bytes(cursor.take(8)?.try_into()?);
			let len = usize::try_from(len).map_err(|_| malformed())?;
			let frame = cursor.take(len)?;
			match decode_record(frame)? {
				Some((record, used)) if used == len => side.push(record),
				_ => return Err(malformed()),
			}
		}
		if cursor.at != bytes.len() {
			return Err(malformed());
		}
		// Every field is valid before any run is adopted, so a refusal here
		// leaves the store as it was. The sender has handed every run on, so a
		// run the receiver cannot adopt is deleted rather than left behind.
		let mut runs = Runs::default();
		let mut named = named.into_iter();
		while let Some((name, records)) = named.next() {
			let adopted = Run::adopt(memory, name.clone(), spec.clone(), records).and_then(|run| {
				runs.push(run).map_err(|flow| match flow {
					ControlFlow::Err(e) => e,
					ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_) => {
						anyhow::anyhow!("Internal error: adopted runs of two breakers")
					}
				})
			});
			if let Err(error) = adopted {
				if let Some(store) = memory.store() {
					store.delete_run(&name);
					for (rest, _) in named {
						store.delete_run(&rest);
					}
				}
				return Err(error);
			}
		}
		Ok(Partial {
			spec,
			runs,
			held: Vec::new(),
			side,
			counts: PartialCounts {
				taken,
			},
		})
	}
}
