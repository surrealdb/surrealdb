//! A GQL hash join whose build side spills to the query's temporary store when the memory
//! lease does not cover the build rows. Its output keeps the order the in-memory join gives.
//!
//! The build side is read a batch at a time ([`BuildSink`]). While the lease covers
//! the build rows, the join builds its table in memory. Once it does not, the build rows
//! are written out as runs sorted by key, then arrival, and the join is a sort-merge join
//! with a deferred probe pass:
//!
//! - every probe row is deferred with its arrival: a probe row with a key is spilled sorted by key,
//!   then arrival; one whose key holds a regex is kept aside, since `=` matches it against every
//!   key; one with no key yields at once what the in-memory join yields for it;
//! - when the probe side is finished, the probe rows and the build rows are merged by key. A probe
//!   row's matches are the build rows of an equal key in arrival order, then the build rows whose
//!   key holds a regex and matches it — the order [`BuildTable::matches`] gives — and what it
//!   yields is what the in-memory join yields for those matches ([`super::binding::pairings`]);
//! - each row yielded is kept with its probe row's arrival and its place among that row's pairings,
//!   and the rows are put back in that order by an external sort and handed on.
//!
//! Equality is `Value: Ord`, as the in-memory table's, so no match is lost to
//! a hash. A cross join pairs every build row with every probe row and is
//! not spilled; its build side keeps the in-memory build-row cap.

// The IR lowering is the only caller of the items here, so dead-code lints are off.
#![allow(dead_code)]

use std::cmp::Ordering;
use std::sync::Arc;

use super::binding::{HostJudge, JoinStep, pairings};
use super::host::RowSink;
use super::partial::{Partial, PartialCounts};
use super::program::HostScalar;
use super::spill::{Merge, RecordOrder, Runs, write_run};
use crate::exec::memory::runs::{BreakerKind, Record, RunOrder, RunSpec};
use crate::exec::memory::{QueryMemory, Reservation, Verdict};
use crate::exec::operators::join::hash_join::{
	BuildTable, JoinKey, join_key, key_has_regex, keys_equal, too_many_build_rows,
};
use crate::exec::{BoxFut, ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

fn by_keys() -> RecordOrder {
	Arc::new(|(a, _), (b, _)| a.cmp(b))
}

/// Sort `held` by key, write it as a run of `spec` appended to `runs`, and release its charge from
/// `account`.
async fn spill_sorted(
	memory: &Arc<QueryMemory>,
	spec: RunSpec,
	runs: &mut Runs,
	held: &mut Vec<Record>,
	account: &mut Reservation,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	let mut rows = std::mem::take(held);
	rows.sort_by(|(a, _), (b, _)| a.cmp(b));
	runs.push(write_run(memory, spec, rows, ctx).await?)?;
	account.clear();
	Ok(())
}

/// A join's build side as it is read, a batch at a time.
pub(super) struct BuildSink {
	/// The bindings a GQL join keys its rows on, or none where the caller
	/// keys each row ([`Self::push`]).
	keys: Vec<String>,
	/// How many key columns a build row's key has.
	width: usize,
	/// The join's build group within its program.
	index: u16,
	max_rows: usize,
	rows: usize,
	account: Reservation,
	memory: Option<Arc<QueryMemory>>,
	/// Build rows with a key, keyed by the key then their arrival.
	held: Vec<Record>,
	runs: Runs,
	/// Build rows whose key holds a regex, in arrival order. They are never
	/// spilled, so they are charged as bytes the query cannot spill.
	regex: Vec<(JoinKey, Value)>,
	regex_account: Reservation,
	/// The producer this build side is one of several of, whose index each
	/// record carries after its key.
	producer: Option<i64>,
}

impl BuildSink {
	/// A build side keyed on the bindings `keys`, failing past `max_rows`
	/// rows as the in-memory table does, charging `ctx`'s query where it can
	/// spill.
	pub(super) fn new(
		keys: &[String],
		max_rows: usize,
		index: u16,
		ctx: &ExecutionContext,
	) -> Self {
		Self::keyed(keys.to_vec(), keys.len(), max_rows, index, ctx)
	}

	/// A build side whose rows the caller keys, `width` columns each.
	pub(super) fn with_width(
		width: usize,
		max_rows: usize,
		index: u16,
		ctx: &ExecutionContext,
	) -> Self {
		Self::keyed(Vec::new(), width, max_rows, index, ctx)
	}

	fn keyed(
		keys: Vec<String>,
		width: usize,
		max_rows: usize,
		index: u16,
		ctx: &ExecutionContext,
	) -> Self {
		let memory = ctx.ctx().memory().filter(|memory| memory.can_spill()).cloned();
		Self {
			keys,
			width,
			index,
			max_rows,
			rows: 0,
			account: match &memory {
				Some(memory) => memory.reservation(),
				None => Reservation::unbounded(),
			},
			regex_account: match &memory {
				Some(memory) => memory.pinned_reservation(),
				None => Reservation::unbounded(),
			},
			memory,
			held: Vec::new(),
			runs: Runs::default(),
			regex: Vec::new(),
			producer: None,
		}
	}

	/// This build side as producer `producer` of several: its records carry
	/// the producer after their key, so partials merge in serial build order.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(super) fn producing(mut self, producer: u32) -> Self {
		self.producer = Some(i64::from(producer));
		self
	}

	/// The build side read so far as a partial: its runs, its held rows sorted
	/// by key, producer and arrival, and its regex rows as `side`. Only a
	/// producing sink has one.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(super) fn into_partial(mut self) -> FlowResult<Partial> {
		if self.producer.is_none() {
			return Err(ControlFlow::Err(anyhow::anyhow!(
				"Internal error: a build side that is not a producer has no partial"
			)));
		}
		let spec = self.spec(BreakerKind::JoinBuild);
		let mut held = std::mem::take(&mut self.held);
		held.sort_by(|(a, _), (b, _)| a.cmp(b));
		self.account.clear();
		Ok(Partial {
			spec,
			runs: std::mem::take(&mut self.runs),
			held,
			side: std::mem::take(&mut self.regex),
			counts: PartialCounts {
				taken: self.rows as u64,
			},
		})
	}

	/// The build side once every row is read: a table in memory, or the runs
	/// it spilled.
	pub(super) async fn finish(mut self, ctx: &ExecutionContext) -> FlowResult<Built> {
		let Some(memory) = self.memory.clone().filter(|_| !self.runs.is_empty()) else {
			let mut table = BuildTable::new();
			let width = self.width;
			for (mut key, row) in self.held {
				key.truncate(width);
				table.insert(key, row, usize::MAX)?;
			}
			for (key, row) in self.regex {
				table.insert(key, row, usize::MAX)?;
			}
			return Ok(Built::Memory(table));
		};
		let spec = self.spec(BreakerKind::JoinBuild);
		spill_sorted(&memory, spec, &mut self.runs, &mut self.held, &mut self.account, ctx).await?;
		Ok(Built::Spilled(Box::new(SpilledJoin {
			width: self.width,
			index: self.index,
			account: memory.reservation(),
			regex_probe_account: memory.pinned_reservation(),
			memory,
			build: self.runs,
			regex: self.regex,
			_regex_account: self.regex_account,
			probes: Vec::new(),
			probe_runs: Runs::default(),
			regex_probes: Vec::new(),
			out: Vec::new(),
			out_runs: Runs::default(),
			arrivals: 0,
		})))
	}
}

impl BuildSink {
	/// The spec of a run of build rows: their key, their producer where there
	/// is one, then their arrival.
	fn spec(&self, kind: BreakerKind) -> RunSpec {
		let arity = self.width + 1 + usize::from(self.producer.is_some());
		RunSpec::new(kind, self.index, arity, RunOrder::Plain)
	}

	/// Take build row `row`, keyed `key`.
	pub(super) async fn push(
		&mut self,
		key: JoinKey,
		row: Value,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		if self.rows >= self.max_rows {
			return Err(too_many_build_rows(self.max_rows));
		}
		let arrival = Value::from(self.rows as i64);
		self.rows += 1;
		let bytes = crate::exec::memory::size::value_bytes(&row)
			+ crate::exec::memory::size::values_bytes(&key);
		if key_has_regex(&key) {
			self.regex.push((key, row));
			if self.regex_account.charge(bytes) == Verdict::Spill {
				self.regex_account.insist(ctx).await?;
			}
			return Ok(());
		}
		let mut keys = key;
		keys.extend(self.producer.map(Value::from));
		keys.push(arrival);
		self.held.push((keys, row));
		if self.account.charge(bytes) == Verdict::Spill
			&& let Some(memory) = &self.memory
		{
			let memory = Arc::clone(memory);
			let spec = self.spec(BreakerKind::JoinBuild);
			spill_sorted(&memory, spec, &mut self.runs, &mut self.held, &mut self.account, ctx)
				.await?;
		}
		Ok(())
	}
}

impl RowSink for BuildSink {
	fn take<'a>(
		&'a mut self,
		rows: Vec<Value>,
		ctx: &'a ExecutionContext,
	) -> BoxFut<'a, FlowResult<()>> {
		Box::pin(async move {
			for row in rows {
				super::fuel::tick(ctx, 1).await?;
				if let Some(key) = join_key(&row, &self.keys) {
					self.push(key, row, ctx).await?;
				}
			}
			Ok(())
		})
	}
}

/// A SQL join's build side as the pipelines that read it see it: a table
/// shared between them, or the runs it spilled.
pub(super) enum JoinBuilt {
	Memory(Arc<BuildTable>),
	Spilled(Box<SpilledJoin>),
}

/// A join's build side once read.
pub(super) enum Built {
	Memory(BuildTable),
	Spilled(Box<SpilledJoin>),
}

/// A join whose build side spilled: its build runs, and the probe rows and
/// pairings it defers until its probe side is finished.
pub(super) struct SpilledJoin {
	/// How many key columns a record's keys begin with.
	width: usize,
	/// The join's build group within its program.
	index: u16,
	memory: Arc<QueryMemory>,
	account: Reservation,
	build: Runs,
	regex: Vec<(JoinKey, Value)>,
	/// What the build rows in `regex` hold, charged for as long as the join
	/// holds them.
	_regex_account: Reservation,
	/// Probe rows with a key, keyed by the key then their arrival.
	probes: Vec<Record>,
	probe_runs: Runs,
	/// Probe rows whose key holds a regex, with their arrival. They are never
	/// spilled, so they are charged as bytes the query cannot spill.
	regex_probes: Vec<(i64, JoinKey, Value)>,
	regex_probe_account: Reservation,
	/// Rows yielded, keyed by their probe row's arrival and their place.
	out: Vec<Record>,
	out_runs: Runs,
	arrivals: i64,
}

#[cfg_attr(not(test), allow(dead_code))]
impl SpilledJoin {
	/// The spilled join a coordinator probes, from its producers' build
	/// partials in producer order: the build runs in serial build order, the
	/// regex rows in serial arrival order. Fails past `max_rows` build rows in
	/// all, before any probe row, as the in-memory table does.
	pub(super) async fn from_partials(
		memory: &Arc<QueryMemory>,
		partials: Vec<Partial>,
		max_rows: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<Box<SpilledJoin>> {
		let taken: u64 = partials.iter().map(|partial| partial.counts.taken).sum();
		if taken > max_rows as u64 {
			return Err(too_many_build_rows(max_rows));
		}
		let Some(first) = partials.first() else {
			return Err(ControlFlow::Err(anyhow::anyhow!("Internal error: a join of no partials")));
		};
		let spec = first.spec.clone();
		if spec.kind != BreakerKind::JoinBuild || spec.arity < 2 {
			return Err(ControlFlow::Err(anyhow::anyhow!(
				"Internal error: partials merged as another breaker"
			)));
		}
		let (width, index) = (spec.arity as usize - 2, spec.index);
		let mut build = Runs::default();
		let mut regex = Vec::new();
		let mut regex_account = memory.pinned_reservation();
		for partial in partials {
			let Partial {
				runs,
				held,
				side,
				..
			} = partial;
			build.append(runs)?;
			if !held.is_empty() {
				build.push(write_run(memory, spec.clone(), held, ctx).await?)?;
			}
			for (key, row) in side {
				let bytes = crate::exec::memory::size::value_bytes(&row)
					+ crate::exec::memory::size::values_bytes(&key);
				if regex_account.charge(bytes) == Verdict::Spill {
					regex_account.insist(ctx).await?;
				}
				regex.push((key, row));
			}
		}
		Ok(Box::new(SpilledJoin {
			width,
			index,
			account: memory.reservation(),
			memory: Arc::clone(memory),
			build,
			regex,
			_regex_account: regex_account,
			regex_probe_account: memory.pinned_reservation(),
			probes: Vec::new(),
			probe_runs: Runs::default(),
			regex_probes: Vec::new(),
			out: Vec::new(),
			out_runs: Runs::default(),
			arrivals: 0,
		}))
	}
}

#[cfg_attr(not(test), allow(dead_code))]
impl SpilledJoin {
	/// One spilled join holding every build run, deferred probe and yielded row
	/// of `parts`, partitions of one join whose probes were numbered across all
	/// of them ([`Self::defer_at`]). Every `parts` holds the same regex build
	/// rows; the union keeps the first's. Every record a part holds is keyed by
	/// a unique (key, producer, arrival) or (arrival, place), so the union's
	/// merges answer what one unpartitioned join's do.
	pub(super) fn union(parts: Vec<SpilledJoin>) -> FlowResult<SpilledJoin> {
		let mut parts = parts.into_iter();
		let Some(mut all) = parts.next() else {
			return Err(ControlFlow::Err(anyhow::anyhow!("Internal error: a union of no joins")));
		};
		for part in parts {
			if part.width != all.width || part.index != all.index {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"Internal error: joins of two breakers were united"
				)));
			}
			let SpilledJoin {
				width: _,
				index: _,
				memory: _,
				account,
				build,
				regex: _,
				_regex_account: _,
				regex_probe_account: mut probe_account,
				probes,
				probe_runs,
				regex_probes,
				out,
				out_runs,
				arrivals,
			} = part;
			all.build.append(build)?;
			all.probe_runs.append(probe_runs)?;
			all.out_runs.append(out_runs)?;
			all.probes.extend(probes);
			all.out.extend(out);
			all.regex_probes.extend(regex_probes);
			all.arrivals = all.arrivals.max(arrivals);
			// The part's lease already admitted these rows, so the verdict is
			// not acted on.
			let used = account.used();
			drop(account);
			let _ = all.account.charge(used);
			// The regex probes moved to the union are held as bytes the query
			// cannot spill, so they stay charged, through the reservation the
			// part's account held for them.
			let held = probe_account.used();
			all.regex_probe_account.adopt(&mut probe_account)?;
			drop(probe_account);
			if all.regex_probe_account.charge(held) == Verdict::Spill {
				all.regex_probe_account.insist_now()?;
			}
		}
		Ok(all)
	}
}

/// What a join yields for a probe row given its matches: the in-memory
/// join's own pairing, for a spilled join to apply to its deferred rows.
pub(super) trait Pairer: Send {
	/// The context the join runs in.
	fn ctx(&self) -> &ExecutionContext;

	/// The rows probe `row` yields given the build rows it `matches`, in
	/// order.
	fn pair<'a>(
		&'a mut self,
		matches: &'a [&'a Value],
		row: Value,
	) -> BoxFut<'a, FlowResult<Vec<Value>>>;
}

/// A GQL join's pairing: [`pairings`] under the step's residual.
pub(super) struct Pairing<'a> {
	pub(super) step: &'a JoinStep<u16, HostScalar>,
	pub(super) residual: HostJudge<'a>,
	pub(super) emitted: &'a mut usize,
	pub(super) max_output_rows: usize,
}

impl Pairer for Pairing<'_> {
	fn ctx(&self) -> &ExecutionContext {
		self.residual.ctx
	}

	fn pair<'a>(
		&'a mut self,
		matches: &'a [&'a Value],
		row: Value,
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
			pairings(
				self.step,
				matches,
				row,
				&mut self.residual,
				self.emitted,
				self.max_output_rows,
			)
			.await
		})
	}
}

impl SpilledJoin {
	/// The spec of a run of probe rows (a key, then an arrival) or of rows
	/// yielded (a probe arrival, then a place).
	fn spec(&self, kind: BreakerKind) -> RunSpec {
		let arity = match kind {
			BreakerKind::JoinOut => 2,
			BreakerKind::Sort
			| BreakerKind::Groups
			| BreakerKind::DistinctSeen
			| BreakerKind::DistinctDeferred
			| BreakerKind::DistinctKept
			| BreakerKind::JoinBuild
			| BreakerKind::JoinProbe
			| BreakerKind::WindowBuffer => self.width + 1,
		};
		RunSpec::new(kind, self.index, arity, RunOrder::Plain)
	}

	/// Keep `rows`, the rows probe `arrival` yields, in order.
	async fn yielded(
		&mut self,
		arrival: i64,
		rows: Vec<Value>,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		for (place, row) in rows.into_iter().enumerate() {
			let bytes = crate::exec::memory::size::value_bytes(&row);
			self.out.push((vec![Value::from(arrival), Value::from(place as i64)], row));
			if self.account.charge(bytes) == Verdict::Spill {
				let memory = Arc::clone(&self.memory);
				let spec = self.spec(BreakerKind::JoinOut);
				spill_sorted(
					&memory,
					spec,
					&mut self.out_runs,
					&mut self.out,
					&mut self.account,
					ctx,
				)
				.await?;
			}
		}
		Ok(())
	}

	/// Defer probe `row`: what it yields is answered when the probe side is
	/// finished ([`Self::finish`]).
	pub(super) async fn defer(
		&mut self,
		key: Option<JoinKey>,
		row: Value,
		with: &mut dyn Pairer,
	) -> FlowResult<()> {
		let arrival = self.arrivals;
		self.defer_at(arrival, key, row, with).await
	}

	/// Defer probe `row`, keyed `key`, as the probe with arrival number `arrival`
	/// among every probe of the join; arrivals are unique per join.
	pub(super) async fn defer_at(
		&mut self,
		arrival: i64,
		key: Option<JoinKey>,
		row: Value,
		with: &mut dyn Pairer,
	) -> FlowResult<()> {
		let ctx = with.ctx().clone();
		let ctx = &ctx;
		self.arrivals = self.arrivals.max(arrival + 1);
		let Some(key) = key else {
			let rows = with.pair(&[], row).await?;
			return self.yielded(arrival, rows, ctx).await;
		};
		if key_has_regex(&key) {
			let bytes = crate::exec::memory::size::value_bytes(&row)
				+ crate::exec::memory::size::values_bytes(&key);
			self.regex_probes.push((arrival, key, row));
			if self.regex_probe_account.charge(bytes) == Verdict::Spill {
				self.regex_probe_account.insist(ctx).await?;
			}
			return Ok(());
		}
		let bytes = crate::exec::memory::size::value_bytes(&row);
		let mut keys = key;
		keys.push(Value::from(arrival));
		self.probes.push((keys, row));
		if self.account.charge(bytes) == Verdict::Spill {
			let memory = Arc::clone(&self.memory);
			let spec = self.spec(BreakerKind::JoinProbe);
			spill_sorted(
				&memory,
				spec,
				&mut self.probe_runs,
				&mut self.probes,
				&mut self.account,
				ctx,
			)
			.await?;
		}
		Ok(())
	}

	/// The rows the deferred probe rows yield, in the order the in-memory
	/// join yields them.
	pub(super) async fn finish(mut self, with: &mut dyn Pairer) -> FlowResult<Merge> {
		let ctx = with.ctx().clone();
		let ctx = &ctx;
		let memory = Arc::clone(&self.memory);
		let width = self.width;
		let mut probes_held = std::mem::take(&mut self.probes);
		probes_held.sort_by(|(a, _), (b, _)| a.cmp(b));
		let probe_runs = std::mem::take(&mut self.probe_runs);
		// The probe and build merges are read side by side while runs of the
		// rows yielded are written: their readers share the query's fan-in,
		// which with the writer is its spill headroom. Every pass is made
		// before either is opened.
		let fan_in = memory.fan_in();
		let order = by_keys();
		let probe_runs = probe_runs.reduce(&memory, &order, fan_in / 2, None, ctx).await?;
		let build_runs = std::mem::take(&mut self.build);
		let build_runs = build_runs.reduce(&memory, &order, fan_in - fan_in / 2, None, ctx).await?;
		let mut probes = probe_runs.open(probes_held, Arc::clone(&order), None).await?;
		let mut build = build_runs.open(Vec::new(), order, None).await?;
		// Each regex probe's matches among the build rows with a key, in key
		// then arrival order, gathered as the build rows are walked.
		let mut regex_matches: Vec<Vec<Value>> = vec![Vec::new(); self.regex_probes.len()];
		let mut head = build.next(ctx).await?;
		let mut group: Vec<Value> = Vec::new();
		let mut group_key: Option<Vec<Value>> = None;
		while let Some((mut keys, row)) = probes.next(ctx).await? {
			let arrival = match keys.pop() {
				Some(Value::Number(n)) => n.to_int(),
				_ => 0,
			};
			if group_key.as_ref() != Some(&keys) {
				group.clear();
				// Build rows ordering before the key are passed; those equal to
				// it are its matches.
				while let Some((build_keys, _)) = &head {
					match build_keys[..width].cmp(&keys[..]) {
						Ordering::Greater => break,
						order @ (Ordering::Less | Ordering::Equal) => {
							let (build_keys, build_row) = head.take().expect("checked above");
							self.offer(&build_keys[..width], &build_row, &mut regex_matches);
							if order == Ordering::Equal {
								group.push(build_row);
							}
							head = build.next(ctx).await?;
						}
					}
				}
				group_key = Some(keys.clone());
			}
			let mut matches: Vec<&Value> = group.iter().collect();
			matches.extend(
				self.regex.iter().filter(|(k, _)| keys_equal(k, &keys)).map(|(_, row)| row),
			);
			let rows = with.pair(&matches, row).await?;
			self.yielded(arrival, rows, ctx).await?;
		}
		if !self.regex_probes.is_empty() {
			while let Some((build_keys, build_row)) = head.take() {
				self.offer(&build_keys[..width], &build_row, &mut regex_matches);
				head = build.next(ctx).await?;
			}
			let regex_probes = std::mem::take(&mut self.regex_probes);
			for ((arrival, key, row), found) in regex_probes.into_iter().zip(regex_matches) {
				let mut matches: Vec<&Value> = self
					.regex
					.iter()
					.filter(|(k, _)| keys_equal(k, &key))
					.map(|(_, row)| row)
					.collect();
				matches.extend(found.iter());
				let rows = with.pair(&matches, row).await?;
				self.yielded(arrival, rows, ctx).await?;
			}
		}
		self.regex_probe_account.clear();
		drop(probes);
		drop(build);
		let mut out = std::mem::take(&mut self.out);
		out.sort_by(|(a, _), (b, _)| a.cmp(b));
		self.account.clear();
		let out_runs = std::mem::take(&mut self.out_runs);
		out_runs.merge(&memory, out, by_keys(), None, ctx).await
	}

	/// Offer build row `row`, keyed `key`, to every regex probe it matches.
	fn offer(&self, key: &[Value], row: &Value, found: &mut [Vec<Value>]) {
		for ((_, probe_key, _), found) in self.regex_probes.iter().zip(found) {
			if keys_equal(key, probe_key) {
				found.push(row.clone());
			}
		}
	}
}
