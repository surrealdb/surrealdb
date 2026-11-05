//! The wire form of what a host place runs: the morsel unit, the morsel, and the frames a leaf
//! answers ([`crate::exec::places`]).
//!
//! A unit is the half of a pipeline a morsel runs, encoded when the statement is lowered
//! ([`encode_unit`]): the pipeline's body and the entries of the program's pools, sorts,
//! aggregates, paging clauses and kinds the body names, renumbered densely so that a unit holds
//! nothing its body does not reach. A unit exists only for a pipeline whose body runs in the
//! storage cursor over a key range ([`MorselInput::Range`] with
//! [`in_place`](super::super::program::Pipeline::in_place)): such a body holds no instruction
//! that hands back to the host, reads no slot, parameter, frame or memo, and reads no key but the
//! records of its range, so a leaf runs it from the unit and a reader alone. Any other pipeline,
//! and one whose body names a sort ranked by a KNN query, has no unit and runs on the node's
//! places.
//!
//! Every encoding begins with [`FORMAT`] and the engine build ([`BUILD`]); a leaf refuses an
//! encoding of another format or build, and the statement runs the morsel itself. Values are
//! written in their `revision` encoding, framed by their length.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context as _, Result, bail, ensure};
use revision::{DeserializeRevisioned, SerializeRevisioned};
use surrealdb_strand::TableName;

use super::super::plain::ValueData;
use super::super::program::{
	AggCall, AggExpr, AggSpec, AggregateSink, ArithOp, CompareOp, Instr, IntCmp, KeyOrder,
	LimitSpec, Operand, Pipeline, Program, SortSpec, Source, UnaryOp,
};
use super::super::row::FieldName;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::operators::AggregateId;
use crate::exec::planner::placement::{Combine, MergeOrder, MorselInput, MorselPlan, Parallel};
use crate::kvs::Direction;
use crate::val::Value;

/// The format of every encoding this module writes.
pub(in crate::exec::vm) const FORMAT: u32 = 1;

/// The engine build a unit was encoded by: the build id a host sets when it builds the engine
/// (`SURREAL_BUILD_ID`), or the crate version. A host deploys its leaves from the build its
/// executor runs.
pub(in crate::exec::vm) const BUILD: &str = match option_env!("SURREAL_BUILD_ID") {
	Some(id) => id,
	None => env!("CARGO_PKG_VERSION"),
};

/// What every encoding opens with.
const MAGIC: &[u8; 4] = b"SMW1";

/// A writer of the wire form.
#[derive(Default)]
pub(in crate::exec::vm) struct Writer(Vec<u8>);

impl Writer {
	/// A writer whose output opens with the format and build.
	pub(in crate::exec::vm) fn opened() -> Self {
		let mut w = Self(Vec::new());
		w.0.extend_from_slice(MAGIC);
		w.uvar(u64::from(FORMAT));
		w.str(BUILD);
		w
	}

	pub(in crate::exec::vm) fn into_bytes(self) -> Vec<u8> {
		self.0
	}

	pub(in crate::exec::vm) fn u8(&mut self, v: u8) {
		self.0.push(v);
	}

	pub(in crate::exec::vm) fn bool(&mut self, v: bool) {
		self.0.push(u8::from(v));
	}

	pub(in crate::exec::vm) fn uvar(&mut self, mut v: u64) {
		loop {
			let byte = (v & 0x7f) as u8;
			v >>= 7;
			if v == 0 {
				self.0.push(byte);
				return;
			}
			self.0.push(byte | 0x80);
		}
	}

	pub(in crate::exec::vm) fn ivar(&mut self, v: i64) {
		self.uvar(((v << 1) ^ (v >> 63)) as u64);
	}

	pub(in crate::exec::vm) fn usize(&mut self, v: usize) {
		self.uvar(v as u64);
	}

	pub(in crate::exec::vm) fn bytes(&mut self, v: &[u8]) {
		self.usize(v.len());
		self.0.extend_from_slice(v);
	}

	pub(in crate::exec::vm) fn str(&mut self, v: &str) {
		self.bytes(v.as_bytes());
	}

	pub(in crate::exec::vm) fn value(&mut self, v: &Value) -> Result<()> {
		let at = self.0.len();
		// The length is written after the value, so a placeholder of the widest varint is not
		// needed: the value is encoded apart and copied in.
		let mut encoded = Vec::new();
		SerializeRevisioned::serialize_revisioned(v, &mut encoded)?;
		self.bytes(&encoded);
		debug_assert!(self.0.len() > at);
		Ok(())
	}

	pub(in crate::exec::vm) fn revisioned<T: SerializeRevisioned>(&mut self, v: &T) -> Result<()> {
		let mut encoded = Vec::new();
		v.serialize_revisioned(&mut encoded)?;
		self.bytes(&encoded);
		Ok(())
	}

	pub(in crate::exec::vm) fn opt<T>(&mut self, v: Option<T>, mut f: impl FnMut(&mut Self, T)) {
		match v {
			None => self.u8(0),
			Some(v) => {
				self.u8(1);
				f(self, v);
			}
		}
	}
}

/// A reader of the wire form.
pub(in crate::exec::vm) struct Reader<'a> {
	bytes: &'a [u8],
	at: usize,
}

impl<'a> Reader<'a> {
	/// A reader of `bytes`, which must open with this module's format and build.
	pub(in crate::exec::vm) fn opened(bytes: &'a [u8]) -> Result<Self> {
		ensure!(bytes.starts_with(MAGIC), "not a morsel encoding");
		let mut r = Self {
			bytes,
			at: MAGIC.len(),
		};
		let format = r.uvar()?;
		ensure!(format == u64::from(FORMAT), "a morsel encoding of format {format}, not {FORMAT}");
		let build = r.str()?;
		ensure!(build == BUILD, "a morsel encoding of engine build {build}, not {BUILD}");
		Ok(r)
	}

	/// A reader of `bytes`, a part of an encoding whose format and build were read already.
	pub(in crate::exec::vm) fn raw(bytes: &'a [u8]) -> Self {
		Self {
			bytes,
			at: 0,
		}
	}

	/// Fails unless every byte has been read.
	pub(in crate::exec::vm) fn done(&self) -> Result<()> {
		ensure!(self.at == self.bytes.len(), "a morsel encoding holds trailing bytes");
		Ok(())
	}

	pub(in crate::exec::vm) fn u8(&mut self) -> Result<u8> {
		let byte = *self.bytes.get(self.at).context("a morsel encoding ends early")?;
		self.at += 1;
		Ok(byte)
	}

	pub(in crate::exec::vm) fn bool(&mut self) -> Result<bool> {
		match self.u8()? {
			0 => Ok(false),
			1 => Ok(true),
			other => bail!("a morsel encoding holds {other} for a flag"),
		}
	}

	pub(in crate::exec::vm) fn uvar(&mut self) -> Result<u64> {
		let mut v = 0u64;
		for shift in (0..64).step_by(7) {
			let byte = self.u8()?;
			v |= u64::from(byte & 0x7f) << shift;
			if byte & 0x80 == 0 {
				return Ok(v);
			}
		}
		bail!("a morsel encoding holds an overlong integer")
	}

	pub(in crate::exec::vm) fn ivar(&mut self) -> Result<i64> {
		let v = self.uvar()?;
		Ok(((v >> 1) as i64) ^ -((v & 1) as i64))
	}

	pub(in crate::exec::vm) fn usize(&mut self) -> Result<usize> {
		usize::try_from(self.uvar()?).context("a morsel encoding holds an oversized count")
	}

	pub(in crate::exec::vm) fn u16(&mut self) -> Result<u16> {
		u16::try_from(self.uvar()?).context("a morsel encoding holds an oversized index")
	}

	pub(in crate::exec::vm) fn u32(&mut self) -> Result<u32> {
		u32::try_from(self.uvar()?).context("a morsel encoding holds an oversized index")
	}

	pub(in crate::exec::vm) fn bytes(&mut self) -> Result<&'a [u8]> {
		let len = self.usize()?;
		let end =
			self.at.checked_add(len).context("a morsel encoding holds an oversized length")?;
		let bytes = self.bytes.get(self.at..end).context("a morsel encoding ends early")?;
		self.at = end;
		Ok(bytes)
	}

	pub(in crate::exec::vm) fn str(&mut self) -> Result<&'a str> {
		std::str::from_utf8(self.bytes()?).context("a morsel encoding holds a malformed string")
	}

	pub(in crate::exec::vm) fn value(&mut self) -> Result<Value> {
		let mut bytes = self.bytes()?;
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}

	pub(in crate::exec::vm) fn revisioned<T: DeserializeRevisioned>(&mut self) -> Result<T> {
		let mut bytes = self.bytes()?;
		Ok(T::deserialize_revisioned(&mut bytes)?)
	}

	pub(in crate::exec::vm) fn opt<T>(
		&mut self,
		mut f: impl FnMut(&mut Self) -> Result<T>,
	) -> Result<Option<T>> {
		match self.u8()? {
			0 => Ok(None),
			1 => f(self).map(Some),
			other => bail!("a morsel encoding holds {other} for an option"),
		}
	}
}

/// The dense numbering a unit gives the entries of one of its program's tables: the entries its
/// body names, in the order the body first names them.
#[derive(Default)]
struct Renumber {
	of: BTreeMap<u32, u32>,
	order: Vec<u32>,
}

impl Renumber {
	fn map(&mut self, at: u32) -> u32 {
		let next = self.order.len() as u32;
		*self.of.entry(at).or_insert_with(|| {
			self.order.push(at);
			next
		})
	}

	fn map16(&mut self, at: u16) -> u16 {
		self.map(u32::from(at)) as u16
	}
}

/// The renumbering of every table a unit's body names.
#[derive(Default)]
struct Tables {
	consts: Renumber,
	fields: Renumber,
	paths: Renumber,
	limits: Renumber,
	sorts: Renumber,
	aggregates: Renumber,
	kinds: Renumber,
}

/// The unit of pipeline `at` of `program`, or `None` when the pipeline has none: it is not a
/// morsel pipeline over a key range whose body runs in the storage cursor, or its body names what
/// a unit cannot carry.
pub(in crate::exec::vm) fn encode_unit(program: &Program, at: usize) -> Option<Vec<u8>> {
	let pipeline = program.pipelines.get(at)?;
	if super::effective_input(pipeline) != Some(MorselInput::Range) || pipeline.enforce.is_some() {
		return None;
	}
	let Parallel::Morsels(plan) = &pipeline.parallel else {
		return None;
	};
	let table = match &pipeline.source {
		Source::Scan {
			table,
			..
		}
		| Source::Record {
			table,
			..
		} => table,
		Source::Index {
			..
		}
		| Source::Lookup {
			..
		}
		| Source::IndexUnion {
			..
		}
		| Source::Bitmap {
			..
		}
		| Source::FullText {
			..
		}
		| Source::Knn {
			..
		}
		| Source::Count {
			..
		}
		| Source::Union(_)
		| Source::Values {
			..
		}
		| Source::Nested {
			..
		}
		| Source::Probe {
			..
		}
		| Source::Sorted {
			..
		}
		| Source::Groups {
			..
		}
		| Source::Split {
			..
		}
		| Source::Join {
			..
		}
		| Source::Once
		| Source::Changes {
			..
		}
		| Source::WriteTargets {
			..
		}
		| Source::WriteInserts {
			..
		}
		| Source::WriteEdges {
			..
		}
		| Source::LightweightEdges {
			..
		}
		| Source::Vacant {
			..
		} => return None,
	};
	let mut tables = Tables::default();
	let mut code = Writer::default();
	code.usize(pipeline.code.len());
	for instr in &pipeline.code {
		put_instr(&mut code, instr, &mut tables)?;
	}
	let mut w = Writer::opened();
	w.str(table.as_str());
	w.usize(pipeline.registers);
	w.bool(pipeline.reads_row);
	put_plan(&mut w, plan);
	w.usize(pipeline.limits.len());
	for &limit in &pipeline.limits {
		w.uvar(u64::from(tables.limits.map16(limit)));
	}
	w.bytes(&code.into_bytes());
	// The tables, in the dense order the body named their entries in.
	let consts: &[Value] = &program.consts;
	w.usize(tables.consts.order.len());
	for &at in &tables.consts.order {
		w.value(consts.get(at as usize)?).ok()?;
	}
	w.usize(tables.fields.order.len());
	for &at in &tables.fields.order {
		w.str(program.fields.get(at as usize)?.as_str());
	}
	w.usize(tables.paths.order.len());
	for &at in &tables.paths.order {
		put_path(&mut w, program.paths.get(at as usize)?);
	}
	w.usize(tables.limits.order.len());
	w.usize(tables.sorts.order.len());
	for &at in &tables.sorts.order {
		put_sort(&mut w, program.sorts.get(at as usize)?)?;
	}
	w.usize(tables.aggregates.order.len());
	for &at in &tables.aggregates.order {
		put_aggregate(&mut w, program.aggregates.get(at as usize)?);
	}
	w.usize(tables.kinds.order.len());
	for &at in &tables.kinds.order {
		w.revisioned(program.kinds.get(at as usize)?).ok()?;
	}
	// What the coordinator maps the leaf's breakers and paging back through.
	for order in [&tables.limits.order, &tables.sorts.order, &tables.aggregates.order] {
		for &at in order {
			w.uvar(u64::from(at));
		}
	}
	Some(w.into_bytes())
}

/// Give every pipeline of `program` that has one its unit ([`encode_unit`]): lowering's copy of
/// the pipeline's morsel half, made when the statement is lowered so that no run encodes one.
pub(in crate::exec::vm) fn attach_units(program: &mut Program) {
	for at in 0..program.pipelines.len() {
		if program.pipelines[at].unit.is_none()
			&& let Some(bytes) = encode_unit(program, at)
		{
			program.pipelines[at].unit = Some(crate::exec::places::MorselUnit::new(bytes));
		}
	}
}

/// The program's indices of the paging clauses, sorts and aggregates a unit renumbered, by the
/// unit's index: what the coordinator reads the leaf's partials back through.
#[derive(Debug, Clone, Default, PartialEq)]
pub(in crate::exec::vm) struct UnitMaps {
	pub(in crate::exec::vm) limits: Vec<u16>,
	pub(in crate::exec::vm) sorts: Vec<u16>,
	pub(in crate::exec::vm) aggregates: Vec<u16>,
}

impl UnitMaps {
	/// The unit's index of the program's paging clause `limit`.
	pub(in crate::exec::vm) fn limit_of(&self, limit: u16) -> Option<u16> {
		self.limits.iter().position(|&at| at == limit).map(|at| at as u16)
	}
}

/// A decoded unit: a program of one pipeline, the leaf's half of the statement's, with the tables
/// its body names.
pub(in crate::exec::vm) struct LeafUnit {
	pub(in crate::exec::vm) program: Arc<Program>,
	pub(in crate::exec::vm) maps: UnitMaps,
}

/// The maps of the unit `bytes`, read without building its program.
pub(in crate::exec::vm) fn unit_maps(bytes: &[u8]) -> Result<UnitMaps> {
	Ok(decode_unit(bytes)?.maps)
}

/// The unit `bytes` encodes.
pub(in crate::exec::vm) fn decode_unit(bytes: &[u8]) -> Result<LeafUnit> {
	let mut r = Reader::opened(bytes)?;
	let table = TableName::from(r.str()?);
	let registers = r.usize()?;
	let reads_row = r.bool()?;
	let plan = get_plan(&mut r)?;
	let limits = (0..r.usize()?).map(|_| r.u16()).collect::<Result<Vec<_>>>()?;
	let code_bytes = r.bytes()?;
	let mut cr = Reader {
		bytes: code_bytes,
		at: 0,
	};
	let code = (0..cr.usize()?).map(|_| get_instr(&mut cr)).collect::<Result<Vec<_>>>()?;
	cr.done()?;
	let consts = (0..r.usize()?).map(|_| r.value()).collect::<Result<Vec<_>>>()?;
	let fields =
		(0..r.usize()?).map(|_| r.str().map(FieldName::new)).collect::<Result<Vec<_>>>()?;
	let paths = (0..r.usize()?).map(|_| get_path(&mut r)).collect::<Result<Vec<_>>>()?;
	let limit_count = r.usize()?;
	let sorts = (0..r.usize()?).map(|_| get_sort(&mut r)).collect::<Result<Vec<_>>>()?;
	let aggregates = (0..r.usize()?).map(|_| get_aggregate(&mut r)).collect::<Result<Vec<_>>>()?;
	let kinds = (0..r.usize()?).map(|_| r.revisioned()).collect::<Result<Vec<_>>>()?;
	let mut maps = UnitMaps::default();
	for _ in 0..limit_count {
		maps.limits.push(r.u16()?);
	}
	for _ in 0..sorts.len() {
		maps.sorts.push(r.u16()?);
	}
	for _ in 0..aggregates.len() {
		maps.aggregates.push(r.u16()?);
	}
	r.done()?;
	let pipeline = Pipeline {
		source: Source::Scan {
			table,
			direction: Direction::Forward,
			topk: None,
			prefilter: None,
			range: None,
			probes: None,
		},
		code,
		registers,
		in_place: true,
		stored_rows: false,
		reads_row,
		enforce: None,
		limits,
		parallel: Parallel::Morsels(plan),
		group: None,
		deref_windows: false,
		frontier_rows: None,
		read_ahead: false,
		rows_ahead: None,
		unit: None,
	};
	let consts = ValueData::new(consts, "a morsel unit's constants")
		.map_err(|_| anyhow::anyhow!("a morsel unit's constants hold a closure"))?;
	let program =
		Program::leaf(pipeline, consts, fields, paths, limit_count, sorts, aggregates, kinds);
	Ok(LeafUnit {
		program: Arc::new(program),
		maps,
	})
}

fn put_plan(w: &mut Writer, plan: &MorselPlan) {
	w.u8(match plan.input {
		MorselInput::Range => 0,
		MorselInput::Batches => 1,
	});
	w.u8(match plan.combine {
		Combine::Concat => 0,
		Combine::StableMerge => 1,
		Combine::TopK => 2,
		Combine::Aggregate => 3,
		Combine::Distinct => 4,
		Combine::OrderedApply => 5,
	});
	w.opt(plan.paged, |w, limit| w.uvar(u64::from(limit)));
	w.bool(plan.prunes);
	w.u8(match plan.merge {
		MergeOrder::Serial => 0,
		MergeOrder::Positioned => 1,
		MergeOrder::Adjacent => 2,
		MergeOrder::Free => 3,
	});
	w.usize(plan.ordered_fold.len());
	for &slot in &plan.ordered_fold {
		w.usize(slot);
	}
}

fn get_plan(r: &mut Reader<'_>) -> Result<MorselPlan> {
	let input = match r.u8()? {
		0 => MorselInput::Range,
		1 => MorselInput::Batches,
		other => bail!("a morsel unit holds input {other}"),
	};
	let combine = match r.u8()? {
		0 => Combine::Concat,
		1 => Combine::StableMerge,
		2 => Combine::TopK,
		3 => Combine::Aggregate,
		4 => Combine::Distinct,
		5 => Combine::OrderedApply,
		other => bail!("a morsel unit holds combine {other}"),
	};
	let paged = r.opt(|r| r.u16())?;
	let prunes = r.bool()?;
	let merge = match r.u8()? {
		0 => MergeOrder::Serial,
		1 => MergeOrder::Positioned,
		2 => MergeOrder::Adjacent,
		3 => MergeOrder::Free,
		other => bail!("a morsel unit holds merge order {other}"),
	};
	let ordered_fold = (0..r.usize()?).map(|_| r.usize()).collect::<Result<Vec<_>>>()?;
	Ok(MorselPlan {
		input,
		combine,
		paged,
		prunes,
		merge,
		ordered_fold,
	})
}

fn put_path(w: &mut Writer, path: &FieldPath) {
	w.usize(path.0.len());
	for part in &path.0 {
		match part {
			FieldPathPart::Field(name) => {
				w.u8(0);
				w.str(name);
			}
			FieldPathPart::Index(at) => {
				w.u8(1);
				w.usize(*at);
			}
			FieldPathPart::First => w.u8(2),
			FieldPathPart::Last => w.u8(3),
			FieldPathPart::Lookup(name) => {
				w.u8(4);
				w.str(name);
			}
		}
	}
}

fn get_path(r: &mut Reader<'_>) -> Result<FieldPath> {
	let parts = (0..r.usize()?)
		.map(|_| {
			Ok(match r.u8()? {
				0 => FieldPathPart::Field(r.str()?.to_owned()),
				1 => FieldPathPart::Index(r.usize()?),
				2 => FieldPathPart::First,
				3 => FieldPathPart::Last,
				4 => FieldPathPart::Lookup(r.str()?.to_owned()),
				other => bail!("a morsel unit holds path part {other}"),
			})
		})
		.collect::<Result<Vec<_>>>()?;
	Ok(FieldPath(parts))
}

fn put_sort(w: &mut Writer, sort: &SortSpec) -> Option<()> {
	if sort.knn_slot.is_some() || sort.knn_query.is_some() {
		return None;
	}
	w.usize(sort.keys.len());
	for key in &sort.keys {
		w.bool(key.ascending);
		w.bool(key.collate);
		w.bool(key.numeric);
	}
	w.opt(sort.keep, |w, keep| w.usize(keep));
	put_spill(w, sort.held);
	w.bool(sort.spill);
	w.bool(sort.pushdown);
	w.bool(sort.floors_at_keep);
	w.bool(sort.random);
	Some(())
}

/// A sort of the unit. A build without the `collate` feature orders a collated key as an
/// uncollated one, so it refuses a unit with one rather than run its sort.
fn get_sort(r: &mut Reader<'_>) -> Result<SortSpec> {
	let keys = (0..r.usize()?)
		.map(|_| {
			let key = KeyOrder {
				ascending: r.bool()?,
				collate: r.bool()?,
				numeric: r.bool()?,
			};
			if key.collate && !cfg!(feature = "collate") {
				bail!(crate::expr::Error::FeatureDisabled {
					feature: "collate",
					what: "Collated sorts",
				});
			}
			Ok(key)
		})
		.collect::<Result<Vec<_>>>()?;
	Ok(SortSpec {
		keys,
		keep: r.opt(|r| r.usize())?,
		held: get_spill(r)?,
		spill: r.bool()?,
		pushdown: r.bool()?,
		floors_at_keep: r.bool()?,
		shares_floor: false,
		random: r.bool()?,
		knn_slot: None,
		knn_query: None,
	})
}

fn put_spill(w: &mut Writer, spill: super::super::payload::Spill) {
	w.u8(match spill {
		super::super::payload::Spill::Bounded => 0,
		super::super::payload::Spill::Spillable => 1,
	});
}

fn get_spill(r: &mut Reader<'_>) -> Result<super::super::payload::Spill> {
	Ok(match r.u8()? {
		0 => super::super::payload::Spill::Bounded,
		1 => super::super::payload::Spill::Spillable,
		other => bail!("a morsel unit holds spill {other}"),
	})
}

/// An aggregate's fields with the calls a morsel folds. What runs only when a group finishes —
/// the extra arguments bound to a call, the expression over the calls' answers and a column of
/// `GROUP ALL` evaluated over no row — runs on the statement's task and is not carried.
fn put_aggregate(w: &mut Writer, sink: &AggregateSink) {
	w.usize(sink.keys);
	put_spill(w, sink.held);
	w.usize(sink.fields.len());
	for field in &sink.fields {
		w.usize(field.output_path.len());
		for part in &field.output_path {
			w.str(part);
		}
		w.bool(field.is_group_key);
		w.opt(field.group_key_index, |w, at| w.usize(at));
		w.opt(field.aggregate.as_ref(), |w, aggregate| {
			w.usize(aggregate.calls.len());
			for call in &aggregate.calls {
				match &call.name {
					AggregateId::Named(name) => {
						w.u8(0);
						w.str(name);
					}
					AggregateId::CountField => w.u8(1),
					AggregateId::ImplicitCollect => w.u8(2),
				}
			}
		});
		w.bool(field.fallback_is_constant);
	}
}

fn get_aggregate(r: &mut Reader<'_>) -> Result<AggregateSink> {
	let keys = r.usize()?;
	let held = get_spill(r)?;
	let fields = (0..r.usize()?)
		.map(|_| {
			let output_path =
				(0..r.usize()?).map(|_| r.str().map(str::to_owned)).collect::<Result<Vec<_>>>()?;
			let is_group_key = r.bool()?;
			let group_key_index = r.opt(|r| r.usize())?;
			let aggregate = r.opt(|r| {
				let calls = (0..r.usize()?)
					.map(|_| {
						let name = match r.u8()? {
							0 => AggregateId::Named(r.str()?.to_owned()),
							1 => AggregateId::CountField,
							2 => AggregateId::ImplicitCollect,
							other => bail!("a morsel unit holds aggregate {other}"),
						};
						Ok(AggCall {
							name,
							extra_args: Vec::new(),
						})
					})
					.collect::<Result<Vec<_>>>()?;
				Ok(AggExpr {
					calls,
					post: None,
				})
			})?;
			Ok(AggSpec {
				output_path,
				is_group_key,
				group_key_index,
				aggregate,
				fallback_is_constant: r.bool()?,
				finish: None,
			})
		})
		.collect::<Result<Vec<_>>>()?;
	Ok(AggregateSink {
		fields,
		keys,
		held,
	})
}

fn put_operand(w: &mut Writer, operand: Operand, t: &mut Tables) {
	match operand {
		Operand::Reg(reg) => {
			w.u8(0);
			w.uvar(u64::from(reg));
		}
		Operand::Const(at) => {
			w.u8(1);
			w.uvar(u64::from(t.consts.map(at)));
		}
		Operand::Field(at) => {
			w.u8(2);
			w.uvar(u64::from(t.fields.map(at)));
		}
	}
}

fn get_operand(r: &mut Reader<'_>) -> Result<Operand> {
	Ok(match r.u8()? {
		0 => Operand::Reg(r.u16()?),
		1 => Operand::Const(r.u32()?),
		2 => Operand::Field(r.u32()?),
		other => bail!("a morsel unit holds operand {other}"),
	})
}

fn put_compare(w: &mut Writer, op: CompareOp) {
	w.u8(match op {
		CompareOp::Equal => 0,
		CompareOp::ExactEqual => 1,
		CompareOp::NotEqual => 2,
		CompareOp::AllEqual => 3,
		CompareOp::AnyEqual => 4,
		CompareOp::LessThan => 5,
		CompareOp::LessThanEqual => 6,
		CompareOp::MoreThan => 7,
		CompareOp::MoreThanEqual => 8,
		CompareOp::Contain => 9,
		CompareOp::NotContain => 10,
		CompareOp::ContainAll => 11,
		CompareOp::ContainAny => 12,
		CompareOp::ContainNone => 13,
		CompareOp::Inside => 14,
		CompareOp::NotInside => 15,
		CompareOp::AllInside => 16,
		CompareOp::AnyInside => 17,
		CompareOp::NoneInside => 18,
		CompareOp::Outside => 19,
		CompareOp::Intersects => 20,
	});
}

fn get_compare(r: &mut Reader<'_>) -> Result<CompareOp> {
	Ok(match r.u8()? {
		0 => CompareOp::Equal,
		1 => CompareOp::ExactEqual,
		2 => CompareOp::NotEqual,
		3 => CompareOp::AllEqual,
		4 => CompareOp::AnyEqual,
		5 => CompareOp::LessThan,
		6 => CompareOp::LessThanEqual,
		7 => CompareOp::MoreThan,
		8 => CompareOp::MoreThanEqual,
		9 => CompareOp::Contain,
		10 => CompareOp::NotContain,
		11 => CompareOp::ContainAll,
		12 => CompareOp::ContainAny,
		13 => CompareOp::ContainNone,
		14 => CompareOp::Inside,
		15 => CompareOp::NotInside,
		16 => CompareOp::AllInside,
		17 => CompareOp::AnyInside,
		18 => CompareOp::NoneInside,
		19 => CompareOp::Outside,
		20 => CompareOp::Intersects,
		other => bail!("a morsel unit holds comparison {other}"),
	})
}

fn put_int_cmp(w: &mut Writer, op: IntCmp) {
	w.u8(match op {
		IntCmp::Eq => 0,
		IntCmp::Ne => 1,
		IntCmp::Lt => 2,
		IntCmp::Le => 3,
		IntCmp::Gt => 4,
		IntCmp::Ge => 5,
	});
}

fn get_int_cmp(r: &mut Reader<'_>) -> Result<IntCmp> {
	Ok(match r.u8()? {
		0 => IntCmp::Eq,
		1 => IntCmp::Ne,
		2 => IntCmp::Lt,
		3 => IntCmp::Le,
		4 => IntCmp::Gt,
		5 => IntCmp::Ge,
		other => bail!("a morsel unit holds integer comparison {other}"),
	})
}

fn put_arith(w: &mut Writer, op: ArithOp) {
	match op {
		ArithOp::Add => w.u8(0),
		ArithOp::Subtract => w.u8(1),
		ArithOp::Multiply => w.u8(2),
		ArithOp::Divide => w.u8(3),
		ArithOp::Remainder => w.u8(4),
		ArithOp::Power => w.u8(5),
		ArithOp::Range {
			start_excluded,
			end_included,
		} => {
			w.u8(6);
			w.bool(start_excluded);
			w.bool(end_included);
		}
	}
}

fn get_arith(r: &mut Reader<'_>) -> Result<ArithOp> {
	Ok(match r.u8()? {
		0 => ArithOp::Add,
		1 => ArithOp::Subtract,
		2 => ArithOp::Multiply,
		3 => ArithOp::Divide,
		4 => ArithOp::Remainder,
		5 => ArithOp::Power,
		6 => ArithOp::Range {
			start_excluded: r.bool()?,
			end_included: r.bool()?,
		},
		other => bail!("a morsel unit holds arithmetic {other}"),
	})
}

fn put_unary(w: &mut Writer, op: UnaryOp, t: &mut Tables) {
	match op {
		UnaryOp::Not => w.u8(0),
		UnaryOp::Negate => w.u8(1),
		UnaryOp::Positive => w.u8(2),
		UnaryOp::RangeTo {
			included,
		} => {
			w.u8(3);
			w.bool(included);
		}
		UnaryOp::RangeFrom {
			excluded,
		} => {
			w.u8(4);
			w.bool(excluded);
		}
		UnaryOp::Cast {
			kind,
		} => {
			w.u8(5);
			w.uvar(u64::from(t.kinds.map(kind)));
		}
	}
}

fn get_unary(r: &mut Reader<'_>) -> Result<UnaryOp> {
	Ok(match r.u8()? {
		0 => UnaryOp::Not,
		1 => UnaryOp::Negate,
		2 => UnaryOp::Positive,
		3 => UnaryOp::RangeTo {
			included: r.bool()?,
		},
		4 => UnaryOp::RangeFrom {
			excluded: r.bool()?,
		},
		5 => UnaryOp::Cast {
			kind: r.u32()?,
		},
		other => bail!("a morsel unit holds unary {other}"),
	})
}

fn put_field_int(
	w: &mut Writer,
	t: &mut Tables,
	field: u32,
	op: IntCmp,
	lit: i64,
	reversed: bool,
	fallback: CompareOp,
) {
	w.uvar(u64::from(t.fields.map(field)));
	put_int_cmp(w, op);
	w.ivar(lit);
	w.bool(reversed);
	put_compare(w, fallback);
}

/// Encode `instr`, renumbering what it names through `t`; `None` for an instruction a unit does
/// not carry. The instructions carried are those a body that runs in the storage cursor holds.
fn put_instr(w: &mut Writer, instr: &Instr, t: &mut Tables) -> Option<()> {
	match instr {
		Instr::Load {
			dst,
			src,
		} => {
			w.u8(0);
			w.uvar(u64::from(*dst));
			put_operand(w, *src, t);
		}
		Instr::Compare {
			op,
			left,
			right,
			dst,
		} => {
			w.u8(1);
			put_compare(w, *op);
			put_operand(w, *left, t);
			put_operand(w, *right, t);
			w.uvar(u64::from(*dst));
		}
		Instr::Arith {
			op,
			left,
			right,
			dst,
		} => {
			w.u8(2);
			put_arith(w, *op);
			put_operand(w, *left, t);
			put_operand(w, *right, t);
			w.uvar(u64::from(*dst));
		}
		Instr::Unary {
			op,
			src,
			dst,
		} => {
			w.u8(3);
			put_unary(w, *op, t);
			put_operand(w, *src, t);
			w.uvar(u64::from(*dst));
		}
		Instr::CompareFieldInt {
			field,
			op,
			lit,
			reversed,
			fallback,
			dst,
		} => {
			w.u8(4);
			put_field_int(w, t, *field, *op, *lit, *reversed, *fallback);
			w.uvar(u64::from(*dst));
		}
		Instr::JumpIfFieldInt {
			field,
			op,
			lit,
			reversed,
			fallback,
			truthy,
			target,
		} => {
			w.u8(5);
			put_field_int(w, t, *field, *op, *lit, *reversed, *fallback);
			w.bool(*truthy);
			w.uvar(u64::from(*target));
		}
		Instr::FilterFieldInt {
			field,
			op,
			lit,
			reversed,
			fallback,
			reject,
		} => {
			w.u8(6);
			put_field_int(w, t, *field, *op, *lit, *reversed, *fallback);
			w.bool(*reject);
		}
		Instr::JumpIfCompare {
			op,
			left,
			right,
			truthy,
			target,
		} => {
			w.u8(7);
			put_compare(w, *op);
			put_operand(w, *left, t);
			put_operand(w, *right, t);
			w.bool(*truthy);
			w.uvar(u64::from(*target));
		}
		Instr::FilterCompare {
			op,
			left,
			right,
			reject,
		} => {
			w.u8(8);
			put_compare(w, *op);
			put_operand(w, *left, t);
			put_operand(w, *right, t);
			w.bool(*reject);
		}
		Instr::Filter {
			cond,
			reject,
		} => {
			w.u8(9);
			put_operand(w, *cond, t);
			w.bool(*reject);
		}
		Instr::Jump {
			target,
		} => {
			w.u8(10);
			w.uvar(u64::from(*target));
		}
		Instr::JumpIf {
			cond,
			truthy,
			target,
		} => {
			w.u8(11);
			put_operand(w, *cond, t);
			w.bool(*truthy);
			w.uvar(u64::from(*target));
		}
		Instr::JumpIfNullish {
			cond,
			nullish,
			target,
		} => {
			w.u8(12);
			put_operand(w, *cond, t);
			w.bool(*nullish);
			w.uvar(u64::from(*target));
		}
		Instr::JumpIfNone {
			cond,
			none,
			target,
		} => {
			w.u8(13);
			put_operand(w, *cond, t);
			w.bool(*none);
			w.uvar(u64::from(*target));
		}
		Instr::NewArray {
			dst,
		} => {
			w.u8(14);
			w.uvar(u64::from(*dst));
		}
		Instr::ArrayPush {
			array,
			value,
		} => {
			w.u8(15);
			w.uvar(u64::from(*array));
			put_operand(w, *value, t);
		}
		Instr::NewSet {
			dst,
		} => {
			w.u8(16);
			w.uvar(u64::from(*dst));
		}
		Instr::CollectPush {
			collection,
			value,
		} => {
			w.u8(17);
			w.uvar(u64::from(*collection));
			put_operand(w, *value, t);
		}
		Instr::NewObject {
			dst,
		} => {
			w.u8(18);
			w.uvar(u64::from(*dst));
		}
		Instr::BuildObject {
			dst,
			fields,
		} => {
			w.u8(19);
			w.uvar(u64::from(*dst));
			w.usize(fields.len());
			for (path, value) in fields.iter() {
				w.uvar(u64::from(t.paths.map(*path)));
				put_operand(w, *value, t);
			}
		}
		Instr::SetField {
			object,
			path,
			value,
		} => {
			w.u8(20);
			w.uvar(u64::from(*object));
			w.uvar(u64::from(t.paths.map(*path)));
			put_operand(w, *value, t);
		}
		Instr::Skip {
			limit,
			target,
		} => {
			w.u8(21);
			w.uvar(u64::from(t.limits.map16(*limit)));
			w.uvar(u64::from(*target));
		}
		Instr::Take {
			limit,
		} => {
			w.u8(22);
			w.uvar(u64::from(t.limits.map16(*limit)));
		}
		Instr::JumpIfLimitReached {
			limit,
			done,
		} => {
			w.u8(23);
			w.uvar(u64::from(t.limits.map16(*limit)));
			w.uvar(u64::from(*done));
		}
		Instr::SortPush {
			sort,
			keys,
			count,
			src,
		} => {
			w.u8(24);
			w.uvar(u64::from(t.sorts.map16(*sort)));
			w.uvar(u64::from(*keys));
			w.uvar(u64::from(*count));
			w.uvar(u64::from(*src));
		}
		Instr::GroupKey {
			aggregate,
			index,
			value,
		} => {
			w.u8(25);
			w.uvar(u64::from(t.aggregates.map16(*aggregate)));
			w.uvar(u64::from(*index));
			put_operand(w, *value, t);
		}
		Instr::GroupSelect {
			aggregate,
		} => {
			w.u8(26);
			w.uvar(u64::from(t.aggregates.map16(*aggregate)));
		}
		Instr::GroupFirstNeeded {
			aggregate,
			field,
			done,
		} => {
			w.u8(27);
			w.uvar(u64::from(t.aggregates.map16(*aggregate)));
			w.uvar(u64::from(*field));
			w.uvar(u64::from(*done));
		}
		Instr::GroupFirstSet {
			aggregate,
			field,
			src,
		} => {
			w.u8(28);
			w.uvar(u64::from(t.aggregates.map16(*aggregate)));
			w.uvar(u64::from(*field));
			put_operand(w, *src, t);
		}
		Instr::AccUpdate {
			aggregate,
			field,
			agg,
			value,
		} => {
			w.u8(29);
			w.uvar(u64::from(t.aggregates.map16(*aggregate)));
			w.uvar(u64::from(*field));
			w.uvar(u64::from(*agg));
			put_operand(w, *value, t);
		}
		Instr::Emit {
			src,
		} => {
			w.u8(30);
			w.uvar(u64::from(*src));
		}
		Instr::EmitEnd {
			src,
		} => {
			w.u8(31);
			put_operand(w, *src, t);
		}
		Instr::EndRow => w.u8(32),
		Instr::StopPipeline => w.u8(33),
		Instr::CoerceField {
			..
		}
		| Instr::BeginComputing {
			..
		}
		| Instr::LandRow {
			..
		}
		| Instr::BindRow {
			..
		}
		| Instr::JumpIfPermissionsBypassed {
			..
		}
		| Instr::LoadParam {
			..
		}
		| Instr::LoadSlot {
			..
		}
		| Instr::SortKey {
			..
		}
		| Instr::Fetch {
			..
		}
		| Instr::Access {
			..
		}
		| Instr::Each {
			..
		}
		| Instr::Throw {
			..
		}
		| Instr::FilterBegin {
			..
		}
		| Instr::MapBegin {
			..
		}
		| Instr::Nav {
			..
		}
		| Instr::CallAllowed {
			..
		}
		| Instr::Call {
			..
		}
		| Instr::FunctionEnter {
			..
		}
		| Instr::FunctionBind {
			..
		}
		| Instr::FunctionReturn {
			..
		}
		| Instr::FunctionCall {
			..
		}
		| Instr::ArrayArg {
			..
		}
		| Instr::ElemNext {
			..
		}
		| Instr::ClosureBind {
			..
		}
		| Instr::ClosureReturn {
			..
		}
		| Instr::Dive {
			..
		}
		| Instr::Subquery {
			..
		}
		| Instr::MakeClosure {
			..
		}
		| Instr::ClosureField {
			..
		}
		| Instr::RequireClosure {
			..
		}
		| Instr::Invoke {
			..
		}
		| Instr::Lookup {
			..
		}
		| Instr::Signal {
			..
		}
		| Instr::Extract {
			..
		}
		| Instr::KnnRank {
			..
		}
		| Instr::Measure {
			..
		}
		| Instr::Mock {
			..
		}
		| Instr::Recurse {
			..
		}
		| Instr::Destructure {
			..
		}
		| Instr::RecordId {
			..
		}
		| Instr::MethodResolve {
			..
		}
		| Instr::Method {
			..
		}
		| Instr::MethodClosure {
			..
		}
		| Instr::SeedObject {
			..
		}
		| Instr::LoadPath {
			..
		}
		| Instr::LoadRow {
			..
		}
		| Instr::FetchRow {
			..
		}
		| Instr::RemoveField {
			..
		}
		| Instr::RemovePath {
			..
		}
		| Instr::Omit {
			..
		}
		| Instr::WriteProjection {
			..
		}
		| Instr::BuildObjectEmit {
			..
		}
		| Instr::SplitPush {
			..
		}
		| Instr::Distinct {
			..
		}
		| Instr::JoinProbe {
			..
		}
		| Instr::VerdictJump {
			..
		}
		| Instr::VerdictSet {
			..
		}
		| Instr::JoinPair {
			..
		}
		| Instr::Next {
			..
		}
		| Instr::Write(_)
		| Instr::Store(_)
		| Instr::Block(_)
		| Instr::TryNone {
			..
		}
		| Instr::TryEnd
		| Instr::MethodRegion {
			..
		}
		| Instr::PermissionRegion {
			..
		}
		| Instr::CallIndexFn {
			..
		}
		| Instr::Matches {
			..
		}
		| Instr::ExternalEnter {
			..
		}
		| Instr::ExternalCall {
			..
		}
		| Instr::Degree {
			..
		}
		| Instr::RepeatRecurse {
			..
		}
		| Instr::JumpIfNotObject {
			..
		}
		| Instr::JumpIfOpaque {
			..
		}
		| Instr::InvalidControlFlow
		| Instr::JumpIfNotRecordId {
			..
		}
		| Instr::DocumentRoot {
			..
		}
		| Instr::JudgeCopy {
			..
		}
		| Instr::Ir(_) => return None,
	}
	Some(())
}

fn get_field_int(r: &mut Reader<'_>) -> Result<(u32, IntCmp, i64, bool, CompareOp)> {
	Ok((r.u32()?, get_int_cmp(r)?, r.ivar()?, r.bool()?, get_compare(r)?))
}

fn get_instr(r: &mut Reader<'_>) -> Result<Instr> {
	Ok(match r.u8()? {
		0 => Instr::Load {
			dst: r.u16()?,
			src: get_operand(r)?,
		},
		1 => Instr::Compare {
			op: get_compare(r)?,
			left: get_operand(r)?,
			right: get_operand(r)?,
			dst: r.u16()?,
		},
		2 => Instr::Arith {
			op: get_arith(r)?,
			left: get_operand(r)?,
			right: get_operand(r)?,
			dst: r.u16()?,
		},
		3 => Instr::Unary {
			op: get_unary(r)?,
			src: get_operand(r)?,
			dst: r.u16()?,
		},
		4 => {
			let (field, op, lit, reversed, fallback) = get_field_int(r)?;
			Instr::CompareFieldInt {
				field,
				op,
				lit,
				reversed,
				fallback,
				dst: r.u16()?,
			}
		}
		5 => {
			let (field, op, lit, reversed, fallback) = get_field_int(r)?;
			Instr::JumpIfFieldInt {
				field,
				op,
				lit,
				reversed,
				fallback,
				truthy: r.bool()?,
				target: r.u32()?,
			}
		}
		6 => {
			let (field, op, lit, reversed, fallback) = get_field_int(r)?;
			Instr::FilterFieldInt {
				field,
				op,
				lit,
				reversed,
				fallback,
				reject: r.bool()?,
			}
		}
		7 => Instr::JumpIfCompare {
			op: get_compare(r)?,
			left: get_operand(r)?,
			right: get_operand(r)?,
			truthy: r.bool()?,
			target: r.u32()?,
		},
		8 => Instr::FilterCompare {
			op: get_compare(r)?,
			left: get_operand(r)?,
			right: get_operand(r)?,
			reject: r.bool()?,
		},
		9 => Instr::Filter {
			cond: get_operand(r)?,
			reject: r.bool()?,
		},
		10 => Instr::Jump {
			target: r.u32()?,
		},
		11 => Instr::JumpIf {
			cond: get_operand(r)?,
			truthy: r.bool()?,
			target: r.u32()?,
		},
		12 => Instr::JumpIfNullish {
			cond: get_operand(r)?,
			nullish: r.bool()?,
			target: r.u32()?,
		},
		13 => Instr::JumpIfNone {
			cond: get_operand(r)?,
			none: r.bool()?,
			target: r.u32()?,
		},
		14 => Instr::NewArray {
			dst: r.u16()?,
		},
		15 => Instr::ArrayPush {
			array: r.u16()?,
			value: get_operand(r)?,
		},
		16 => Instr::NewSet {
			dst: r.u16()?,
		},
		17 => Instr::CollectPush {
			collection: r.u16()?,
			value: get_operand(r)?,
		},
		18 => Instr::NewObject {
			dst: r.u16()?,
		},
		19 => {
			let dst = r.u16()?;
			let fields = (0..r.usize()?)
				.map(|_| Ok((r.u32()?, get_operand(r)?)))
				.collect::<Result<Vec<_>>>()?;
			Instr::BuildObject {
				dst,
				fields: fields.into_boxed_slice(),
			}
		}
		20 => Instr::SetField {
			object: r.u16()?,
			path: r.u32()?,
			value: get_operand(r)?,
		},
		21 => Instr::Skip {
			limit: r.u16()?,
			target: r.u32()?,
		},
		22 => Instr::Take {
			limit: r.u16()?,
		},
		23 => Instr::JumpIfLimitReached {
			limit: r.u16()?,
			done: r.u32()?,
		},
		24 => Instr::SortPush {
			sort: r.u16()?,
			keys: r.u16()?,
			count: r.u16()?,
			src: r.u16()?,
		},
		25 => Instr::GroupKey {
			aggregate: r.u16()?,
			index: r.u16()?,
			value: get_operand(r)?,
		},
		26 => Instr::GroupSelect {
			aggregate: r.u16()?,
		},
		27 => Instr::GroupFirstNeeded {
			aggregate: r.u16()?,
			field: r.u16()?,
			done: r.u32()?,
		},
		28 => Instr::GroupFirstSet {
			aggregate: r.u16()?,
			field: r.u16()?,
			src: get_operand(r)?,
		},
		29 => Instr::AccUpdate {
			aggregate: r.u16()?,
			field: r.u16()?,
			agg: r.u16()?,
			value: get_operand(r)?,
		},
		30 => Instr::Emit {
			src: r.u16()?,
		},
		31 => Instr::EmitEnd {
			src: get_operand(r)?,
		},
		32 => Instr::EndRow,
		33 => Instr::StopPipeline,
		other => bail!("a morsel unit holds instruction {other}"),
	})
}

/// The paging clauses of a leaf's program: as many as the unit names, each bounding nothing. A
/// morsel's paging is set by its request, never read from a clause.
pub(in crate::exec::vm) fn leaf_limits(count: usize) -> Vec<LimitSpec> {
	(0..count)
		.map(|_| LimitSpec {
			limit: None,
			start: None,
			group: None,
		})
		.collect()
}

/// What a leaf runs one morsel with besides its unit and range: the direction and version of the
/// read, the paging the statement stood at, the entries one read asks for, whether the morsel's
/// rows stream to the statement as they are produced, and the layouts the scanned table's packed
/// records are decoded through.
pub(in crate::exec::vm) struct Request {
	pub(in crate::exec::vm) direction: Direction,
	pub(in crate::exec::vm) version: Option<u64>,
	/// The unit's paging clause and the rows that can reach it and be taken, at most.
	pub(in crate::exec::vm) paged: Option<(u16, Option<usize>)>,
	pub(in crate::exec::vm) chunk: u32,
	pub(in crate::exec::vm) streams: bool,
	pub(in crate::exec::vm) layouts: Option<Arc<crate::catalog::TableLayouts>>,
}

/// The layouts `layouts`, written for a leaf; `None` when a listed generation's descriptor is one
/// this build refuses or cannot write, or the ring is refused.
fn put_layouts(w: &mut Writer, layouts: &crate::catalog::TableLayouts) -> Option<()> {
	if layouts.ring_refusal().is_some() {
		return None;
	}
	w.uvar(u64::from(layouts.namespace().0));
	w.uvar(u64::from(layouts.database().0));
	w.str(layouts.table().as_str());
	let ring = layouts.ring();
	for generation in [ring.base(), ring.head(), ring.floor()] {
		w.opt(generation, |w, generation| w.uvar(u64::from(generation.get())));
	}
	let mut listed = Vec::new();
	for (generation, layout) in layouts.generations() {
		let crate::catalog::GenerationLayout::Readable(descriptor) = layout else {
			return None;
		};
		let encoded = crate::catalog::stored::entry::encode_stored(
			&**descriptor,
			&surrealdb_kvs::EncodeContext::CURRENT,
		)
		.ok()?;
		listed.push((generation.get(), encoded.bytes));
	}
	w.usize(listed.len());
	for (generation, bytes) in &listed {
		w.uvar(u64::from(*generation));
		w.bytes(bytes);
	}
	w.uvar(u64::from(layouts.damaged_keys()));
	Some(())
}

fn get_layouts(r: &mut Reader<'_>) -> Result<crate::catalog::TableLayouts> {
	use crate::catalog::{DatabaseId, Generation, GenerationRing, NamespaceId, TableLayouts};
	let ns = NamespaceId(r.u32()?);
	let db = DatabaseId(r.u32()?);
	let table = TableName::from(r.str()?);
	let mut ends = [None; 3];
	for end in &mut ends {
		*end = r.opt(|r| Generation::new(r.u16()?).context("a morsel holds generation 0"))?;
	}
	let ring = match ends {
		[None, None, None] => GenerationRing::NONE,
		[Some(base), Some(head), floor] => GenerationRing::new(base, head, floor)
			.context("a morsel holds a malformed generation ring")?,
		_ => bail!("a morsel holds a malformed generation ring"),
	};
	let generations = (0..r.usize()?)
		.map(|_| {
			let generation = Generation::new(r.u16()?).context("a morsel holds generation 0")?;
			Ok((generation, r.bytes()?.to_vec().into_boxed_slice()))
		})
		.collect::<Result<Vec<_>>>()?;
	let damaged = r.u32()?;
	Ok(TableLayouts::from_stored(ns, db, table, ring, generations, damaged)?)
}

/// The request `request` for a morsel; `None` when its layouts cannot be written.
pub(in crate::exec::vm) fn encode_request(request: &Request) -> Option<Vec<u8>> {
	let mut w = Writer::opened();
	w.u8(match request.direction {
		Direction::Forward => 0,
		Direction::Backward => 1,
	});
	w.opt(request.version, |w, version| w.uvar(version));
	w.opt(request.paged, |w, (limit, needed)| {
		w.uvar(u64::from(limit));
		w.opt(needed, |w, needed| w.usize(needed));
	});
	w.uvar(u64::from(request.chunk));
	w.bool(request.streams);
	match &request.layouts {
		None => w.u8(0),
		Some(layouts) => {
			w.u8(1);
			put_layouts(&mut w, layouts)?;
		}
	}
	Some(w.into_bytes())
}

/// The request `bytes` encodes.
pub(in crate::exec::vm) fn decode_request(bytes: &[u8]) -> Result<Request> {
	let mut r = Reader::opened(bytes)?;
	let direction = match r.u8()? {
		0 => Direction::Forward,
		1 => Direction::Backward,
		other => bail!("a morsel holds direction {other}"),
	};
	let version = r.opt(|r| r.uvar())?;
	let paged = r.opt(|r| Ok((r.u16()?, r.opt(|r| r.usize())?)))?;
	let chunk = r.u32()?;
	let streams = r.bool()?;
	let layouts = r.opt(get_layouts)?.map(Arc::new);
	r.done()?;
	Ok(Request {
		direction,
		version,
		paged,
		chunk,
		streams,
		layouts,
	})
}

/// A frame a leaf answers: rows the morsel produced, which the statement takes as the morsel
/// runs, or the morsel's end.
pub(super) enum Frame {
	Rows(Vec<Value>),
	End {
		result: Box<std::result::Result<super::run::MorselResult, anyhow::Error>>,
		emitted: u64,
	},
}

const ROWS: u8 = 0;
const END_OK: u8 = 1;
const END_ERR: u8 = 2;

/// The frame of `rows`, produced by the morsel and taken by the statement in that order.
pub(super) fn rows_frame(rows: &[Value]) -> Result<Vec<u8>> {
	let mut w = Writer::opened();
	w.u8(ROWS);
	w.usize(rows.len());
	for row in rows {
		w.value(row)?;
	}
	Ok(w.into_bytes())
}

/// Write the failure `e` as the chain of its messages, outermost first.
fn put_error(w: &mut Writer, e: &anyhow::Error) {
	let chain: Vec<String> = e.chain().map(ToString::to_string).collect();
	w.usize(chain.len());
	for message in &chain {
		w.str(message);
	}
}

/// The failure [`put_error`] wrote: an error whose messages, outermost first, are the chain the
/// leaf's failure held, so it renders as that failure rendered.
fn get_error(r: &mut Reader<'_>) -> Result<anyhow::Error> {
	let chain = (0..r.usize()?).map(|_| r.str().map(str::to_owned)).collect::<Result<Vec<_>>>()?;
	let mut messages = chain.into_iter().rev();
	let mut e = anyhow::Error::msg(messages.next().unwrap_or_default());
	for message in messages {
		e = e.context(message);
	}
	Ok(e)
}

fn put_control(w: &mut Writer, flow: &crate::expr::ControlFlow) {
	match flow {
		crate::expr::ControlFlow::Err(e) => put_error(w, e),
		other @ crate::expr::ControlFlow::Break
		| other @ crate::expr::ControlFlow::Continue
		| other @ crate::expr::ControlFlow::Return(_) => {
			put_error(w, &anyhow::anyhow!("a morsel's row ended with {other:?}"))
		}
	}
}

fn put_sort_row(w: &mut Writer, row: &super::super::interp::SortRow) -> Result<()> {
	use super::super::interp::SortRow;
	match row {
		SortRow::Value(value) => {
			w.u8(0);
			w.value(value)?;
		}
		SortRow::Stored {
			bytes,
			split,
		} => {
			w.u8(1);
			w.bytes(bytes);
			w.usize(*split);
		}
		SortRow::Slab {
			start,
			split,
			end,
		} => {
			w.u8(2);
			w.usize(*start);
			w.usize(*split);
			w.usize(*end);
		}
	}
	Ok(())
}

fn get_sort_row(r: &mut Reader<'_>) -> Result<super::super::interp::SortRow> {
	use super::super::interp::SortRow;
	Ok(match r.u8()? {
		0 => SortRow::Value(r.value()?),
		1 => SortRow::Stored {
			bytes: r.bytes()?.to_vec(),
			split: r.usize()?,
		},
		2 => SortRow::Slab {
			start: r.usize()?,
			split: r.usize()?,
			end: r.usize()?,
		},
		other => bail!("a morsel holds sort row {other}"),
	})
}

/// The frame of a morsel's end on the leaf: its result, its breakers numbered as the unit numbers
/// them, or its failure. A result holding a breaker that spilled, or an accumulator whose state
/// cannot be saved, is written as a failure: a leaf holds no store to spill to.
pub(super) fn end_frame(
	end: std::result::Result<super::run::MorselResult, anyhow::Error>,
	streamed: u64,
	reads_row: bool,
) -> Vec<u8> {
	let result = match end {
		Err(e) => return failed_frame(&e),
		Ok(result) => result,
	};
	match put_result(&result, streamed, reads_row) {
		Ok(bytes) => bytes,
		Err(e) => failed_frame(&e),
	}
}

fn failed_frame(e: &anyhow::Error) -> Vec<u8> {
	let mut w = Writer::opened();
	w.u8(END_ERR);
	put_error(&mut w, e);
	w.into_bytes()
}

fn put_result(
	result: &super::run::MorselResult,
	streamed: u64,
	reads_row: bool,
) -> Result<Vec<u8>> {
	use super::run::End;
	ensure!(result.spilled.is_empty(), "a morsel at a host place spilled a breaker");
	let mut w = Writer::opened();
	w.u8(END_OK);
	// What the leaf did, first, so that a host reads it without the program the rest is read
	// through ([`frame_stats`]).
	let [entries, key_bytes, value_bytes] = result.scanned;
	put_stats(
		&mut w,
		&LeafStats {
			entries,
			key_bytes,
			value_bytes,
			chunks: result.chunks,
			elapsed_ns: u64::try_from(result.elapsed.as_nanos()).unwrap_or(u64::MAX),
			rows_judged: result.rows as u64,
			rows_emitted: streamed + result.output.len() as u64,
			column_reads: if reads_row {
				result.rows as u64
			} else {
				0
			},
			decoded: result.decoded,
			reads_row,
		},
	);
	w.uvar(result.bytes);
	w.usize(result.held);
	w.usize(result.output.len());
	for row in &result.output {
		w.value(row)?;
	}
	w.usize(result.sorts.len());
	for (at, entries) in result.sorts.iter().enumerate() {
		w.usize(entries.len());
		for (keys, row) in entries {
			w.usize(keys.len());
			for key in keys {
				w.value(key)?;
			}
			put_sort_row(&mut w, row)?;
		}
		w.bytes(result.slabs.get(at).map_or(&[][..], Vec::as_slice));
		let arrivals = result.arrivals.get(at).copied().unwrap_or_default();
		w.opt(arrivals.run, |w, run| w.usize(run));
		w.bool(arrivals.floored);
		w.uvar(arrivals.arrived);
		w.usize(arrivals.kept);
		w.opt(arrivals.merge, |w, merge| {
			for word in merge.words() {
				w.usize(word);
			}
		});
	}
	w.usize(result.groups.len());
	for groups in &result.groups {
		match groups {
			None => w.u8(0),
			Some(groups) => {
				w.u8(1);
				let bytes = groups
					.put_wire()
					.context("a morsel's aggregate holds a state that cannot be saved")?;
				w.bytes(&bytes);
			}
		}
	}
	w.usize(result.ends.len());
	for end in &result.ends {
		match end {
			End::Passed(rows) => {
				w.u8(0);
				w.usize(*rows);
			}
			End::Row(row) => {
				w.u8(1);
				w.bool(row.reached);
				w.bool(row.skipped);
				w.usize(row.emitted);
				match &row.failure {
					None => w.u8(0),
					Some(flow) => {
						w.u8(1);
						put_control(&mut w, flow);
					}
				}
				match &row.key {
					None => w.u8(0),
					Some(key) => {
						w.u8(1);
						w.value(key)?;
					}
				}
			}
		}
	}
	Ok(w.into_bytes())
}

/// The frame `bytes`, a leaf's answer for a morsel of the pipeline `program` holds at the unit's
/// maps `maps`: a result's breakers are numbered as `program` numbers them, and its groups are
/// read with the functions `functions` holds for `program`'s aggregates.
pub(super) fn decode_frame(
	bytes: &[u8],
	program: &Program,
	maps: &UnitMaps,
	functions: &super::super::interp::AggFunctions,
) -> Result<Frame> {
	use super::super::interp::{Arrivals, Merge};
	use super::run::{End, MorselResult, RowEnd};
	let mut r = Reader::opened(bytes)?;
	match r.u8()? {
		ROWS => {
			let rows = (0..r.usize()?).map(|_| r.value()).collect::<Result<Vec<_>>>()?;
			r.done()?;
			Ok(Frame::Rows(rows))
		}
		END_ERR => {
			let e = get_error(&mut r)?;
			r.done()?;
			Ok(Frame::End {
				result: Box::new(Err(e)),
				emitted: 0,
			})
		}
		END_OK => {
			let stats = get_stats(&mut r)?;
			let bytes = r.uvar()?;
			let held = r.usize()?;
			let output = (0..r.usize()?).map(|_| r.value()).collect::<Result<Vec<_>>>()?;
			let mut sorts: Vec<Vec<_>> = (0..program.sorts.len()).map(|_| Vec::new()).collect();
			let mut slabs: Vec<Vec<u8>> = (0..program.sorts.len()).map(|_| Vec::new()).collect();
			let mut arrivals = vec![Arrivals::default(); program.sorts.len()];
			let count = r.usize()?;
			ensure!(count == maps.sorts.len(), "a morsel answers other sorts than its unit's");
			for &into in &maps.sorts {
				let into = into as usize;
				ensure!(into < program.sorts.len(), "a morsel answers a sort its program lacks");
				let entries = (0..r.usize()?)
					.map(|_| {
						let keys =
							(0..r.usize()?).map(|_| r.value()).collect::<Result<Vec<_>>>()?;
						Ok((keys, get_sort_row(&mut r)?))
					})
					.collect::<Result<Vec<_>>>()?;
				sorts[into] = entries;
				slabs[into] = r.bytes()?.to_vec();
				arrivals[into] = Arrivals {
					run: r.opt(|r| r.usize())?,
					floored: r.bool()?,
					arrived: r.uvar()?,
					kept: r.usize()?,
					merge: r.opt(|r| {
						let mut words = [0usize; 7];
						for word in &mut words {
							*word = r.usize()?;
						}
						Ok(Merge::from_words(words))
					})?,
				};
			}
			let mut groups: Vec<Option<super::super::interp::Groups>> =
				(0..program.aggregates.len()).map(|_| None).collect();
			let count = r.usize()?;
			ensure!(
				count == maps.aggregates.len(),
				"a morsel answers other aggregates than its unit's"
			);
			for &into in &maps.aggregates {
				let into = into as usize;
				if r.u8()? == 1 {
					let functions = functions
						.get(into)
						.context("a morsel answers an aggregate its program lacks")?;
					groups[into] =
						Some(super::super::interp::Groups::get_wire(r.bytes()?, functions)?);
				}
			}
			let ends = (0..r.usize()?)
				.map(|_| {
					Ok(match r.u8()? {
						0 => End::Passed(r.usize()?),
						_ => End::Row(RowEnd {
							reached: r.bool()?,
							skipped: r.bool()?,
							emitted: r.usize()?,
							failure: match r.u8()? {
								0 => None,
								_ => Some(crate::expr::ControlFlow::Err(get_error(&mut r)?)),
							},
							key: match r.u8()? {
								0 => None,
								_ => Some(r.value()?),
							},
						}),
					})
				})
				.collect::<Result<Vec<_>>>()?;
			let rows = stats.rows_judged as usize;
			let elapsed = std::time::Duration::from_nanos(stats.elapsed_ns);
			let chunks = stats.chunks;
			let scanned = [stats.entries, stats.key_bytes, stats.value_bytes];
			let decoded = stats.decoded;
			r.done()?;
			Ok(Frame::End {
				result: Box::new(Ok(MorselResult {
					output,
					sorts,
					slabs,
					arrivals,
					groups,
					spilled: Vec::new(),
					sort_accounts: Vec::new(),
					group_accounts: Vec::new(),
					ends,
					rows,
					elapsed,
					bytes,
					chunks,
					held,
					scanned,
					decoded,
				})),
				emitted: stats.rows_emitted,
			})
		}
		other => bail!("a morsel frame of kind {other}"),
	}
}

/// What a leaf did for one morsel, as its end frame states it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeafStats {
	/// The entries the leaf read from its reader, and their key and value bytes.
	pub entries: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
	/// The reads the entries came in.
	pub chunks: u64,
	/// How long the morsel ran at the leaf.
	pub elapsed_ns: u64,
	/// The entries the body ran over.
	pub rows_judged: u64,
	/// The rows the morsel handed to the statement's output, streamed or in its end.
	pub rows_emitted: u64,
	/// The entries the body read by the columns it names rather than decoded whole: every entry
	/// it ran over where [`Self::reads_row`] holds, none otherwise.
	pub column_reads: u64,
	/// The entries the leaf decoded whole: those whose row the body handed on as a value. Every
	/// other entry was judged, and read where it was read, in its stored bytes.
	pub decoded: u64,
	/// Whether the unit's body reads fields of its row and never takes the row whole, so that a
	/// packed record is read by its columns.
	pub reads_row: bool,
}

fn put_stats(w: &mut Writer, stats: &LeafStats) {
	for count in [
		stats.entries,
		stats.key_bytes,
		stats.value_bytes,
		stats.chunks,
		stats.elapsed_ns,
		stats.rows_judged,
		stats.rows_emitted,
		stats.column_reads,
		stats.decoded,
	] {
		w.uvar(count);
	}
	w.bool(stats.reads_row);
}

fn get_stats(r: &mut Reader<'_>) -> Result<LeafStats> {
	Ok(LeafStats {
		entries: r.uvar()?,
		key_bytes: r.uvar()?,
		value_bytes: r.uvar()?,
		chunks: r.uvar()?,
		elapsed_ns: r.uvar()?,
		rows_judged: r.uvar()?,
		rows_emitted: r.uvar()?,
		column_reads: r.uvar()?,
		decoded: r.uvar()?,
		reads_row: r.bool()?,
	})
}

/// The leaf's figures in `frame`, a morsel's end frame; `None` for any other frame, a failed
/// morsel's end, or bytes this build does not read.
pub fn frame_stats(frame: &[u8]) -> Option<LeafStats> {
	let mut r = Reader::opened(frame).ok()?;
	if r.u8().ok()? != END_OK {
		return None;
	}
	get_stats(&mut r).ok()
}

/// Whether the body of the unit `unit` reads fields of its row and never takes the row whole
/// ([`LeafStats::reads_row`]).
pub fn unit_reads_row(unit: &[u8]) -> Result<bool> {
	let mut r = Reader::opened(unit)?;
	let _table = r.str()?;
	let _registers = r.usize()?;
	r.bool()
}
