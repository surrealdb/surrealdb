//! The value-dependence record: which plan decision consumed which value known at plan time.
//!
//! A decision that consumes a bind parameter, a session value, a request `LET` or a literal of
//! the statement text records the dependence here, and nothing else is recorded about values.
//! A caching policy reads this record to choose, per value, between folding it into a program and
//! reading it from a slot. It needs nothing else from the IR.
//!
//! A record belongs to one planning session and is dropped with it. It names a decision by its
//! [`FoldConsumer`] and a value by slot, name or literal ordinal, never by an op or value id,
//! because passes replace ops: the same shape compiled twice yields equal records whatever its
//! whitespace or its position in the request.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use smallvec::SmallVec;
use surrealdb_strand::Strand;

use super::slots::SlotTable;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::core::{Ir, OpData, OpKind, Provenance};
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::choose::ValueKindClass;
use crate::exec::ir::iface::fold::FoldConsumer;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::premise::SlotId;

/// The most distinct dependences one statement records. Past it the statement is marked
/// overflowed and recording stops.
pub(crate) const MAX_VALUE_DEPS: usize = 256;

/// The most ops [`DepRecorder::consumed`] visits walking back from one value. Past it the
/// statement is marked overflowed.
pub(crate) const MAX_CONSUMED_WALK: usize = 4096;

/// The position of a literal of the statement text among the statement's literals, in pre-order.
///
/// Independent of whitespace, comments and the statement's position in its request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct LiteralOrdinal(pub(crate) u32);

/// Where a bound name's value came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum BindingOrigin {
	/// Passed by the client with the request.
	ClientParam,
	/// A session variable.
	SessionVar,
	/// A `LET` whose value is a constant.
	LetConst,
	/// A `LET` whose value is computed. A name with no recorded origin is this.
	LetComputed,
}

/// A value known at plan time.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum ValueRef {
	/// A bind parameter, session variable or request `LET`, by slot. Its origin is in
	/// [`ValueDeps::origins`].
	Slot(SlotId),
	/// `$auth`, `$session`, `$token` or `$access`, by root name.
	Principal(Strand),
	/// A `DEFINE PARAM ... PERMISSIONS FULL` read with no binding in scope.
	Param(Strand),
	/// A name bound nowhere, consumed as `NONE`.
	Absent(Strand),
	/// A literal of the statement text.
	Literal(LiteralOrdinal),
}

/// How a decision used a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum ValueUse {
	/// The decision depends on the value itself.
	Exact,
	/// The decision depends only on the class of the value.
	Kind(ValueKindClass),
	/// The decision depends on the name having no value.
	Unbound,
}

/// One decision's use of one value.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ValueDep {
	pub(crate) value: ValueRef,
	/// The decision that consumed the value.
	pub(crate) consumer: FoldConsumer,
	pub(crate) used: ValueUse,
}

/// The dependences of one statement: sorted, deduplicated, and bounded by [`MAX_VALUE_DEPS`].
///
/// A statement that overflows depends on every value, as far as any policy reading the record is
/// concerned.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ValueDeps {
	deps: SmallVec<[ValueDep; 4]>,
	origins: SmallVec<[(SlotId, BindingOrigin); 4]>,
	overflow: bool,
}

impl ValueDeps {
	/// An empty record.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Records `dep`. A dependence already held is not held twice, and the record stays sorted.
	/// Once [`MAX_VALUE_DEPS`] distinct dependences are held, a further new one marks the record
	/// overflowed and is not stored.
	pub(crate) fn record(&mut self, dep: ValueDep) {
		match self.deps.binary_search(&dep) {
			Ok(_) => {}
			Err(_) if self.deps.len() >= MAX_VALUE_DEPS => self.overflow = true,
			Err(at) => self.deps.insert(at, dep),
		}
	}

	/// Records where the value of `slot` came from. The first origin recorded for a slot stands.
	pub(crate) fn note_origin(&mut self, slot: SlotId, origin: BindingOrigin) {
		if !self.origins.iter().any(|(held, _)| *held == slot) {
			self.origins.push((slot, origin));
			self.origins.sort_unstable_by_key(|(slot, _)| *slot);
		}
	}

	/// Marks the record as depending on every value. A recorder stores nothing further in it.
	pub(crate) fn mark_overflowed(&mut self) {
		self.overflow = true;
	}

	/// The dependences, sorted.
	pub(crate) fn deps(&self) -> &[ValueDep] {
		&self.deps
	}

	/// The origin of the value of `slot`, or `None` when none was recorded.
	pub(crate) fn origin(&self, slot: SlotId) -> Option<BindingOrigin> {
		self.origins.iter().find(|(held, _)| *held == slot).map(|(_, origin)| *origin)
	}

	/// Whether more than [`MAX_VALUE_DEPS`] distinct dependences were seen.
	pub(crate) fn overflowed(&self) -> bool {
		self.overflow
	}
}

/// The literal of the statement text each bound `expr.literal` op stands for.
///
/// Filled by the binder. Statement-relative: the ops and the ordinals belong to one statement.
#[derive(Clone, Debug, Default)]
pub(crate) struct LiteralTable(Vec<(OpId, LiteralOrdinal)>);

impl LiteralTable {
	/// An empty table.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Records that `op` stands for the literal at `ordinal`. An op already held takes the new
	/// ordinal.
	pub(crate) fn insert(&mut self, op: OpId, ordinal: LiteralOrdinal) {
		match self.0.binary_search_by_key(&op, |(held, _)| *held) {
			Ok(at) => self.0[at].1 = ordinal,
			Err(at) => self.0.insert(at, (op, ordinal)),
		}
	}

	/// The literal `op` stands for, or `None` when it stands for none.
	pub(crate) fn get(&self, op: OpId) -> Option<LiteralOrdinal> {
		self.0.binary_search_by_key(&op, |(held, _)| *held).ok().map(|at| self.0[at].1)
	}
}

/// The value-dependence record of one compile, per statement.
///
/// The interior is a `std::sync::Mutex`, so a planner future holding the recorder stays `Send`.
/// The lock is held only for the duration of one call and never across an `.await`. Every method
/// that records acts on the statement entered last, and does nothing outside a statement.
pub(crate) struct DepRecorder {
	state: Mutex<RecorderState>,
}

struct RecorderState {
	/// The statement being planned.
	current: Option<usize>,
	/// The record of every statement entered or absorbed, by statement index.
	records: BTreeMap<usize, ValueDeps>,
}

impl DepRecorder {
	/// A recorder holding no record.
	pub(crate) fn new() -> Self {
		Self {
			state: Mutex::new(RecorderState {
				current: None,
				records: BTreeMap::new(),
			}),
		}
	}

	/// Starts recording for statement `index`. The statement has a record from here on, empty
	/// until something is recorded. Entering a statement ends the one entered before it.
	pub(crate) fn enter(&self, index: usize) {
		let mut state = self.lock();
		state.current = Some(index);
		state.records.entry(index).or_default();
	}

	/// Ends the current statement.
	pub(crate) fn leave(&self) {
		self.lock().current = None;
	}

	/// Records `dep` for the current statement. Outside a statement, or once that statement's
	/// record is overflowed, nothing is recorded.
	pub(crate) fn record(&self, dep: ValueDep) {
		self.with_current(|deps| {
			if !deps.overflowed() {
				deps.record(dep);
			}
		});
	}

	/// Records where the value of `slot` came from, for the current statement. The first origin
	/// recorded for a slot stands.
	pub(crate) fn note_origin(&self, slot: SlotId, origin: BindingOrigin) {
		self.with_current(|deps| {
			if !deps.overflowed() {
				deps.note_origin(slot, origin);
			}
		});
	}

	/// Marks the current statement as depending on every value.
	pub(crate) fn mark_overflowed(&self) {
		self.with_current(ValueDeps::mark_overflowed);
	}

	/// Merges a record another phase produced for statement `index`.
	///
	/// The dependences and origins of `deps` are merged first, so the record keeps whatever the
	/// phase did name, a principal dependence included. `complete == false` states that the phase
	/// could not record everything it consumed, which then marks the statement overflowed, as does
	/// an overflowed `deps`. A statement already overflowed takes nothing.
	pub(crate) fn absorb(&self, index: usize, deps: &ValueDeps, complete: bool) {
		let mut state = self.lock();
		let held = state.records.entry(index).or_default();
		if held.overflowed() {
			return;
		}
		for dep in deps.deps() {
			held.record(dep.clone());
		}
		for (slot, origin) in &deps.origins {
			held.note_origin(*slot, *origin);
		}
		if !complete || deps.overflowed() {
			held.mark_overflowed();
		}
	}

	/// Records what decision `consumer` consumed through `value`, for the current statement.
	///
	/// The walk visits the ops that define `value`, back through the `expr` ops that compute their
	/// result from their operands alone, to the roots a value known at plan time comes from:
	/// - an `expr.literal` records `Literal(ordinal)` when its op, or under `Provenance::From` an
	///   op it stands for, is in `literals`;
	/// - an `expr.free_param` records `Slot` of its name, or marks the statement overflowed when
	///   `slots` holds no slot for the name;
	/// - an `expr.session_param` records `Principal`, and an `expr.catalog_param` records `Param`.
	///
	/// The walk also continues through an op that calls a function or a method, applies a closure,
	/// branches or short-circuits, or builds a closure or destructure: such an op is not itself a
	/// value known at plan time, but the decision depends on every value it computes from. It
	/// continues through the operands and through every op the op's regions hold, so a name or
	/// literal read in a branch is recorded too.
	///
	/// A root that is a block argument marks the statement overflowed, since its source is not
	/// named by the walk. A root that is an op of another dialect, or an `expr` op that reads a
	/// row, storage, the clock or entropy, ends the walk with no record: what it yields is not a
	/// value known at plan time. The walk visits each op once. Past [`MAX_CONSUMED_WALK`] ops it
	/// stops and marks the statement overflowed.
	///
	/// A dependence is recorded as [`ValueUse::Exact`]. Outside a statement nothing is walked.
	pub(crate) fn consumed(
		&self,
		ir: &Ir,
		value: ValueId,
		consumer: FoldConsumer,
		slots: &SlotTable,
		literals: &LiteralTable,
	) {
		if self.lock().current.is_none() {
			return;
		}
		let mut reached = Reached::default();
		walk_consumed(ir, value, slots, literals, &mut reached);
		self.with_current(|deps| {
			for value in reached.values {
				if deps.overflowed() {
					break;
				}
				deps.record(ValueDep {
					value,
					consumer,
					used: ValueUse::Exact,
				});
			}
			if reached.overflow {
				deps.mark_overflowed();
			}
		});
	}

	/// The records, sorted by statement index. Consumes the recorder.
	pub(crate) fn finish(self) -> Vec<(usize, ValueDeps)> {
		self.state
			.into_inner()
			.unwrap_or_else(PoisonError::into_inner)
			.records
			.into_iter()
			.collect()
	}

	/// Runs `f` on the record of the current statement, when there is one.
	fn with_current(&self, f: impl FnOnce(&mut ValueDeps)) {
		let mut state = self.lock();
		if let Some(index) = state.current {
			f(state.records.entry(index).or_default());
		}
	}

	/// The state. Every update leaves it consistent, so a lock poisoned by a panic elsewhere is
	/// still sound to read.
	fn lock(&self) -> MutexGuard<'_, RecorderState> {
		self.state.lock().unwrap_or_else(PoisonError::into_inner)
	}
}

impl Default for DepRecorder {
	fn default() -> Self {
		Self::new()
	}
}

/// What one walk of [`DepRecorder::consumed`] reached.
#[derive(Default)]
struct Reached {
	values: Vec<ValueRef>,
	overflow: bool,
}

/// Walks back from `start` and collects the values it reaches into `reached`.
fn walk_consumed(
	ir: &Ir,
	start: ValueId,
	slots: &SlotTable,
	literals: &LiteralTable,
	reached: &mut Reached,
) {
	let mut pending = vec![start];
	let mut visited: HashSet<OpId> = HashSet::new();
	while let Some(value) = pending.pop() {
		let op = match ir.value(value).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => op,
			// The value a block's argument holds comes from outside the walk.
			Some(ValueDef::BlockArg {
				..
			}) => {
				reached.overflow = true;
				continue;
			}
			None => continue,
		};
		if !visited.insert(op) {
			continue;
		}
		if visited.len() > MAX_CONSUMED_WALK {
			reached.overflow = true;
			return;
		}
		let Some(data) = ir.op(op) else {
			continue;
		};
		let OpKind::Expr(kind) = &data.kind else {
			continue;
		};
		match kind {
			ExprOp::Literal(..) => literal_roots(op, data, literals, reached),
			ExprOp::FreeParam {
				name,
			} => match slots.id_of(name.as_str()) {
				Some(slot) => reached.values.push(ValueRef::Slot(slot)),
				None => reached.overflow = true,
			},
			ExprOp::SessionParam {
				name,
			} => reached.values.push(ValueRef::Principal(name.clone())),
			ExprOp::CatalogParam {
				name,
			} => reached.values.push(ValueRef::Param(name.clone())),
			// The result is a function of the operands alone.
			ExprOp::Array
			| ExprOp::Set
			| ExprOp::Object {
				..
			}
			| ExprOp::RecordId(..)
			| ExprOp::Access {
				..
			}
			| ExprOp::Index
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Flatten {
				..
			}
			| ExprOp::SetField {
				..
			}
			| ExprOp::Prefix(..)
			| ExprOp::Convert(..)
			| ExprOp::Postfix {
				..
			}
			| ExprOp::Binary {
				..
			}
			| ExprOp::Truthy => pending.extend(data.operands.iter().copied()),
			// An op whose result is not a value known at plan time, but which computes from its
			// operands and from the ops of the regions it owns.
			ExprOp::Dispatch(..)
			| ExprOp::Method {
				..
			}
			| ExprOp::Invoke
			| ExprOp::Call(..)
			| ExprOp::Destructure {
				..
			}
			| ExprOp::Optional
			| ExprOp::ShortCircuit {
				..
			}
			| ExprOp::ClosureCreate(..)
			| ExprOp::TupleRow {
				..
			}
			| ExprOp::If => {
				pending.extend(data.operands.iter().copied());
				for region in &data.regions {
					for held in ops_in(ir, *region) {
						if let Some(inner) = ir.op(held) {
							pending.extend(inner.results.iter().copied());
							pending.extend(inner.operands.iter().copied());
						}
					}
				}
			}
			// An op that reads a row, storage, the clock or entropy, owns a region whose result is
			// not a function of plan-time values, or ends one, ends the walk with no record: what
			// it yields is not a value known at plan time.
			ExprOp::Table {
				..
			}
			| ExprOp::Mock(..)
			| ExprOp::Constant {
				..
			}
			| ExprOp::Field {
				..
			}
			| ExprOp::ComputedRead(..)
			| ExprOp::Doc
			| ExprOp::Deref {
				..
			}
			| ExprOp::Each
			| ExprOp::RepeatRecurse
			| ExprOp::Throw
			| ExprOp::Subquery(_)
			| ExprOp::Yield
			| ExprOp::Agg {
				..
			}
			| ExprOp::Now {
				..
			}
			| ExprOp::Raise(..)
			| ExprOp::DepthCharge {
				..
			} => {}
		}
	}
}

/// The literals of the statement text the `expr.literal` op `op` stands for: itself when bound,
/// and every op it derives from otherwise.
fn literal_roots(op: OpId, data: &OpData, literals: &LiteralTable, reached: &mut Reached) {
	let stood_for = match &data.provenance {
		Provenance::Bound => SmallVec::<[OpId; 2]>::from_slice(&[op]),
		Provenance::From(origin) => {
			let mut ops = SmallVec::from_slice(&[op]);
			ops.extend(origin.iter().copied());
			ops
		}
	};
	for held in stood_for {
		if let Some(ordinal) = literals.get(held) {
			reached.values.push(ValueRef::Literal(ordinal));
		}
	}
}

/// The names a kept unit's `ContextDeps` must also guard: the slot names, `Param` names and
/// `Absent` names `deps` holds. A slot `slots` holds no name for is skipped.
///
/// `None` for an overflowed record, which depends on every value: no name set guards it, so the
/// caller must not keep a unit under it. Principal dependences are excluded from the set:
/// [`reads_principal`] routes those.
pub(crate) fn context_names(deps: &ValueDeps, slots: &SlotTable) -> Option<BTreeSet<String>> {
	if deps.overflowed() {
		return None;
	}
	let mut names = BTreeSet::new();
	for dep in deps.deps() {
		match &dep.value {
			ValueRef::Slot(slot) => {
				if let Some(name) = slots.name(*slot) {
					names.insert(name.to_owned());
				}
			}
			ValueRef::Param(name) | ValueRef::Absent(name) => {
				names.insert(name.as_str().to_owned());
			}
			ValueRef::Principal(..) | ValueRef::Literal(..) => {}
		}
	}
	Some(names)
}

/// Whether a decision consumed a principal value, which keeps the statement only through
/// `request_statement`, under the values it folded.
///
/// An overflowed record depends on every value, a principal's among them, so it answers `true`
/// whether or not a principal dependence was stored.
pub(crate) fn reads_principal(deps: &ValueDeps) -> bool {
	deps.overflowed() || deps.deps().iter().any(|dep| matches!(dep.value, ValueRef::Principal(..)))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::{
		FactCache, InsertPoint, IrScope, LocOrigin, Location, OpSpec, Rewriter,
	};
	use crate::exec::ir::expr::ops::ShortCircuitOp;
	use crate::exec::ir::iface::ids::{FrameId, SnapshotId};
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::expr::{BinaryOperator, Function};
	use crate::val::Value;

	fn dep(slot: u16, consumer: FoldConsumer) -> ValueDep {
		ValueDep {
			value: ValueRef::Slot(SlotId(slot)),
			consumer,
			used: ValueUse::Exact,
		}
	}

	#[test]
	fn a_record_is_sorted_and_deduplicated() {
		let mut deps = ValueDeps::new();
		deps.record(dep(2, FoldConsumer::IndexChoice));
		deps.record(dep(1, FoldConsumer::IndexChoice));
		deps.record(dep(2, FoldConsumer::IndexChoice));
		assert_eq!(
			deps.deps(),
			[dep(1, FoldConsumer::IndexChoice), dep(2, FoldConsumer::IndexChoice)]
		);
		assert!(!deps.overflowed());
	}

	#[test]
	fn a_record_past_its_bound_overflows_and_stops_growing() {
		let mut deps = ValueDeps::new();
		for slot in 0..=MAX_VALUE_DEPS as u16 {
			deps.record(dep(slot, FoldConsumer::BranchChoice));
		}
		assert!(deps.overflowed());
		assert_eq!(deps.deps().len(), MAX_VALUE_DEPS);
	}

	#[test]
	fn the_first_origin_of_a_slot_stands() {
		let mut deps = ValueDeps::new();
		deps.note_origin(SlotId(0), BindingOrigin::ClientParam);
		deps.note_origin(SlotId(0), BindingOrigin::LetComputed);
		assert_eq!(deps.origin(SlotId(0)), Some(BindingOrigin::ClientParam));
		assert_eq!(deps.origin(SlotId(1)), None);
	}

	fn request() -> Ir {
		Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		)
	}

	fn spec(kind: ExprOp, operands: &[ValueId]) -> OpSpec {
		OpSpec {
			kind: OpKind::Expr(kind),
			operands: operands.iter().copied().collect(),
			results: std::iter::once(TypeKnowledge::Unknown).collect(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Synthesized,
			},
		}
	}

	/// Builds one `expr` op at the end of the top block and answers it and its result.
	fn add(ir: &mut Ir, kind: ExprOp, operands: &[ValueId]) -> (OpId, ValueId) {
		let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut facts = FactCache::new();
		let mut rw = Rewriter::new(ir, &mut facts);
		let op = rw.create(InsertPoint::End(block), spec(kind, operands)).expect("the op is built");
		let result = rw.ir().op(op).expect("the op exists").results[0];
		(op, result)
	}

	fn literal(ir: &mut Ir, value: i64) -> (OpId, ValueId) {
		add(ir, ExprOp::Literal(Box::new(Value::from(value))), &[])
	}

	fn free(ir: &mut Ir, name: &str) -> ValueId {
		add(
			ir,
			ExprOp::FreeParam {
				name: Strand::from(name),
			},
			&[],
		)
		.1
	}

	fn slots_of(query: &str) -> SlotTable {
		let mut ast = crate::syn::parse(query).expect("the query parses");
		SlotTable::of(&ast.expressions.remove(0))
	}

	fn consumed(
		recorder: &DepRecorder,
		ir: &Ir,
		value: ValueId,
		slots: &SlotTable,
		literals: &LiteralTable,
	) {
		recorder.consumed(ir, value, FoldConsumer::BranchChoice, slots, literals);
	}

	fn only(recorder: DepRecorder) -> ValueDeps {
		let mut records = recorder.finish();
		assert_eq!(records.len(), 1);
		records.remove(0).1
	}

	#[test]
	fn a_marked_record_stays_overflowed_and_keeps_what_it_held() {
		let mut deps = ValueDeps::new();
		deps.record(dep(0, FoldConsumer::IndexChoice));
		deps.mark_overflowed();
		assert!(deps.overflowed());
		assert_eq!(deps.deps(), [dep(0, FoldConsumer::IndexChoice)]);
	}

	#[test]
	fn a_literal_table_answers_by_op_and_replaces_on_reinsert() {
		let mut ir = request();
		let (first, _) = literal(&mut ir, 1);
		let (second, _) = literal(&mut ir, 2);
		let (third, _) = literal(&mut ir, 3);
		let mut table = LiteralTable::new();
		table.insert(second, LiteralOrdinal(7));
		table.insert(first, LiteralOrdinal(4));
		assert_eq!(table.get(first), Some(LiteralOrdinal(4)));
		assert_eq!(table.get(second), Some(LiteralOrdinal(7)));
		assert_eq!(table.get(third), None);
		table.insert(first, LiteralOrdinal(9));
		assert_eq!(table.get(first), Some(LiteralOrdinal(9)));
	}

	#[test]
	fn the_recorder_records_only_inside_a_statement() {
		let recorder = DepRecorder::new();
		recorder.record(dep(0, FoldConsumer::IndexChoice));
		recorder.enter(3);
		recorder.record(dep(1, FoldConsumer::IndexChoice));
		recorder.leave();
		recorder.record(dep(2, FoldConsumer::IndexChoice));
		let records = recorder.finish();
		assert_eq!(records.len(), 1);
		assert_eq!(records[0].0, 3);
		assert_eq!(records[0].1.deps(), [dep(1, FoldConsumer::IndexChoice)]);
	}

	#[test]
	fn the_recorder_answers_statements_in_index_order() {
		let recorder = DepRecorder::new();
		recorder.enter(2);
		recorder.leave();
		recorder.enter(0);
		recorder.leave();
		let order: Vec<usize> = recorder.finish().into_iter().map(|(index, _)| index).collect();
		assert_eq!(order, [0, 2]);
	}

	#[test]
	fn an_overflowed_statement_takes_nothing_more() {
		let recorder = DepRecorder::new();
		recorder.enter(0);
		recorder.record(dep(0, FoldConsumer::IndexChoice));
		recorder.mark_overflowed();
		recorder.record(dep(1, FoldConsumer::IndexChoice));
		recorder.note_origin(SlotId(1), BindingOrigin::ClientParam);
		let deps = only(recorder);
		assert!(deps.overflowed());
		assert_eq!(deps.deps(), [dep(0, FoldConsumer::IndexChoice)]);
		assert_eq!(deps.origin(SlotId(1)), None);
	}

	#[test]
	fn absorbing_merges_dependences_and_origins() {
		let mut other = ValueDeps::new();
		other.record(dep(1, FoldConsumer::TypeFact));
		other.note_origin(SlotId(1), BindingOrigin::SessionVar);
		let recorder = DepRecorder::new();
		recorder.enter(0);
		recorder.record(dep(0, FoldConsumer::TypeFact));
		recorder.leave();
		recorder.absorb(0, &other, true);
		let deps = only(recorder);
		assert!(!deps.overflowed());
		assert_eq!(deps.deps(), [dep(0, FoldConsumer::TypeFact), dep(1, FoldConsumer::TypeFact)]);
		assert_eq!(deps.origin(SlotId(1)), Some(BindingOrigin::SessionVar));
	}

	#[test]
	fn an_incomplete_or_overflowed_absorb_overflows_the_statement() {
		let complete = ValueDeps::new();
		let mut overflowed = ValueDeps::new();
		overflowed.mark_overflowed();
		let recorder = DepRecorder::new();
		recorder.absorb(0, &complete, false);
		recorder.absorb(1, &overflowed, true);
		recorder.absorb(2, &complete, true);
		let records = recorder.finish();
		let flags: Vec<bool> = records.iter().map(|(_, deps)| deps.overflowed()).collect();
		assert_eq!(flags, [true, true, false]);
	}

	#[test]
	fn absorbing_an_incomplete_record_keeps_what_it_named() {
		let mut named = ValueDeps::new();
		named.record(dep(1, FoldConsumer::IndexChoice));
		named.note_origin(SlotId(1), BindingOrigin::ClientParam);
		let recorder = DepRecorder::new();
		recorder.absorb(0, &named, false);
		let deps = only(recorder);
		assert!(deps.overflowed());
		assert_eq!(deps.deps(), [dep(1, FoldConsumer::IndexChoice)]);
		assert_eq!(deps.origin(SlotId(1)), Some(BindingOrigin::ClientParam));
	}

	#[test]
	fn consumed_walks_operators_to_a_slot_and_a_literal() {
		let slots = slots_of("RETURN $a + 1");
		let mut ir = request();
		let a = free(&mut ir, "a");
		let (one, one_value) = literal(&mut ir, 1);
		let (_, sum) = add(
			&mut ir,
			ExprOp::Binary {
				op: BinaryOperator::Add,
			},
			&[a, one_value],
		);
		let mut literals = LiteralTable::new();
		literals.insert(one, LiteralOrdinal(0));
		let recorder = DepRecorder::new();
		recorder.enter(0);
		consumed(&recorder, &ir, sum, &slots, &literals);
		let deps = only(recorder);
		let held: Vec<ValueRef> = deps.deps().iter().map(|dep| dep.value.clone()).collect();
		assert_eq!(held, [ValueRef::Slot(SlotId(0)), ValueRef::Literal(LiteralOrdinal(0))]);
		assert!(
			deps.deps().iter().all(
				|dep| dep.consumer == FoldConsumer::BranchChoice && dep.used == ValueUse::Exact
			)
		);
		assert!(!deps.overflowed());
	}

	#[test]
	fn consumed_names_principal_and_stored_parameter_roots() {
		let mut ir = request();
		let (_, auth) = add(
			&mut ir,
			ExprOp::SessionParam {
				name: Strand::from("auth"),
			},
			&[],
		);
		let (_, stored) = add(
			&mut ir,
			ExprOp::CatalogParam {
				name: Strand::from("limit"),
			},
			&[],
		);
		let (_, pair) = add(&mut ir, ExprOp::Array, &[auth, stored]);
		let recorder = DepRecorder::new();
		recorder.enter(0);
		consumed(&recorder, &ir, pair, &SlotTable::default(), &LiteralTable::new());
		let deps = only(recorder);
		assert!(reads_principal(&deps));
		let held: Vec<ValueRef> = deps.deps().iter().map(|dep| dep.value.clone()).collect();
		assert_eq!(
			held,
			[ValueRef::Principal(Strand::from("auth")), ValueRef::Param(Strand::from("limit"))]
		);
	}

	#[test]
	fn consumed_follows_the_ops_a_literal_derives_from() {
		let mut ir = request();
		let (original, _) = literal(&mut ir, 1);
		let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let derived = {
			let mut facts = FactCache::new();
			let mut rw = Rewriter::new(&mut ir, &mut facts);
			let op = rw
				.derived(&[original], |rw| {
					rw.create(
						InsertPoint::End(block),
						spec(ExprOp::Literal(Box::new(Value::from(2))), &[]),
					)
				})
				.expect("the op is built");
			rw.ir().op(op).expect("the op exists").results[0]
		};
		let mut literals = LiteralTable::new();
		literals.insert(original, LiteralOrdinal(5));
		let recorder = DepRecorder::new();
		recorder.enter(0);
		consumed(&recorder, &ir, derived, &SlotTable::default(), &literals);
		let deps = only(recorder);
		assert_eq!(deps.deps()[0].value, ValueRef::Literal(LiteralOrdinal(5)));
	}

	#[test]
	fn consumed_records_nothing_past_an_op_that_is_not_a_pure_operator() {
		let slots = slots_of("RETURN $a");
		let mut ir = request();
		let a = free(&mut ir, "a");
		let (_, read) = add(
			&mut ir,
			ExprOp::Access {
				field: Strand::from("f"),
			},
			&[a],
		);
		let (_, stopped) = add(&mut ir, ExprOp::Each, &[read]);
		let recorder = DepRecorder::new();
		recorder.enter(0);
		consumed(&recorder, &ir, stopped, &slots, &LiteralTable::new());
		let deps = only(recorder);
		assert!(deps.deps().is_empty());
		assert!(!deps.overflowed());
	}

	/// The slots one walk from `value` records, in order.
	fn slots_reached(ir: &Ir, value: ValueId, slots: &SlotTable) -> (Vec<ValueRef>, bool) {
		let recorder = DepRecorder::new();
		recorder.enter(0);
		consumed(&recorder, ir, value, slots, &LiteralTable::new());
		let deps = only(recorder);
		(deps.deps().iter().map(|dep| dep.value.clone()).collect(), deps.overflowed())
	}

	#[test]
	fn consumed_walks_the_operands_of_a_call() {
		let slots = slots_of("RETURN type::table($t)");
		let mut ir = request();
		let t = free(&mut ir, "t");
		let (_, call) =
			add(&mut ir, ExprOp::call(Function::Normal("type::table".to_owned())), &[t]);
		let (held, overflowed) = slots_reached(&ir, call, &slots);
		assert_eq!(held, [ValueRef::Slot(SlotId(0))]);
		assert!(!overflowed);
	}

	#[test]
	fn consumed_walks_the_operands_of_a_short_circuit_and_a_method() {
		let slots = slots_of("RETURN [$a ?? 1, $b.f()]");
		let mut ir = request();
		let a = free(&mut ir, "a");
		let (_, coalesced) = add(
			&mut ir,
			ExprOp::ShortCircuit {
				op: ShortCircuitOp::NullCoalescing,
			},
			&[a],
		);
		let (held, overflowed) = slots_reached(&ir, coalesced, &slots);
		assert_eq!(held, [ValueRef::Slot(SlotId(0))]);
		assert!(!overflowed);

		let b = free(&mut ir, "b");
		let (_, method) = add(
			&mut ir,
			ExprOp::Method {
				name: Strand::from("f"),
			},
			&[b],
		);
		let (held, overflowed) = slots_reached(&ir, method, &slots);
		assert_eq!(held, [ValueRef::Slot(SlotId(1))]);
		assert!(!overflowed);
	}

	#[test]
	fn consumed_overflows_on_a_free_name_with_no_slot() {
		let mut ir = request();
		let missing = free(&mut ir, "missing");
		let recorder = DepRecorder::new();
		recorder.enter(0);
		consumed(&recorder, &ir, missing, &SlotTable::default(), &LiteralTable::new());
		assert!(only(recorder).overflowed());
	}

	#[test]
	fn consumed_overflows_past_the_walk_bound() {
		let slots = slots_of("RETURN $a");
		let mut ir = request();
		let mut value = free(&mut ir, "a");
		for _ in 0..MAX_CONSUMED_WALK {
			value = add(&mut ir, ExprOp::Truthy, &[value]).1;
		}
		let recorder = DepRecorder::new();
		recorder.enter(0);
		consumed(&recorder, &ir, value, &slots, &LiteralTable::new());
		assert!(only(recorder).overflowed());
	}

	#[test]
	fn consumed_outside_a_statement_records_nothing() {
		let slots = slots_of("RETURN $a");
		let mut ir = request();
		let a = free(&mut ir, "a");
		let recorder = DepRecorder::new();
		consumed(&recorder, &ir, a, &slots, &LiteralTable::new());
		assert!(recorder.finish().is_empty());
	}

	#[test]
	fn context_names_hold_slot_param_and_absent_names_but_no_principal() {
		let slots = slots_of("RETURN [$a, $b]");
		let mut deps = ValueDeps::new();
		let consumer = FoldConsumer::BranchChoice;
		for value in [
			ValueRef::Slot(SlotId(1)),
			ValueRef::Slot(SlotId(9)),
			ValueRef::Principal(Strand::from("auth")),
			ValueRef::Param(Strand::from("p")),
			ValueRef::Absent(Strand::from("gone")),
			ValueRef::Literal(LiteralOrdinal(0)),
		] {
			deps.record(ValueDep {
				value,
				consumer,
				used: ValueUse::Exact,
			});
		}
		let names: Vec<String> = context_names(&deps, &slots)
			.expect("a complete record has names")
			.into_iter()
			.collect();
		assert_eq!(names, ["b", "gone", "p"]);
		assert!(reads_principal(&deps));
		assert!(!reads_principal(&ValueDeps::new()));
	}

	#[test]
	fn an_overflowed_record_reads_a_principal_and_names_no_guard() {
		let slots = slots_of("RETURN $a");
		let mut deps = ValueDeps::new();
		deps.record(dep(0, FoldConsumer::BranchChoice));
		assert!(!reads_principal(&deps));
		assert_eq!(context_names(&deps, &slots), Some(BTreeSet::from(["a".to_owned()])));
		deps.mark_overflowed();
		assert!(deps.deps().iter().all(|held| !matches!(held.value, ValueRef::Principal(..))));
		assert!(reads_principal(&deps));
		assert_eq!(context_names(&deps, &slots), None);
	}

	#[test]
	fn a_principal_dependence_past_the_bound_still_reads_as_a_principal() {
		let mut deps = ValueDeps::new();
		for slot in 0..MAX_VALUE_DEPS as u16 {
			deps.record(dep(slot, FoldConsumer::BranchChoice));
		}
		deps.record(ValueDep {
			value: ValueRef::Principal(Strand::from("auth")),
			consumer: FoldConsumer::IndexChoice,
			used: ValueUse::Exact,
		});
		assert!(deps.overflowed());
		assert!(reads_principal(&deps));
	}

	#[test]
	fn absorbing_an_incomplete_record_keeps_a_principal_dependence() {
		let mut principal = ValueDeps::new();
		principal.record(ValueDep {
			value: ValueRef::Principal(Strand::from("auth")),
			consumer: FoldConsumer::IndexChoice,
			used: ValueUse::Exact,
		});
		let recorder = DepRecorder::new();
		recorder.absorb(0, &principal, false);
		let deps = only(recorder);
		assert!(deps.overflowed());
		assert_eq!(deps.deps(), principal.deps());
		assert!(reads_principal(&deps));
	}
}
