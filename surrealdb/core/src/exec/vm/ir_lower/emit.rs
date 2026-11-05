//! The builder lowering emits into: the code buffer and the register, label and pool allocators.
//!
//! An [`Emitter`] belongs to one lowering. Nothing it allocates outlives it or is shared between
//! lowerings, so a pool entry is interned within one emitter only.
//!
//! # Labels
//!
//! A [`Label`] the emitter hands out names a position to be bound later, and is an identifier,
//! never an instruction index. Every jump target in the code of an emitter is an identifier from
//! [`Emitter::label`]. [`Emitter::finish_pipeline`] rewrites each one to the index its
//! [`Emitter::bind`] fixed, so a finished pipeline holds instruction indices only.
//!
//! # Pools
//!
//! The constant, kind, parameter, field and path pools hold each distinct entry once. The call,
//! method, closure literal, graph lookup, subquery, nested statement, user-function call and host
//! call pools hold one entry per site, because each entry names a site of the program rather than
//! a value. Constants are compared by representation and never by `==`, which equates `1`, `1.0`
//! and `1dec`.
//!
//! # Groups
//!
//! A block statement lowered into the program as a group of its own ([`Emitter::open_group`])
//! stamps its pipelines and the verdicts its filters keep with the group. Its permission frames
//! form one contiguous range of the pool. Each open of the statement starts that range afresh, and
//! a frame is shared only with an equal frame of the same group.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Range;

use surrealdb_strand::Strand;

use super::super::program::{
	BlockInstr, CALL_FRAME, CallSpec, ClosureValueSpec, DeferredVector, DegreeSpec, ExternalSpec,
	FetchPlan, FetchStepSpec, FunctionSpec, Instr, KnnQuery, Label, LookupSpec, MatchesSpec,
	MethodSpec, NestedSpec, ParamSpec, PermissionSpec, Pipeline, Program, ROW, RecordIdSpec, Reg,
	Source, SubquerySpec, reads_row, reads_row_register,
};
use super::super::row::FieldName;
use crate::exec::field_path::FieldPath;
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::phys::placement_attr::FrontierHops;
use crate::exec::planner::placement::Parallel;
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::Kind;
use crate::val::{Number, Value};

/// Where a value a block binds is read from.
#[derive(Debug, Clone)]
pub(super) struct BoundSlot {
	/// The slot of the running state the value is held in.
	pub(super) slot: u16,
	/// The name the value is bound to in the frame, when the container carries it.
	pub(super) name: Option<Strand>,
}

/// The code buffer and allocators of one lowering.
///
/// Registers and labels are allocated from the emitter and a pool index is the length of its pool
/// at the entry's first appearance. An allocation that would pass the range of its index type
/// answers [`IrError::Capacity`] and leaves the emitter as it was.
pub(crate) struct Emitter {
	code: Vec<Instr>,
	/// The next register to hand out. [`ROW`] is never handed out.
	registers: u16,
	/// The instruction index each label handed out is bound to, once it is.
	targets: Vec<Option<Label>>,
	consts: Vec<Value>,
	/// The index in `consts` of each distinct constant, keyed by its representation. The keys
	/// derive from user-controlled content, so the map keeps the default hasher.
	const_index: HashMap<ConstKey, u32>,
	kinds: Vec<Kind>,
	calls: Vec<CallSpec>,
	matches: Vec<MatchesSpec>,
	methods: Vec<MethodSpec>,
	params: Vec<ParamSpec>,
	fields: Vec<String>,
	paths: Vec<FieldPath>,
	record_ids: Vec<RecordIdSpec>,
	closure_values: Vec<ClosureValueSpec>,
	host_code: Vec<Pipeline>,
	/// Each fetch of the program: its paths and whether it reads its links in windows.
	fetches: Vec<FetchPlan>,
	/// The frames the program's permission predicates and layer columns are evaluated in.
	permissions: Vec<PermissionSpec>,
	/// The group whose start forgets each verdict a filter keeps.
	verdicts: Vec<Option<u16>>,
	/// The names each projection layer computes.
	computed_names: Vec<Vec<Strand>>,
	lookups: Vec<LookupSpec>,
	/// The statements held in an expression, one entry per site.
	subqueries: Vec<SubquerySpec>,
	/// The statements held in an expression that run as groups of the program, one entry per site.
	nested: Vec<NestedSpec>,
	/// The values defined outside the statement being lowered as a group of the program that its
	/// code reads as `$parent`: the row of the frame the statement is written in, which the
	/// instruction that runs the group binds as `$parent`.
	parent_rows: Vec<ValueId>,
	/// The query vector each KNN search of the program computes when the statement runs, by the
	/// value it is computed from: the searches over one value share one evaluation.
	knn_searches: Vec<(ValueId, std::sync::Arc<KnnQuery>)>,
	/// The calls of user-defined functions, one entry per site.
	functions: Vec<FunctionSpec>,
	/// The calls of code the host runs, one entry per site.
	externals: Vec<ExternalSpec>,
	/// The degree counts of `count(->edge)` calls, one entry per site.
	degrees: Vec<DegreeSpec>,
	/// How many call frames the program's calls of user-defined functions have taken.
	call_frames: u16,
	/// The block statement being lowered as a group of its own, `None` for the program's own code.
	group: Option<u16>,
	/// The first permission frame the code being lowered may share: the start of its group's
	/// range of the pool.
	permission_floor: usize,
	/// The owner of the probes of the statement being lowered, which runs under a strategy that
	/// keeps the rows it reads: the group that evaluates the statement, `None` for the program's
	/// own code. `None` outside such a statement, where no probe stands.
	probe_owner: Option<Option<u16>>,
	/// The values a block binds in a slot of the running state, which every lowerer of the program
	/// reads them from: the pipelines of a block's statements run over the same state.
	bound: FxHashMap<ValueId, BoundSlot>,
	/// The hops of the pipeline being lowered whose sources the physical plan has read in
	/// windows. A pipeline built beside the one in flight has none.
	frontier_hops: FrontierHops,
	/// The lookup that ends at each hop, by the graph-step op that states the hop.
	hop_lookups: FxHashMap<OpId, HopLookup>,
	/// The lookups the pipeline in flight has emitted.
	pipeline_lookups: Vec<u32>,
}

/// The lookup whose last hop a graph-step op states.
#[derive(Clone, Copy, Debug)]
pub(super) struct HopLookup {
	/// The index of the lookup in the program's lookups.
	pub(super) lookup: u32,
	/// How many hops the lookup has: it covers that many graph steps, ending at this one.
	pub(super) hops: usize,
}

/// What [`Emitter::open_group`] set aside, which [`Emitter::close_group`] restores.
#[derive(Clone, Copy, Debug)]
#[must_use]
pub(super) struct GroupMark {
	outer: Option<u16>,
	floor: usize,
	permissions: usize,
}

/// The pipeline in flight, held while another is built beside it.
///
/// [`Emitter::set_aside`] hands one out and [`Emitter::restore`] takes it back. The pools are
/// shared by both pipelines and are never part of it.
pub(super) struct Aside {
	code: Vec<Instr>,
	registers: u16,
	targets: Vec<Option<Label>>,
	frontier_hops: FrontierHops,
	pipeline_lookups: Vec<u32>,
}

/// The identity under which a constant is pooled: two constants share an entry only when their
/// keys are equal. A scalar's key holds its exact content, so an equal key is a hit without a
/// further comparison. Any other value is keyed by its `Debug` text and confirmed with `==`.
/// A scalar's key partitions constants as the `Debug` text confirmed by `==` does: `Int(1)` and
/// `Float(1.0)` differ, as do `Float(0.0)` and `Float(-0.0)` and decimals of different scale, and
/// every NaN, whatever its sign or payload, shares one key as `Value` equality treats them as one.
#[derive(Hash, PartialEq, Eq)]
enum ConstKey {
	None,
	Null,
	Bool(bool),
	Int(i64),
	Float(u64),
	Decimal([u8; 16]),
	BigDecimal(String),
	I8(i8),
	U8(u8),
	I16(i16),
	U16(u16),
	I32(i32),
	U32(u32),
	U64(u64),
	F16(u16),
	F32(u32),
	Strand(String),
	Text(String),
}

impl ConstKey {
	fn of(value: &Value) -> Self {
		match value {
			Value::None => Self::None,
			Value::Null => Self::Null,
			Value::Bool(b) => Self::Bool(*b),
			Value::Number(Number::Int(i)) => Self::Int(*i),
			Value::Number(Number::Float(f)) if f.is_nan() => Self::Float(f64::NAN.to_bits()),
			Value::Number(Number::Float(f)) => Self::Float(f.to_bits()),
			Value::Number(Number::Decimal(d)) => Self::Decimal(d.serialize()),
			Value::Number(Number::BigDecimal(d)) => Self::BigDecimal(d.to_string()),
			Value::Number(Number::I8(i)) => Self::I8(*i),
			Value::Number(Number::U8(u)) => Self::U8(*u),
			Value::Number(Number::I16(i)) => Self::I16(*i),
			Value::Number(Number::U16(u)) => Self::U16(*u),
			Value::Number(Number::I32(i)) => Self::I32(*i),
			Value::Number(Number::U32(u)) => Self::U32(*u),
			Value::Number(Number::U64(u)) => Self::U64(*u),
			Value::Number(Number::F16(f)) if f.is_nan() => {
				Self::F16(surrealdb_types::f16::NAN.to_bits())
			}
			Value::Number(Number::F16(f)) => Self::F16(f.to_bits()),
			Value::Number(Number::F32(f)) if f.is_nan() => Self::F32(f32::NAN.to_bits()),
			Value::Number(Number::F32(f)) => Self::F32(f.to_bits()),
			Value::String(s) => Self::Strand(s.as_str().to_owned()),
			other @ (Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => Self::Text(format!("{other:?}")),
		}
	}

	/// Whether the key holds the constant's whole content.
	fn is_exact(&self) -> bool {
		!matches!(self, Self::Text(_))
	}
}

#[allow(dead_code)] // Called by the lowering of chunks 4 to 8.
impl Emitter {
	/// An emitter with no code, no registers but [`ROW`] and empty pools.
	pub(super) fn new() -> Self {
		Self {
			code: Vec::new(),
			registers: ROW + 1,
			targets: Vec::new(),
			consts: Vec::new(),
			const_index: HashMap::new(),
			kinds: Vec::new(),
			calls: Vec::new(),
			matches: Vec::new(),
			methods: Vec::new(),
			params: Vec::new(),
			fields: Vec::new(),
			paths: Vec::new(),
			record_ids: Vec::new(),
			closure_values: Vec::new(),
			host_code: Vec::new(),
			fetches: Vec::new(),
			permissions: Vec::new(),
			verdicts: Vec::new(),
			computed_names: Vec::new(),
			lookups: Vec::new(),
			subqueries: Vec::new(),
			nested: Vec::new(),
			parent_rows: Vec::new(),
			knn_searches: Vec::new(),
			functions: Vec::new(),
			externals: Vec::new(),
			degrees: Vec::new(),
			call_frames: 0,
			group: None,
			permission_floor: 0,
			probe_owner: None,
			bound: FxHashMap::default(),
			frontier_hops: FrontierHops::default(),
			hop_lookups: FxHashMap::default(),
			pipeline_lookups: Vec::new(),
		}
	}

	/// Records that the value `value` is read from the slot `bound` names, by every lowerer of
	/// the program.
	pub(super) fn bind_slot(&mut self, value: ValueId, bound: BoundSlot) {
		self.bound.insert(value, bound);
	}

	/// The values the program's block binds in slots.
	pub(super) fn bound_slots(&self) -> &FxHashMap<ValueId, BoundSlot> {
		&self.bound
	}

	/// Starts the lowering of block statement `group` as a group of its own: the pipelines
	/// finished and the verdicts allocated until [`Emitter::close_group`] belong to it, and its
	/// permission frames are new entries of the pool, shared only among themselves.
	pub(super) fn open_group(&mut self, group: u16) -> GroupMark {
		let permissions = self.permissions.len();
		GroupMark {
			outer: self.group.replace(group),
			floor: std::mem::replace(&mut self.permission_floor, permissions),
			permissions,
		}
	}

	/// Ends the group `mark` opened and answers the range of the permission pool it filled, which
	/// is the group's [`GroupSpec::permissions`](super::super::program::GroupSpec). The code
	/// lowered after it belongs to the group, or the program's own code, that was open before.
	pub(super) fn close_group(&mut self, mark: GroupMark) -> Result<Range<u16>, IrError> {
		let index =
			|at: usize| u16::try_from(at).map_err(|_| IrError::Capacity("vm permission frames"));
		let range = index(mark.permissions)?..index(self.permissions.len())?;
		self.group = mark.outer;
		self.permission_floor = mark.floor;
		Ok(range)
	}

	/// The block statement being lowered as a group of its own, `None` for the program's own code.
	pub(super) fn group(&self) -> Option<u16> {
		self.group
	}

	/// Starts the lowering of a statement whose probes the code being lowered owns. Each probe
	/// the statement reads through keeps its rows until the next [`Emitter::open_group`].
	/// Answers the owner that [`Emitter::release_probes`] restores once the statement is lowered.
	pub(super) fn own_probes(&mut self) -> Option<Option<u16>> {
		self.probe_owner.replace(self.group)
	}

	/// Ends what [`Emitter::own_probes`] started, restoring the owner it answered.
	pub(super) fn release_probes(&mut self, outer: Option<Option<u16>>) {
		self.probe_owner = outer;
	}

	/// The owner of the probes of the statement being lowered: `None` outside a statement whose
	/// probes an evaluation owns.
	pub(super) fn probe_owner(&self) -> Option<Option<u16>> {
		self.probe_owner
	}

	/// The slot a nearest-neighbour search of the code being lowered publishes each row's distance
	/// in, which `vector::distance::knn()` of the same statement reads: one per statement of the
	/// program, `0` for the program's own code and the group's index plus one for a block
	/// statement.
	pub(super) fn knn_slot(&self) -> u16 {
		self.group.map_or(0, |group| group.saturating_add(1))
	}

	/// The query vector a KNN search computes from `value` when the statement runs: the one an
	/// earlier search of the program over `value` holds, else the one `make` builds, in the group
	/// the emitter lowers.
	pub(super) fn knn_search(
		&mut self,
		value: ValueId,
		make: impl FnOnce(&mut Self) -> Result<DeferredVector, IrError>,
	) -> Result<std::sync::Arc<KnnQuery>, IrError> {
		if let Some((_, query)) = self.knn_searches.iter().find(|(held, _)| *held == value) {
			return Ok(std::sync::Arc::clone(query));
		}
		let query = std::sync::Arc::new(KnnQuery {
			vector: make(self)?,
			group: self.group(),
			before_rows: false,
		});
		self.knn_searches.push((value, std::sync::Arc::clone(&query)));
		Ok(query)
	}

	/// Appends `instr` and answers the index it sits at.
	pub(super) fn emit(&mut self, instr: Instr) -> Label {
		let at = self.here();
		self.code.push(instr);
		at
	}

	/// The registers handed out so far, [`ROW`] included.
	pub(super) fn register_count(&self) -> usize {
		usize::from(self.registers)
	}

	/// The index the next instruction will sit at.
	pub(super) fn here(&self) -> Label {
		self.code.len() as Label
	}

	/// The instructions emitted so far, for a caller that scans the range it emitted.
	pub(super) fn code(&self) -> &[Instr] {
		&self.code
	}

	/// The instructions emitted so far, for a caller that rewrites one it emitted.
	///
	/// A rewrite replaces an instruction with one of the same position, or drops the tail after
	/// the last label that was bound: a label bound to an index past the code that remains would
	/// name another instruction.
	pub(super) fn code_mut(&mut self) -> &mut Vec<Instr> {
		&mut self.code
	}

	/// A register no earlier call handed out.
	pub(super) fn reg(&mut self) -> Result<Reg, IrError> {
		let reg = self.registers;
		self.registers = reg.checked_add(1).ok_or(IrError::Capacity("vm registers"))?;
		Ok(reg)
	}

	/// `count` consecutive registers no earlier call handed out, answering the first.
	///
	/// An allocation that would pass the range of a register leaves the emitter as it was.
	pub(super) fn regs(&mut self, count: usize) -> Result<Reg, IrError> {
		let first = self.registers;
		let count = Reg::try_from(count).map_err(|_| IrError::Capacity("vm registers"))?;
		self.registers = first.checked_add(count).ok_or(IrError::Capacity("vm registers"))?;
		Ok(first)
	}

	/// A label that is bound later, for a jump to place before its target.
	pub(super) fn label(&mut self) -> Result<Label, IrError> {
		let label =
			Label::try_from(self.targets.len()).map_err(|_| IrError::Capacity("vm labels"))?;
		self.targets.push(None);
		Ok(label)
	}

	/// Binds `label` to the index of the next instruction.
	///
	/// A label is bound once. Binding one this emitter did not hand out, or one already bound,
	/// answers [`IrError::Stale`].
	pub(super) fn bind(&mut self, label: Label) -> Result<(), IrError> {
		let at = self.here();
		match self.targets.get_mut(label as usize) {
			Some(target @ None) => {
				*target = Some(at);
				Ok(())
			}
			Some(Some(_)) => Err(IrError::Stale("ir_lower::emit: label bound twice")),
			None => Err(IrError::Stale("ir_lower::emit: unknown label")),
		}
	}

	/// The index of `value` in the constant pool, appended on its first appearance.
	pub(super) fn constant(&mut self, value: Value) -> Result<u32, IrError> {
		let next = |consts: &[Value]| {
			u32::try_from(consts.len()).map_err(|_| IrError::Capacity("vm constants"))
		};
		let index = match self.const_index.entry(ConstKey::of(&value)) {
			Entry::Occupied(held) => {
				let index = *held.get();
				if held.key().is_exact() || self.consts[index as usize] == value {
					return Ok(index);
				}
				next(&self.consts)?
			}
			Entry::Vacant(slot) => *slot.insert(next(&self.consts)?),
		};
		self.consts.push(value);
		Ok(index)
	}

	/// The constant pool filled so far.
	pub(super) fn constants(&self) -> &[Value] {
		&self.consts
	}

	/// The index of `kind` in the kind pool.
	pub(super) fn kind(&mut self, kind: Kind) -> Result<u32, IrError> {
		intern(&mut self.kinds, kind, "vm kinds")
	}

	/// The index of a new entry for the call site `call`. Each call site has an entry of its own.
	pub(super) fn call(&mut self, call: CallSpec) -> Result<u32, IrError> {
		push(&mut self.calls, call, "vm calls")
	}

	/// The index of a new entry for the `MATCHES` operator `spec` describes. Each operator site
	/// has an entry of its own.
	pub(super) fn matches(&mut self, spec: MatchesSpec) -> Result<u32, IrError> {
		push(&mut self.matches, spec, "vm matches")
	}

	/// The index of a new entry for the method call site `method`.
	pub(super) fn method(&mut self, method: MethodSpec) -> Result<u32, IrError> {
		push(&mut self.methods, method, "vm methods")
	}

	/// The index of a new entry for the closure literal `spec` makes values of. Each literal site
	/// has an entry of its own.
	pub(super) fn closure_value(&mut self, spec: ClosureValueSpec) -> Result<u32, IrError> {
		push(&mut self.closure_values, spec, "vm closure values")
	}

	/// The index of `param` in the parameter pool.
	pub(super) fn param(&mut self, param: ParamSpec) -> Result<u32, IrError> {
		intern(&mut self.params, param, "vm params")
	}

	/// The index of the field named `name` in the field pool.
	pub(super) fn field(&mut self, name: &str) -> Result<u32, IrError> {
		if let Some(index) = self.fields.iter().position(|held| held == name) {
			return Ok(index as u32);
		}
		push(&mut self.fields, name.to_owned(), "vm fields")
	}

	/// The index of `path` in the path pool.
	pub(super) fn path(&mut self, path: FieldPath) -> Result<u32, IrError> {
		intern(&mut self.paths, path, "vm paths")
	}

	/// How many call sites and method call sites the program holds.
	///
	/// A site lets the host evaluate code, so a block name bound before a new site is added can
	/// be read through the frame by that code.
	pub(super) fn call_sites(&self) -> (usize, usize) {
		(self.calls.len(), self.methods.len())
	}

	/// The name of the parameter at `index` of the parameter pool.
	pub(super) fn param_name(&self, index: u32) -> Option<&str> {
		self.params.get(index as usize).map(|param| param.name.as_str())
	}

	/// Sets whether the `BlockInstr::Bind` at instruction index `at` also binds its name in the
	/// frame.
	///
	/// Answers [`IrError::Stale`] when the instruction at `at` is not a bind.
	pub(super) fn set_bind_frame(&mut self, at: Label, frame: bool) -> Result<(), IrError> {
		match self.code.get_mut(at as usize) {
			Some(Instr::Block(BlockInstr::Bind {
				frame: bound,
				..
			})) => {
				*bound = frame;
				Ok(())
			}
			_ => Err(IrError::Stale("ir_lower::emit: a bind that is not one")),
		}
	}

	/// The index of a new entry for the record id `record`, whose key holds computed parts. Each
	/// record id site has an entry of its own.
	pub(super) fn record_id(&mut self, record: RecordIdSpec) -> Result<u32, IrError> {
		push(&mut self.record_ids, record, "vm record ids")
	}

	/// The index of a new entry for the statement `spec` runs where it is held in an expression.
	/// Each site has an entry of its own.
	pub(super) fn subquery(&mut self, spec: SubquerySpec) -> Result<u32, IrError> {
		push(&mut self.subqueries, spec, "vm subqueries")
	}

	/// The index of a new entry for the statement `spec` runs as a group of the program where it
	/// is held in an expression. Each site has an entry of its own.
	pub(super) fn nested(&mut self, spec: NestedSpec) -> Result<u32, IrError> {
		push(&mut self.nested, spec, "vm nested statements")
	}

	/// Starts the lowering of a statement as a group of the program in which every read of one of
	/// `rows` is a read of `$parent`. Answers the values [`Emitter::restore_parent_rows`] restores
	/// once the statement is lowered.
	pub(super) fn read_as_parent(&mut self, rows: Vec<ValueId>) -> Vec<ValueId> {
		std::mem::replace(&mut self.parent_rows, rows)
	}

	/// Ends what [`Emitter::read_as_parent`] started, restoring the values it answered.
	pub(super) fn restore_parent_rows(&mut self, outer: Vec<ValueId>) {
		self.parent_rows = outer;
	}

	/// Whether the code being lowered reads `value` as `$parent`.
	pub(super) fn is_parent_row(&self, value: ValueId) -> bool {
		self.parent_rows.contains(&value)
	}

	/// The index of a new entry for the call of a user-defined function `spec` describes. Each
	/// site has an entry of its own.
	pub(super) fn function(&mut self, spec: FunctionSpec) -> Result<u32, IrError> {
		push(&mut self.functions, spec, "vm functions")
	}

	/// The index of a new entry for the call of code the host runs that `spec` describes. Each
	/// site has an entry of its own.
	pub(super) fn external(&mut self, spec: ExternalSpec) -> Result<u32, IrError> {
		push(&mut self.externals, spec, "vm external calls")
	}

	/// A call frame no other call of the program takes: the frame a call of a user-defined
	/// function enters, numbered from [`CALL_FRAME`] up, which names no permission frame.
	pub(super) fn call_frame(&mut self) -> Result<u16, IrError> {
		let frame =
			CALL_FRAME.checked_add(self.call_frames).ok_or(IrError::Capacity("vm call frames"))?;
		self.call_frames += 1;
		Ok(frame)
	}

	/// States the hops of the pipeline about to be lowered whose sources the physical plan reads
	/// in windows. [`Emitter::end_hops`] ends the statement.
	pub(super) fn set_frontier_hops(&mut self, hops: FrontierHops) {
		self.frontier_hops = hops;
	}

	/// Whether the plan reads the sources of the hop the graph-step op `hop` states in windows.
	pub(super) fn serves_hop(&self, hop: OpId) -> bool {
		self.frontier_hops.serves(hop)
	}

	/// Ends what [`Emitter::set_frontier_hops`] stated and forgets the lookups the pipeline in
	/// flight emitted.
	pub(super) fn end_hops(&mut self) {
		self.frontier_hops = FrontierHops::default();
		self.pipeline_lookups.clear();
	}

	/// Records that the lookup `answer.lookup` ends at the hop the graph-step op `hop` states.
	pub(super) fn note_hop_lookup(&mut self, hop: OpId, answer: HopLookup) {
		self.hop_lookups.insert(hop, answer);
	}

	/// Forgets the lookup that ends at the hop the graph-step op `hop` states, so no window of the
	/// pipeline's rows reads ahead for it: the lookup runs on one path of the code alone.
	pub(super) fn forget_hop_lookup(&mut self, hop: OpId) {
		self.hop_lookups.remove(&hop);
	}

	/// The lookup that ends at the hop the graph-step op `hop` states, when one does.
	pub(super) fn hop_lookup(&self, hop: OpId) -> Option<HopLookup> {
		self.hop_lookups.get(&hop).copied()
	}

	/// Whether the pipeline in flight emitted the lookup `lookup`.
	pub(super) fn emitted_lookup(&self, lookup: u32) -> bool {
		self.pipeline_lookups.contains(&lookup)
	}

	/// The index of the graph lookup `spec`, appended to the program's lookups. The pipeline in
	/// flight runs it.
	pub(super) fn lookup(&mut self, spec: LookupSpec) -> Result<u32, IrError> {
		let index = push(&mut self.lookups, spec, "vm lookups")?;
		self.pipeline_lookups.push(index);
		Ok(index)
	}

	/// The index of the degree count `spec`, appended to the program's degree counts.
	pub(super) fn degree(&mut self, spec: DegreeSpec) -> Result<u32, IrError> {
		push(&mut self.degrees, spec, "vm degree counts")
	}

	/// The index of `pipeline` in the host pipelines of the program: code the host runs over a
	/// value of its own, outside the body that names it. A host pipeline belongs to no group: the
	/// code that names it runs it.
	pub(super) fn host_code(&mut self, mut pipeline: Pipeline) -> Result<u16, IrError> {
		let index = u16::try_from(self.host_code.len())
			.map_err(|_| IrError::Capacity("vm host pipelines"))?;
		pipeline.group = None;
		self.host_code.push(pipeline);
		Ok(index)
	}

	/// The index of a new entry for the fetch of `paths`, each the steps one `FETCH` path walks,
	/// which reads its links in windows where `windows`.
	pub(super) fn fetch(
		&mut self,
		paths: Vec<Vec<FetchStepSpec>>,
		windows: bool,
	) -> Result<u16, IrError> {
		let index =
			u16::try_from(self.fetches.len()).map_err(|_| IrError::Capacity("vm fetches"))?;
		self.fetches.push(FetchPlan {
			paths,
			windows,
		});
		Ok(index)
	}

	/// The index of the permission frame `spec`: the entry of an equal frame of the open group (or
	/// of the program's own code), or a new one.
	///
	/// A frame index names a permission frame only below [`CALL_FRAME`], so the pool answers
	/// [`IrError::Capacity`] where an index would reach it.
	pub(super) fn permission_frame(&mut self, spec: PermissionSpec) -> Result<u16, IrError> {
		let shared = self.permissions.get(self.permission_floor..).unwrap_or(&[]);
		let index = match shared.iter().position(|held| *held == spec) {
			Some(index) => self.permission_floor + index,
			None => {
				self.permissions.push(spec);
				self.permissions.len() - 1
			}
		};
		u16::try_from(index)
			.ok()
			.filter(|index| *index < CALL_FRAME)
			.ok_or(IrError::Capacity("vm permission frames"))
	}

	/// The permission frame at `index` of the pool, `None` past its end.
	pub(super) fn permission_at(&self, index: u16) -> Option<&PermissionSpec> {
		self.permissions.get(usize::from(index))
	}

	/// The index of a new verdict cell, which a filter fills the first time its once-judged
	/// conjuncts run in a run of the statement; the start of the open group forgets it.
	pub(super) fn verdict(&mut self) -> Result<u16, IrError> {
		let index =
			u16::try_from(self.verdicts.len()).map_err(|_| IrError::Capacity("vm verdicts"))?;
		self.verdicts.push(self.group);
		Ok(index)
	}

	/// The index of a new entry for the names a projection layer computes.
	pub(super) fn computed_names(&mut self, names: Vec<Strand>) -> Result<u32, IrError> {
		push(&mut self.computed_names, names, "vm computed names")
	}

	/// Sets the pipeline in flight aside, leaving the emitter as a fresh one over the same pools,
	/// to build a pipeline beside it. [`Emitter::finish_pipeline`] ends the pipeline built since,
	/// and [`Emitter::restore`] returns to the one set aside.
	pub(super) fn set_aside(&mut self) -> Aside {
		Aside {
			code: std::mem::take(&mut self.code),
			registers: std::mem::replace(&mut self.registers, ROW + 1),
			targets: std::mem::take(&mut self.targets),
			frontier_hops: std::mem::take(&mut self.frontier_hops),
			pipeline_lookups: std::mem::take(&mut self.pipeline_lookups),
		}
	}

	/// Returns to the pipeline `aside` holds, dropping whatever was emitted since and not finished.
	pub(super) fn restore(&mut self, aside: Aside) {
		self.code = aside.code;
		self.registers = aside.registers;
		self.targets = aside.targets;
		self.frontier_hops = aside.frontier_hops;
		self.pipeline_lookups = aside.pipeline_lookups;
	}

	/// Ends the pipeline whose code was emitted since the last one, reading rows from `source`.
	///
	/// Every label in the code becomes the index it was bound to; a label that was never bound
	/// answers [`IrError::Stale`]. The pipeline belongs to the open group, runs serially and holds
	/// no limit or enforcement: those are set from the physical plan by the caller that has them.
	/// Its `registers` is the count handed out, [`ROW`] included.
	///
	/// `in_place` and `stored_rows` follow from the code alone: a body runs on the stored bytes of
	/// a record, or of a sorted row, only when its source reads storage (or is sorted) and the body
	/// holds no backward jump and no instruction the interpreter does not answer from the row and
	/// the program alone. The instructions that qualify are those of `runs_on_the_row`; any other
	/// leaves both `false`, which is always correct. A body that takes the row whole through the
	/// [`ROW`] register (`$this` or `$self` as a value) leaves both `false` as well: a reader that
	/// reads a record where it lies holds no row in that register, so the body runs over the
	/// record decoded whole.
	///
	/// The emitter is left with no code, the registers of a fresh emitter and no labels, ready for
	/// the next pipeline of the same program; its pools are kept.
	pub(super) fn finish_pipeline(&mut self, source: Source) -> Result<Pipeline, IrError> {
		let mut code = std::mem::take(&mut self.code);
		let targets = std::mem::take(&mut self.targets);
		let registers = std::mem::replace(&mut self.registers, ROW + 1);
		for instr in &mut code {
			for label in instr.labels_mut() {
				*label = targets
					.get(*label as usize)
					.copied()
					.flatten()
					.ok_or(IrError::Stale("ir_lower::emit: label never bound"))?;
			}
		}
		let host_free =
			!jumps_back(&code) && code.iter().all(runs_on_the_row) && !reads_row_register(&code);
		let in_place = source.reads_storage() && host_free;
		let stored_rows = matches!(source, Source::Sorted { .. }) && host_free;
		let reads_row = in_place && reads_row(&code);
		Ok(Pipeline {
			source,
			code,
			registers: registers as usize,
			in_place,
			stored_rows,
			reads_row,
			enforce: None,
			limits: Vec::new(),
			parallel: Parallel::Serial,
			group: self.group,
			deref_windows: false,
			frontier_rows: None,
			read_ahead: false,
			rows_ahead: None,
			unit: None,
		})
	}

	/// Drops the pipeline in flight, which began with no code: its instructions, labels, registers
	/// and lookups. The pools keep every entry the pipeline added, and nothing refers to one it
	/// added.
	pub(super) fn abandon_pipeline(&mut self) {
		self.code.clear();
		self.targets.clear();
		self.registers = ROW + 1;
		self.frontier_hops = FrontierHops::default();
		self.pipeline_lookups.clear();
	}

	/// The program of `pipelines`, over the pools this emitter filled.
	///
	/// Every field of [`Program`] is set.
	/// The fields the pools do not cover are empty or `None`, and the flags are their neutral
	/// value: `scalar` and `rows` are `false`, `keeps_row_order` is `false` and `required_context`
	/// is [`ContextLevel::Root`]. A caller that emits a builtin which keeps row order, or a program
	/// whose value is its one row, sets that field on the result.
	///
	/// The program's access mode is `access_mode` or, where a closure literal's body writes, a
	/// write: a program that makes a closure that writes runs in a transaction that may write.
	///
	/// A constant pool that holds a closure answers [`IrError::Invariant`]: a closure value is made
	/// by [`Instr::MakeClosure`] when the program runs, the binder binds a closure literal as
	/// `expr.closure_create`, and the plan-time folder folds no closure, so no literal holds one.
	///
	/// A program that writes holds a pipeline of its own planned as morsels only where the pipeline
	/// holds the cut of a write ([`write::guard_writing_morsels`](super::write)).
	pub(super) fn finish_program(
		self,
		pipelines: Vec<Pipeline>,
		access_mode: AccessMode,
	) -> Result<Program, IrError> {
		let program = self.assemble_program(pipelines, access_mode)?;
		super::write::guard_writing_morsels(&program, &|at| {
			program.pipelines.get(at).is_some_and(super::write::holds_cut)
		})?;
		Ok(program)
	}

	/// [`Self::finish_program`] without the check of the pipelines' placement.
	pub(super) fn assemble_program(
		self,
		pipelines: Vec<Pipeline>,
		access_mode: AccessMode,
	) -> Result<Program, IrError> {
		let access_mode = self
			.closure_values
			.iter()
			.map(|closure| closure.access_mode)
			.fold(access_mode, AccessMode::combine);
		let consts = super::super::plain::ValueData::new(self.consts, "the constant pool")
			.map_err(|_| IrError::Invariant {
				what: "the constant pool holds a closure value, which no program holds",
			})?;
		Ok(Program {
			pipelines,
			consts,
			fields: self.fields.iter().map(|name| FieldName::new(name)).collect(),
			paths: self.paths,
			limits: Vec::new(),
			sorts: Vec::new(),
			splits: Vec::new(),
			joins: Vec::new(),
			distincts: Vec::new(),
			verdicts: self.verdicts,
			fetches: self.fetches,
			aggregates: Vec::new(),
			permissions: self.permissions,
			computed_names: self.computed_names,
			shaped_seed: None,
			table: None,
			access_mode,
			write: None,
			scalar: false,
			rows: false,
			groups: Vec::new(),
			analyses: Vec::new(),
			served: Vec::new(),
			kinds: self.kinds,
			params: self.params,
			calls: self.calls,
			matches: self.matches,
			externals: self.externals,
			degrees: self.degrees,
			methods: self.methods,
			record_ids: self.record_ids,
			subqueries: self.subqueries,
			nested: self.nested,
			probes: Vec::new(),
			lookups: self.lookups,
			functions: self.functions,
			closures: Vec::new(),
			closure_values: self.closure_values,
			host_code: self.host_code,
			reservations: Vec::new(),
			implicit_tables: Vec::new(),
			units: Vec::new().into(),
			reach: None,
			dispatch: None,
			unit: None,
			tail: None,
			events: None,
			required_context: ContextLevel::Root,
			keeps_row_order: false,
			request: None,
		})
	}
}

/// The index of `entry` in `pool`, appended when the pool holds none equal to it.
fn intern<T: PartialEq>(pool: &mut Vec<T>, entry: T, what: &'static str) -> Result<u32, IrError> {
	match pool.iter().position(|held| *held == entry) {
		Some(index) => Ok(index as u32),
		None => push(pool, entry, what),
	}
}

/// The index `entry` takes appended to `pool`.
fn push<T>(pool: &mut Vec<T>, entry: T, what: &'static str) -> Result<u32, IrError> {
	let index = u32::try_from(pool.len()).map_err(|_| IrError::Capacity(what))?;
	pool.push(entry);
	Ok(index)
}

/// Whether any instruction of `code` may continue at or before itself: a loop, at whose backward
/// jump the interpreter spends fuel and may yield.
fn jumps_back(code: &[Instr]) -> bool {
	code.iter().enumerate().any(|(at, instr)| {
		instr.clone().labels_mut().into_iter().any(|label| *label as usize <= at)
	})
}

/// Whether the interpreter answers `instr` from the row and the program alone, so that a body of
/// such instructions runs on the stored bytes of a record without a hand-off to the host.
///
/// An instruction that yields to the host (a parameter read, a call, a method), reads a record,
/// writes or defines is not one, and neither is any instruction this list does not name. The
/// paging instructions ([`Instr::Skip`], [`Instr::Take`], [`Instr::JumpIfLimitReached`]) are: they
/// read and count the statement's paging state alone, never the row. A brute-force ranking
/// ([`Instr::KnnRank`]) is one where its query vector is in hand before any row is read
/// ([`KnnRank::before_rows`](crate::exec::vm::program::KnnRank)): it then reads the row's vector
/// and computes a distance, and hands the host nothing.
fn runs_on_the_row(instr: &Instr) -> bool {
	if let Instr::KnnRank {
		rank,
		..
	} = instr
	{
		return rank.before_rows;
	}
	// A vector measure's check and query are settled as the run starts, and it measures a row,
	// or calls its builtin over the row's field, in the interpreter.
	if let Instr::Measure {
		..
	} = instr
	{
		return true;
	}
	matches!(
		instr,
		Instr::Load { .. }
			| Instr::Compare { .. }
			| Instr::Arith { .. }
			| Instr::Unary { .. }
			| Instr::CompareFieldInt { .. }
			| Instr::JumpIfFieldInt { .. }
			| Instr::FilterFieldInt { .. }
			| Instr::JumpIfCompare { .. }
			| Instr::FilterCompare { .. }
			| Instr::Filter { .. }
			| Instr::Jump { .. }
			| Instr::JumpIf { .. }
			| Instr::JumpIfNullish { .. }
			| Instr::JumpIfNone { .. }
			| Instr::NewArray { .. }
			| Instr::ArrayPush { .. }
			| Instr::NewSet { .. }
			| Instr::CollectPush { .. }
			| Instr::NewObject { .. }
			| Instr::BuildObject { .. }
			| Instr::SetField { .. }
			| Instr::Skip { .. }
			| Instr::Take { .. }
			| Instr::JumpIfLimitReached { .. }
			| Instr::SortPush { .. }
			| Instr::GroupKey { .. }
			| Instr::GroupSelect { .. }
			| Instr::GroupFirstNeeded { .. }
			| Instr::GroupFirstSet { .. }
			| Instr::AccUpdate { .. }
			| Instr::Emit { .. }
			| Instr::EmitEnd { .. }
			| Instr::EndRow
			| Instr::StopPipeline
	)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::*;
	use crate::exec::vm::program::Operand;
	use crate::kvs::Direction;

	#[test]
	fn a_closure_in_the_constant_pool_is_an_invariant() {
		let mut em = Emitter::new();
		let closure = crate::val::Value::Closure(Box::new(crate::val::Closure::Expr {
			args: Vec::new(),
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::None),
			captures: Default::default(),
		}));
		em.constant(closure).expect("the constant is pooled");
		assert!(matches!(
			em.finish_program(Vec::new(), AccessMode::ReadOnly),
			Err(IrError::Invariant { .. })
		));
	}

	fn scan() -> Source {
		Source::Scan {
			table: TableName::from("t"),
			direction: Direction::Forward,
			topk: None,
			prefilter: None,
			range: None,
			probes: None,
		}
	}

	#[test]
	fn registers_start_after_the_row_and_overflow_is_capacity() {
		let mut em = Emitter::new();
		assert_eq!(em.reg().unwrap(), ROW + 1);
		assert_eq!(em.reg().unwrap(), ROW + 2);
		em.registers = u16::MAX;
		assert!(matches!(em.reg(), Err(IrError::Capacity("vm registers"))));
		// The failed allocation leaves the emitter as it was.
		assert_eq!(em.registers, u16::MAX);
	}

	#[test]
	fn forward_labels_are_rewritten_to_the_index_they_bind() {
		let mut em = Emitter::new();
		let end = em.label().unwrap();
		em.emit(Instr::Jump {
			target: end,
		});
		em.emit(Instr::Load {
			dst: 1,
			src: Operand::Const(0),
		});
		em.bind(end).unwrap();
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).unwrap();
		assert_eq!(
			pipeline.code[0],
			Instr::Jump {
				target: 2
			}
		);
		assert!(!pipeline.in_place);
	}

	#[test]
	fn an_unbound_or_doubly_bound_label_is_stale() {
		let mut em = Emitter::new();
		let label = em.label().unwrap();
		em.emit(Instr::Jump {
			target: label,
		});
		assert!(matches!(em.finish_pipeline(Source::Once), Err(IrError::Stale(_))));
		let mut em = Emitter::new();
		let label = em.label().unwrap();
		em.bind(label).unwrap();
		assert!(matches!(em.bind(label), Err(IrError::Stale(_))));
		assert!(matches!(em.bind(9), Err(IrError::Stale(_))));
	}

	#[test]
	fn constants_are_interned_by_representation() {
		let mut em = Emitter::new();
		let int = em.constant(Value::from(1_i64)).unwrap();
		let float = em.constant(Value::from(1.0_f64)).unwrap();
		assert_ne!(int, float);
		assert_eq!(em.constant(Value::from(1_i64)).unwrap(), int);
		assert_eq!(em.constant(Value::from(1.0_f64)).unwrap(), float);
		let zero = em.constant(Value::from(0.0_f64)).unwrap();
		let negative_zero = em.constant(Value::from(-0.0_f64)).unwrap();
		assert_ne!(zero, negative_zero);
	}

	#[test]
	fn scalar_constants_keep_their_dedup_classes() {
		use rust_decimal::Decimal;
		let mut em = Emitter::new();
		let int = em.constant(Value::from(1_i64)).unwrap();
		let float = em.constant(Value::from(1.0_f64)).unwrap();
		let dec = em.constant(Value::from(Decimal::from(1))).unwrap();
		let zero = em.constant(Value::from(0.0_f64)).unwrap();
		let neg_zero = em.constant(Value::from(-0.0_f64)).unwrap();
		let nan = em.constant(Value::from(f64::NAN)).unwrap();
		let all = [int, float, dec, zero, neg_zero, nan];
		for (i, a) in all.iter().enumerate() {
			for b in &all[i + 1..] {
				assert_ne!(a, b);
			}
		}
		assert_eq!(em.constant(Value::from(1_i64)).unwrap(), int);
		assert_eq!(em.constant(Value::from(1.0_f64)).unwrap(), float);
		assert_eq!(em.constant(Value::from(Decimal::from(1))).unwrap(), dec);
		assert_eq!(em.constant(Value::from(-0.0_f64)).unwrap(), neg_zero);
		// Every NaN, whatever its sign or payload, shares the first NaN's entry.
		assert_eq!(em.constant(Value::from(f64::NAN)).unwrap(), nan);
		assert_eq!(em.constant(Value::from(-f64::NAN)).unwrap(), nan);
		assert_eq!(em.constant(Value::from(f64::from_bits(0x7ff8_0000_0000_0001))).unwrap(), nan);
		// Decimals of different scale and strings keep their own classes.
		let scaled = em.constant(Value::from(Decimal::new(10, 1))).unwrap();
		assert_ne!(scaled, dec);
		let a = em.constant(Value::from("a")).unwrap();
		assert_eq!(em.constant(Value::from("a")).unwrap(), a);
		assert_ne!(em.constant(Value::from("b")).unwrap(), a);
		assert_eq!(em.constant(Value::None).unwrap(), em.constant(Value::None).unwrap());
		assert_ne!(em.constant(Value::Null).unwrap(), em.constant(Value::None).unwrap());
		assert_eq!(
			em.constant(Value::Bool(true)).unwrap(),
			em.constant(Value::Bool(true)).unwrap()
		);
	}

	#[test]
	fn value_pools_hold_each_entry_once_and_call_sites_one_each() {
		let mut em = Emitter::new();
		assert_eq!(em.field("a").unwrap(), 0);
		assert_eq!(em.field("b").unwrap(), 1);
		assert_eq!(em.field("a").unwrap(), 0);
		assert_eq!(em.kind(Kind::Int).unwrap(), 0);
		assert_eq!(em.kind(Kind::Int).unwrap(), 0);
		assert_eq!(em.path(FieldPath::field("a")).unwrap(), 0);
		assert_eq!(em.path(FieldPath::field("a")).unwrap(), 0);
		let call = CallSpec {
			name: "math::abs".to_owned(),
			plan_depth: 0,
			index: None,
			closure_args: Vec::new(),
		};
		assert_eq!(em.call(call.clone()).unwrap(), 0);
		assert_eq!(em.call(call).unwrap(), 1);
	}

	#[test]
	fn in_place_follows_the_emitted_code() {
		let filter = |em: &mut Emitter| {
			let end = em.label().unwrap();
			em.emit(Instr::JumpIf {
				cond: Operand::Field(0),
				truthy: false,
				target: end,
			});
			em.emit(Instr::Emit {
				src: ROW,
			});
			em.bind(end).unwrap();
			em.emit(Instr::EndRow);
		};
		// A storage source and a body the interpreter answers from the row: in place.
		let mut em = Emitter::new();
		filter(&mut em);
		assert!(em.finish_pipeline(scan()).unwrap().in_place);
		// The same body over a source that is not storage.
		let mut em = Emitter::new();
		filter(&mut em);
		assert!(!em.finish_pipeline(Source::Once).unwrap().in_place);
		// A body that hands back to the host.
		let mut em = Emitter::new();
		let dst = em.reg().unwrap();
		em.emit(Instr::LoadParam {
			param: 0,
			dst,
			frame: None,
		});
		em.emit(Instr::EndRow);
		assert!(!em.finish_pipeline(scan()).unwrap().in_place);
		// A body that jumps back.
		let mut em = Emitter::new();
		let top = em.label().unwrap();
		em.bind(top).unwrap();
		em.emit(Instr::Jump {
			target: top,
		});
		assert!(!em.finish_pipeline(scan()).unwrap().in_place);
	}

	/// `reads_row` is a function of the emitted code: a body that reads fields of its row and takes
	/// it whole nowhere reads it in place, and any other body is run over the decoded record.
	#[test]
	fn reads_row_follows_the_emitted_code() {
		let reads = |code: Vec<Instr>| {
			let mut em = Emitter::new();
			for instr in code {
				em.emit(instr);
			}
			let pipeline = em.finish_pipeline(scan()).unwrap();
			(pipeline.in_place, pipeline.reads_row)
		};
		let field = Operand::Field(0);
		// A filter over a field, then the row emitted pending: the field is read, the row is not
		// taken whole.
		assert_eq!(
			reads(vec![
				Instr::FilterFieldInt {
					field: 0,
					op: crate::exec::vm::program::IntCmp::Eq,
					lit: 1,
					reversed: false,
					fallback: crate::exec::vm::program::CompareOp::Equal,
					reject: true,
				},
				Instr::Emit {
					src: ROW,
				},
				Instr::EndRow,
			]),
			(true, true)
		);
		assert_eq!(
			reads(vec![
				Instr::Compare {
					op: crate::exec::vm::program::CompareOp::Equal,
					left: field,
					right: Operand::Const(0),
					dst: 1,
				},
				Instr::EmitEnd {
					src: Operand::Reg(1),
				},
			]),
			(true, true)
		);
		// The row emitted and nothing read of it.
		assert_eq!(
			reads(vec![
				Instr::Emit {
					src: ROW,
				},
				Instr::EndRow,
			]),
			(true, false)
		);
		// A read of the row through its register is a read of it whole, which no reader of a
		// record where it lies answers: the body runs over the decoded record.
		assert_eq!(
			reads(vec![
				Instr::Compare {
					op: crate::exec::vm::program::CompareOp::Equal,
					left: field,
					right: Operand::Reg(ROW),
					dst: 1,
				},
				Instr::EndRow,
			]),
			(false, false)
		);
		// An instruction that hands back to the host is not in place, and reads no row in place.
		assert_eq!(
			reads(vec![
				Instr::LoadRow {
					dst: 1,
				},
				Instr::Compare {
					op: crate::exec::vm::program::CompareOp::Equal,
					left: field,
					right: Operand::Const(0),
					dst: 2,
				},
				Instr::EndRow,
			]),
			(false, false)
		);
		// A source that is not storage is not in place.
		let mut em = Emitter::new();
		em.emit(Instr::Load {
			dst: 1,
			src: field,
		});
		em.emit(Instr::EndRow);
		assert!(!em.finish_pipeline(Source::Once).unwrap().reads_row);
	}

	#[test]
	fn finish_program_keeps_the_pools_and_neutral_flags() {
		let mut em = Emitter::new();
		let value = em.constant(Value::from(7_i64)).unwrap();
		let dst = em.reg().unwrap();
		em.emit(Instr::Load {
			dst,
			src: Operand::Const(value),
		});
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).unwrap();
		assert_eq!(pipeline.registers, 2);
		let field = em.field("x").unwrap();
		assert_eq!(field, 0);
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).unwrap();
		assert_eq!(program.pipelines.len(), 1);
		assert_eq!(program.consts.len(), 1);
		assert_eq!(program.fields.len(), 1);
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
		assert!(!program.scalar && !program.rows && !program.keeps_row_order);
		assert_eq!(program.required_context, ContextLevel::Root);
		assert!(program.groups.is_empty() && program.units.is_empty());
	}
}
