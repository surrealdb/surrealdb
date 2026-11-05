//! The compile driver: plans and lowers every unit a program needs, before the program runs.
//!
//! [`ir_lower`] lowers one container and reports the units it needs as requests
//! ([`UnitRequest`]); it plans nothing. The [`Compiler`] lowers the container it is given, asks
//! its [`UnitPlanner`] for the container each request names, lowers that into the slot the
//! lowering reserved for it, and continues until no lowering has a request left. The program it
//! answers is closed: the VM compiles nothing while it runs, except what a fault on a closure
//! value or a served statement asks of the driver again.
//!
//! # The planning callback
//!
//! The planner answers a request with a [`Resolved`]: the container of a unit and the signature
//! its head names, the statement a block serves when it reaches it, or the reach of a table unit
//! the compile cache holds. It is handed the container that made the request, whose facts it
//! reads for what the unit inherits (whether an enclosing statement writes) and which it extracts
//! a region from for a request that names one, and the catalog overlay that container was planned
//! over: a unit reached from a statement planned over a block's speculative schema is planned
//! over the same overlay unless the planner answers another, which then governs what that unit
//! requests in turn. The persistent compile cache belongs to the planner: the driver keeps no
//! cache of its own and keys nothing by catalog version.
//!
//! # What a subquery unit gives its requester
//!
//! A statement in scalar position is a unit of its own, and the program that evaluates it takes
//! three things from it that only its lowering knows: the unit's access mode, the context level
//! it requires and whether it keeps row order. The lowering of the requester states none of them
//! for a unit it has not seen, so the driver, once every unit is lowered, writes them into the
//! requester's [`SubquerySpec`](super::program::SubquerySpec) for the unit and widens the
//! requester's own access mode, context level and row-order flag by them. The units fold into
//! their requesters from the last lowered to the first, so what a unit takes from the units it
//! evaluates is in it before it folds into its own requester.
//!
//! The body of a closure literal and the body of a recursion are units of their requester in the
//! same way, and give it their access mode alone: the driver writes it into the requester's
//! [`ClosureValueSpec`](super::program::ClosureValueSpec) for a closure body, and widens the
//! requester's own access mode by it. A unit that writes also makes every statement the
//! requester runs in place ([`NestedSpec`](super::program::NestedSpec)) one that does not only
//! read, since such a statement may evaluate the unit.
//!
//! A call of a user-defined function takes two of these from its body unit: the body's access
//! mode is the call's `body_access_mode`, and the program that calls it writes and keeps row order
//! where the body does. A body unit is shared by every program of the compile that calls the
//! function, and bodies may call each other, so the driver writes these into every call of the
//! compile that names the unit, repeating until nothing widens.
//!
//! An error the planner answers reaches the caller unchanged, so an unknown catalog answer
//! (`NotCoveredKind::LateBind`) drives the late-bound retry of the caller ([`binds_late`]), and a
//! refusal keeps its kind and text. A compile that fails leaves the compiler as it found it.
//!
//! # Units extracted from their requester
//!
//! A request that names a region of the requesting container ([`region_unit`]) is answered by
//! copying the region out as a container of its own (`Ir::copy_region`), already physical. A
//! subquery is the statement region, run rowless; the body of a recursion is the loop's body
//! region, run over its element as the row. The literals the region reads from outside it are
//! copied in. Every other value defined outside it is an input of the unit: a trailing argument of
//! its top block, which the requester hands over on the instruction that runs the unit, in the
//! same order ([`Ir::region_inputs`](crate::exec::ir::core::ir::Ir)), and which the unit is lowered
//! to read ([`Planned::unit_inputs`]).
//!
//! The body of a closure literal ([`closure_unit`]) is copied out the same way, from the
//! `expr.closure_create` of the requesting container whose literal has the digest the request
//! names. The planner answers it with what the unit's head names ([`ClosureTemplate`]): the
//! literal, the names it captures and whether it captures the row, from the op, and the catalog
//! stamps and `VERSION` stamp of the transaction it plans in, which a run compares before it takes
//! the unit for a value. The body is lowered for the frame of the container that made the literal,
//! where its values are made and invoked.
//!
//! # The frame a unit is lowered for
//!
//! A unit is lowered for the frame and re-entry depth the planner answers for it ([`UnitScope`]),
//! and otherwise for those of the container that asked for it, as it inherits that container's
//! overlay: the body of a user-defined function is bound for the caller's principal capped by the
//! definition's `AUTH LIMIT`, one re-entry deeper, and its head names that frame, which the run
//! admits the unit against; a statement copied out of that body runs inside it and is lowered for
//! the same frame. A statement copied out of a clause that runs under a definition's `AUTH LIMIT`
//! is lowered for the principal that limit caps ([`region_limit`]). The root is lowered for the
//! compile's. The compile's frame and depth are restored whether a lowering succeeds or not.
//!
//! The planner is handed the requests the requesting container descends from ([`Ask::path`]), so
//! that what a unit inherits from the units around it (a body's principal) is the planner's to
//! derive.
//!
//! # The session a statement is planned under
//!
//! Every request is asked under a [`PlanSession`] ([`Ask::session`]): the session the statement
//! being compiled is planned under, which the units it requests inherit. A request compiled
//! statement by statement ([`Compiler::request`]) plans each statement it compiles under the
//! session the statements before it leave. A statement it cannot plan ahead is served
//! when the run reaches it.
//!
//! # Sealing
//!
//! A program that reads records by id where the table is known only from data is given the
//! dispatch over the tables it reaches. The driver asks the planner for the read unit of each
//! table the program names, and of the tables those units read in turn, so a run finds each kept
//! when it loads it by name. A table the catalog does not hold is no candidate.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};

use surrealdb_strand::TableName;

use super::dispatch::{Reach, TableDispatch, reach_of};
use super::ir_lower::{
	ClosureTemplate, ContainerId, IndexResolver, LowerCx, LowerLimits, Mark, Needed, Slot,
	UnitRequest, UseOperand, lower_closure_body, lower_request_statement, lower_serving,
	lower_subscription, lower_unit, request_program, request_slots, request_steps,
};
use super::live::LiveUnit;
use super::program::{
	ClosureDigest, Program, RequestCode, RequestStep, UnitFrame, UnitId, UnitSig,
};
use super::units::CompiledUnits;
use super::{NotCovered, NotCoveredKind};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Capabilities, Force, Options};
use crate::exec::config::ExecConfig;
use crate::exec::function::FunctionRegistry;
use crate::exec::index::access_path;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::build::phys::index_ref;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ClosureAttrs, ExprOp};
use crate::exec::ir::iface::IndexRef;
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::{LetBinding, LoopSink};
use crate::exec::ir::logical::stages::write_loop;
use crate::exec::ir::logical::verify::loop_body_args;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::access::TableAnswers;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::surface::Surface;
use crate::exec::{AccessMode, BoxFut};
use crate::expr::logical::{EffectSet, LogicalOp};
use crate::expr::paths::{DB, NS};
use crate::kvs::Transaction;
use crate::kvs::speculation::Overlay;
use crate::val::Value;

/// The session the statements of a request leave for the ones after them, as planning reads it.
///
/// A request is planned statement by statement under the session its predecessors leave: the
/// options an `OPTION` sets, the namespace and database a `USE` selects, and the constants a `LET`
/// binds. `computed` names the `LET`s whose value only the run knows, so a statement that reads
/// one is not planned against a guess. Planning only reads this; the catalog creation, the
/// authorisation and the result row of a `USE` happen when the statement runs.
#[derive(Clone)]
pub(crate) struct PlanSession {
	/// The options the session carries: import, force, and the selected namespace and database.
	pub(crate) opt: Options,
	/// The `$session` value, with the namespace and database a `USE` selected. Shared with the
	/// context the request runs in until a `USE` changes it.
	session: Arc<Value>,
	/// The constants the `LET`s before the statement bound, by name.
	bound: HashMap<String, Value>,
	/// The names of the `LET`s whose value only the run computes.
	pub(crate) computed: HashSet<String>,
	/// The values of the request's own inputs, which a `USE` target may name as a parameter.
	pub(crate) inputs: Vec<(String, Value)>,
	/// The context a statement is bound in under this session, once [`Self::bound_context`] has
	/// built it. The mutators keep it equal to what a fresh build would give, or drop it.
	context: Option<BoundContext>,
}

/// The context [`PlanSession::bound_context`] built, with what it was built from.
#[derive(Clone)]
struct BoundContext {
	/// The context the request runs in, which the bound context is a snapshot of.
	base: FrozenContext,
	/// The transaction the bound context carries.
	txn: Arc<Transaction>,
	/// `base` with `txn`, the session's `$session` value and its constants.
	ctx: FrozenContext,
}

impl PlanSession {
	/// The session a request starts from: `opt`, and the `$session` value `session`.
	pub(crate) fn new(opt: Options, session: Arc<Value>) -> Self {
		Self {
			opt,
			session,
			bound: HashMap::new(),
			computed: HashSet::new(),
			inputs: Vec::new(),
			context: None,
		}
	}

	/// The `$session` value, with the namespace and database a `USE` selected.
	pub(crate) fn session(&self) -> &Value {
		&self.session
	}

	/// Whether a `LET` before the statement bound `name` to a constant.
	pub(crate) fn binds_constant(&self, name: &str) -> bool {
		self.bound.contains_key(name)
	}

	/// The context a statement of the request running in `base` is bound in, in `txn`: a
	/// snapshot of `base` holding the transaction, the `$session` value and the constants of this
	/// session.
	///
	/// The context is built once and kept: the mutators bring it up to date in place while no
	/// one else holds it, so a request of n statements builds it once rather than n times, and a
	/// `LET` or `USE` between them costs one binding rather than a copy of every variable the
	/// request holds.
	pub(crate) fn bound_context(
		&mut self,
		base: &FrozenContext,
		txn: &Arc<Transaction>,
	) -> FrozenContext {
		if let Some(bound) = &self.context
			&& Arc::ptr_eq(&bound.base, base)
			&& Arc::ptr_eq(&bound.txn, txn)
		{
			return Arc::clone(&bound.ctx);
		}
		let mut ctx = Context::snapshot(base);
		ctx.set_transaction(Arc::clone(txn));
		ctx.add_value("session", Arc::clone(&self.session));
		for (name, value) in &self.bound {
			ctx.add_value(name.clone(), value.clone().into());
		}
		let ctx = ctx.freeze();
		self.context = Some(BoundContext {
			base: Arc::clone(base),
			txn: Arc::clone(txn),
			ctx: Arc::clone(&ctx),
		});
		ctx
	}

	/// Apply `edit` to the kept bound context in place, or drop the context when someone else
	/// holds it, so the next [`Self::bound_context`] builds it afresh.
	fn edit_context(&mut self, edit: impl FnOnce(&mut Context)) {
		if let Some(bound) = &mut self.context {
			match Arc::get_mut(&mut bound.ctx) {
				Some(ctx) => edit(ctx),
				None => self.context = None,
			}
		}
	}

	/// An `OPTION name = what`: `IMPORT` and `FORCE` change the options and nothing else does.
	pub(crate) fn option(&mut self, name: &str, what: bool) {
		if name.eq_ignore_ascii_case("IMPORT") {
			self.opt.set_import(what);
		} else if name.eq_ignore_ascii_case("FORCE") {
			self.opt.force = if what {
				Force::All
			} else {
				Force::None
			};
		}
	}

	/// A `USE` whose names are stated: each level it selects replaces the session's.
	pub(crate) fn use_names(&mut self, ns: Option<&str>, db: Option<&str>) {
		if let Some(ns) = ns {
			self.opt.set_ns(Some(ns.into()));
			Arc::make_mut(&mut self.session).put(NS.as_ref(), ns.to_owned().into());
		}
		if let Some(db) = db {
			self.opt.set_db(Some(db.into()));
			Arc::make_mut(&mut self.session).put(DB.as_ref(), db.to_owned().into());
		}
		if ns.is_some() || db.is_some() {
			let session = Arc::clone(&self.session);
			self.edit_context(|ctx| ctx.add_value("session", session));
		}
	}

	/// A `LET` of `name`: a constant is bound for the statements after it, and any other value
	/// leaves the name computed.
	pub(crate) fn bind_let(&mut self, name: &str, constant: Option<Value>) {
		match constant {
			Some(constant) => {
				self.computed.remove(name);
				let value = Arc::new(constant.clone());
				self.bound.insert(name.to_owned(), constant);
				self.edit_context(|ctx| ctx.add_value(name.to_owned(), value));
			}
			None => {
				self.computed.insert(name.to_owned());
				// The bound context holds the constant a name loses here; what a fresh build
				// gives it instead is the base's binding, so the context is built afresh.
				if self.bound.remove(name).is_some() {
					self.context = None;
				}
			}
		}
	}

	/// The session after the statement of `slot`, `constant` being the value a session `LET`
	/// binds where it is a constant of the request.
	///
	/// Answers whether the session after the slot is stated. It is not after a `USE` whose target
	/// is neither a written name nor a parameter whose value this session holds, nor after
	/// `USE DEFAULT`, whose answer is the default configuration the run reads: the statements after
	/// such a `USE` are planned when the run reaches them, under the session the run leaves.
	pub(crate) fn after(&mut self, slot: &Slot, constant: Option<Value>) -> bool {
		match slot {
			Slot::Option {
				name,
				what,
				..
			} => {
				self.option(name, *what);
				true
			}
			Slot::Use {
				ns,
				db,
				computed,
				operands,
				..
			} => {
				if !*computed {
					self.use_names(ns.as_deref(), db.as_deref());
					return true;
				}
				let ns = operands.ns.stated(self);
				let db = operands.db.stated(self);
				let stated = ns != Stated::Unknown
					&& db != Stated::Unknown
					&& (ns != Stated::Absent || db != Stated::Absent);
				if stated {
					self.use_names(ns.name(), db.name());
				}
				stated
			}
			Slot::Let {
				name,
				..
			} => {
				self.bind_let(name, constant);
				true
			}
			Slot::Begin
			| Slot::Commit
			| Slot::Cancel
			| Slot::Run {
				..
			} => true,
		}
	}

	/// The string a parameter holds for this session: a constant `LET` of the request, or one of
	/// its inputs, where it is a string.
	fn named(&self, param: &str) -> Option<String> {
		let held = match self.computed.contains(param) {
			true => None,
			false => self.bound.get(param).or_else(|| {
				self.inputs.iter().rev().find(|(name, _)| name == param).map(|(_, value)| value)
			}),
		};
		match held {
			Some(Value::String(name)) => Some(name.as_str().to_owned()),
			_ => None,
		}
	}
}

/// What a session knows of one operand of a `USE` before it runs.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Stated {
	/// The statement has no such operand.
	Absent,
	/// The name the operand gives.
	Name(String),
	/// A name only the run can state.
	Unknown,
}

impl Stated {
	fn name(&self) -> Option<&str> {
		match self {
			Self::Name(name) => Some(name),
			Self::Absent | Self::Unknown => None,
		}
	}
}

impl UseOperand {
	/// What `session` knows of this operand: a written name, or a parameter it holds.
	fn stated(&self, session: &PlanSession) -> Stated {
		match self {
			Self::Absent => Stated::Absent,
			Self::Written(name) => Stated::Name(name.clone()),
			Self::Param(param) => session.named(param).map_or(Stated::Unknown, Stated::Name),
			Self::Computed => Stated::Unknown,
		}
	}
}

/// Where the walk of a request's statements starts ([`Compiler::request`]).
pub(crate) struct RequestStart {
	/// The session the first statement planned is planned under. Its `inputs` hold the request's
	/// own inputs, which a `USE` target may name as a parameter.
	pub(crate) session: PlanSession,
	/// The position of the first statement planned. The statements before it have run: they are
	/// stepped through, and only a `LET` among them binds for the session.
	pub(crate) first: usize,
	/// Whether the request starts inside a transaction block that is open.
	pub(crate) in_block: bool,
}

/// The program of a request and, for each of its statement programs by index, the statements
/// that program's blocks serve when they reach them, as written.
pub(crate) struct CompiledRequest {
	pub(crate) program: Program,
	pub(crate) served: Vec<Vec<Arc<LogicalOp>>>,
}

/// The statement programs a request walk has compiled so far.
#[derive(Default)]
struct RequestStatements {
	/// For each statement with a program, in order, the index of its program, or `None` for one
	/// served when it is reached.
	indices: VecDeque<Option<u32>>,
	programs: Vec<Arc<Program>>,
	served: Vec<Vec<Arc<LogicalOp>>>,
}

/// The effects through which a statement may change the catalog the statements after it are
/// planned against: a write of data or outside the transaction, a counter advanced, a
/// notification, deferred work, a catalog change, or a body whose effects are not stated. A
/// session write is not one of them: the session a statement leaves is [`PlanSession`]'s.
const CHANGES_FOR_LATER: EffectSet = EffectSet::WRITES_DATA
	.union(EffectSet::WRITES_EXTERNAL)
	.union(EffectSet::ADVANCES_STATE)
	.union(EffectSet::NOTIFY)
	.union(EffectSet::DEFERS)
	.union(EffectSet::CHANGES_CATALOG)
	.union(EffectSet::OPAQUE_BODY);

/// Whether the ops of the statement region `region` may change the catalog the statements after
/// it are planned against ([`CHANGES_FOR_LATER`]). A `LIVE` or `ACCESS` statement of any form
/// counts as storing whatever its effects are.
fn may_change_catalog(ir: &Ir, registry: &FunctionRegistry, region: RegionId) -> bool {
	let mut facts = FactCache::new();
	ops_in(ir, region).any(|op| {
		matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Live(_) | PhysOp::Access(_)))
		) || effects::of_with(ir, &mut facts, &structural_cx(registry, ir), op)
			.set
			.intersects(CHANGES_FOR_LATER)
	})
}

/// Whether an op inside `region` reads one of `names` as a free parameter: a session `LET` of the
/// request binds no value of the container, so a later statement reads its name as one.
fn reads_any(ir: &Ir, region: RegionId, names: &HashSet<String>) -> bool {
	if names.is_empty() {
		return false;
	}
	let mut reads = false;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		if let Some(OpKind::Expr(ExprOp::FreeParam {
			name,
		})) = ir.op(op).map(|data| &data.kind)
			&& names.contains(name.as_str())
		{
			reads = true;
			return Walk::Stop;
		}
		Walk::Continue
	});
	reads
}

/// The constant a session `LET` whose statement region is `region` binds, coerced to `kind`:
/// the literal its `phys.let` takes. `None` where the value is no literal or does not coerce, so
/// the name is computed for the statements after it.
fn let_constant(ir: &Ir, region: RegionId, kind: Option<&crate::expr::Kind>) -> Option<Value> {
	let op = ops_in(ir, region).find(|op| {
		matches!(
			ir.op(*op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Let(attrs))) if attrs.binding == LetBinding::Session
		)
	})?;
	let value = *ir.op(op)?.operands.first()?;
	let ValueDef::OpResult {
		op: def,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let OpKind::Expr(ExprOp::Literal(literal)) = &ir.op(def)?.kind else {
		return None;
	};
	match kind {
		Some(kind) => literal.as_ref().clone().coerce_to_kind(kind).ok(),
		None => Some(literal.as_ref().clone()),
	}
}

/// A container the planner produced, past the physical boundary and ready to lower.
pub(crate) struct Planned {
	pub(crate) ir: Ir,
	pub(crate) facts: FactCache,
	/// The catalog overlay the container was planned over: what the schema statements of the
	/// block before it wrote speculatively. `None` for the catalog as the block found it.
	pub(crate) planned_over: Option<Arc<Overlay>>,
	/// How many trailing arguments of the container's top block are values the program that runs
	/// it hands it: the inputs of a unit copied out of its requester ([`region_unit`]), which it
	/// is lowered to read ([`LowerCx::with_unit_inputs`]). `0` for any other container.
	pub(crate) unit_inputs: u16,
}

impl Planned {
	/// A container planned over the catalog as it stands.
	pub(crate) fn new(ir: Ir, facts: FactCache) -> Self {
		Self {
			ir,
			facts,
			planned_over: None,
			unit_inputs: 0,
		}
	}

	/// This container, planned over `over`.
	pub(crate) fn over(mut self, over: Option<Arc<Overlay>>) -> Self {
		self.planned_over = over;
		self
	}
}

/// The references of the indexes the containers of one compile open, which the planning callback
/// fills as it plans each container and the lowering reads through [`IndexResolver`].
///
/// A reference holds what the access-path decision read: the definition with its array columns
/// read as elements, the columns known to hold no array and the columns that are coarse components
/// of the record id. The key is the reference an op carries (table, name, position among the
/// indexes the read may use and the array-free columns), so two reads of one table that admit
/// different indexes (a current read and a versioned one, which admits only the full-text
/// indexes) hold their own references. Two containers that resolve the same key to different
/// definitions (a unit planned over a block's speculative schema redefining the index) cannot both
/// be lowered: [`SharedIndexes::learn`] answers a late bind for the second, which plans its
/// statement again with nothing shared.
#[derive(Default)]
pub(crate) struct SharedIndexes {
	known: RwLock<HashMap<IndexRef, access_path::IndexRef>>,
}

impl SharedIndexes {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Records the references of every index `answers` offers a read of its table.
	///
	/// An index already recorded under the same reference is left as it is. A key recorded for
	/// another definition is a late bind.
	pub(crate) fn learn(
		&self,
		answers: &TableAnswers,
		registry: &FunctionRegistry,
	) -> Result<(), NotCovered> {
		let mut known = self
			.known
			.write()
			.map_err(|_| NotCovered::internal("the index references of a compile are poisoned"))?;
		for reference in answers.index_refs(registry) {
			let key = index_ref(&reference);
			match known.get(&key) {
				Some(held) if *held == reference => {}
				Some(_) => return Err(NotCovered::late_bind()),
				None => {
					known.insert(key, reference);
				}
			}
		}
		Ok(())
	}
}

impl IndexResolver for SharedIndexes {
	fn resolve(&self, index: &IndexRef) -> Option<access_path::IndexRef> {
		self.known.read().ok()?.get(index).cloned()
	}
}

/// What a compile reads of the request that asks for it: the node's registry and capabilities, the
/// frame the units are lowered for, the request's configuration, the indexes its containers open,
/// the compile cache the planning callback serves units from and the session the compile starts
/// under.
pub(crate) struct CompileEnv<'a> {
	pub(crate) registry: &'a FunctionRegistry,
	pub(crate) capabilities: Arc<Capabilities>,
	pub(crate) frame: UnitFrame,
	/// Whether the reads of the principal the units are lowered for are permission checked
	/// ([`LowerCx::reads_checked`]).
	pub(crate) reads_checked: bool,
	/// The re-entry depth the units are lowered at.
	pub(crate) depth: u32,
	pub(crate) config: &'a ExecConfig,
	pub(crate) indexes: &'a dyn IndexResolver,
	pub(crate) compiled: Option<&'a CompiledUnits>,
	/// The session the compile's root is planned under ([`Ask::session`]); a statement of a
	/// request is planned under the one [`Compiler::statement`] is given.
	pub(crate) session: PlanSession,
	/// Whether an `EXPLAIN ANALYZE` the units render leaves out elapsed time and batch counts
	/// ([`LowerCx::redact_volatile`]).
	pub(crate) redact_volatile: bool,
	/// The statement families the request's entry serves: the units are lowered under the same
	/// surface as the request's own statements, so stored code is refused a statement the entry
	/// does not serve ([`LowerCx::schema`]).
	pub(crate) surface: &'static Surface,
}

impl<'a> CompileEnv<'a> {
	/// The lowering context of a compile over this environment: the index resolver and the limits
	/// of the request's configuration installed, and an empty unit table.
	pub(crate) fn context(&self) -> LowerCx<'a> {
		let cx = LowerCx::new(
			self.registry,
			Arc::clone(&self.capabilities),
			self.frame.clone(),
			self.depth,
		)
		.with_reads_checked(self.reads_checked)
		.with_indexes(self.indexes)
		.with_limits(LowerLimits::of(self.config))
		.with_redact_volatile(self.redact_volatile)
		.with_schema(self.surface.schema_lowering)
		.with_explain(self.surface.explain_lowering);
		match self.compiled {
			Some(compiled) => cx.with_compiled(compiled),
			None => cx,
		}
	}
}

/// The frame a unit is lowered for, whether its principal's reads are permission checked, and
/// the re-entry depth it is lowered at, where they are not the compile's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UnitScope {
	pub(crate) frame: UnitFrame,
	pub(crate) reads_checked: bool,
	pub(crate) depth: u32,
	/// The measure of the stored code the unit is lowered from, where it is a function body
	/// ([`LowerCx::depth_sites`](super::ir_lower::LowerCx)); `None` keeps the measure of the
	/// container that asked for it.
	pub(crate) depth_sites: Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>>,
	/// Whether the actor the unit runs on behalf of suppresses enforcement, where it differs from
	/// the unit's own frame: the cascades the unit triggers are requested for it. `None` where the
	/// actor's enforcement is the frame's.
	pub(crate) actor_suppressed: Option<bool>,
}

/// What the planner answers a request with.
pub(crate) enum Resolved {
	/// The container of a unit, the signature the unit's head names, and the frame and depth it
	/// is lowered for: `None` for those of the container that asked for it.
	Unit {
		planned: Box<Planned>,
		sig: UnitSig,
		scope: Option<UnitScope>,
	},
	/// The body of a closure literal of the requesting container, copied out of it, and what the
	/// unit's head names. It is lowered for the frame and depth of the container that asked.
	Closure {
		planned: Box<Planned>,
		template: Box<ClosureTemplate>,
	},
	/// A late-bound statement, as written, which the program plans when it reaches it. Only the
	/// root container of a compile holds served statements.
	Served(Arc<LogicalOp>),
	/// The reach of the read unit of a table, which the planner has lowered and the compile cache
	/// holds; `None` for a table the catalog does not hold.
	Table(Option<Reach>),
	/// The bundle of a linked write's stages, which the write's stage tables hold.
	Linked(Arc<Program>),
}

/// The container that made a request, which the planner reads.
pub(crate) struct Requester<'a> {
	pub(crate) id: ContainerId,
	pub(crate) ir: &'a Ir,
	pub(crate) facts: &'a FactCache,
}

/// A request the planner answers.
pub(crate) struct Ask<'a> {
	pub(crate) request: &'a UnitRequest,
	/// The container that made the request.
	pub(crate) by: Requester<'a>,
	/// The overlay the requesting container was planned over.
	pub(crate) over: Option<&'a Arc<Overlay>>,
	/// The requests the requesting container descends from, from the compile's root to the one
	/// that asked for it: empty for the root.
	pub(crate) path: &'a [UnitRequest],
	/// The session the statement being compiled is planned under, which every unit it requests
	/// is planned under too.
	pub(crate) session: &'a PlanSession,
	/// The tables the requested unit is planned under the guard of ([`held_for`]): a unit
	/// planned anew puts each on its planner's cycle guard, so a statement it holds that reads
	/// one leaves that table's permission unresolved rather than planning it again.
	pub(crate) held: &'a [TableName],
}

/// Plans what a lowering asks for: the callback of [`Compiler`].
///
/// A request that names a position inside the requesting container (a region, a served
/// statement, a closure body) is answered from that container, which the planner was handed.
pub(crate) trait UnitPlanner: Send {
	fn plan<'a>(&'a mut self, ask: Ask<'a>) -> BoxFut<'a, Result<Resolved, NotCovered>>;

	/// Plans the statement at `position` of the request [`Compiler::request`] compiles, alone,
	/// under `session`: the options, namespace, database and constants the statements before it
	/// leave. A catalog answer the planner does not hold is a late bind, which serves the
	/// statement when it is reached.
	///
	/// A planner that plans no request statement answers [`NotCoveredKind::Unsupported`].
	fn plan_statement<'a>(
		&'a mut self,
		position: u32,
		session: &'a PlanSession,
	) -> BoxFut<'a, Result<Planned, NotCovered>> {
		let _ = session;
		Box::pin(async move {
			Err(NotCovered::unsupported(format!(
				"the planner plans no statement of a request (position {position})"
			)))
		})
	}
}

impl<T: UnitPlanner + ?Sized> UnitPlanner for &mut T {
	fn plan<'a>(&'a mut self, ask: Ask<'a>) -> BoxFut<'a, Result<Resolved, NotCovered>> {
		(**self).plan(ask)
	}

	fn plan_statement<'a>(
		&'a mut self,
		position: u32,
		session: &'a PlanSession,
	) -> BoxFut<'a, Result<Planned, NotCovered>> {
		(**self).plan_statement(position, session)
	}
}

/// What kind of program a container is lowered to.
pub(super) enum Shape {
	/// The program an operator runs, which serves its blocks' late-bound statements.
	Serving,
	/// A statement of a request, whose units are the request's.
	Statement,
	/// A unit of the program being compiled.
	Unit(UnitSig),
	/// The body of a closure literal, a unit of the program being compiled whose head names what
	/// the template states.
	Closure(Box<ClosureTemplate>),
}

/// Lowers a container. [`Direct`] is the lowering of production; a test substitutes one that
/// makes requests the real lowering does not make.
pub(super) trait Lower: Send {
	fn lower(
		&self,
		cx: &mut LowerCx<'_>,
		planned: &Planned,
		shape: Shape,
	) -> Result<super::ir_lower::Lowered, NotCovered>;
}

/// The lowering of `ir_lower`.
pub(super) struct Direct;

impl Lower for Direct {
	fn lower(
		&self,
		cx: &mut LowerCx<'_>,
		planned: &Planned,
		shape: Shape,
	) -> Result<super::ir_lower::Lowered, NotCovered> {
		let (ir, facts) = (&planned.ir, &planned.facts);
		match shape {
			Shape::Serving => lower_serving(cx, ir, facts),
			Shape::Statement => lower_request_statement(cx, ir, facts),
			Shape::Unit(sig) => lower_unit(cx, ir, facts, sig),
			Shape::Closure(template) => lower_closure_body(cx, ir, &template),
		}
	}
}

/// A program compiled in full, with the statements its blocks serve when they reach them as
/// written, at the index of the program's `served` entry for each.
pub(crate) struct Compiled {
	pub(crate) program: Program,
	pub(crate) served: Vec<Arc<LogicalOp>>,
}

/// The state of a [`Compiler`] at a point, to restore on a failed compile.
#[derive(Clone, Copy)]
struct Checkpoint {
	cx: Mark,
	held: usize,
	folds: usize,
}

/// The compile driver of one program, or of one request's statements.
pub(crate) struct Compiler<'c, P, L = Direct> {
	cx: LowerCx<'c>,
	planner: P,
	lower: L,
	/// Every container taken so far, by [`ContainerId`].
	held: Vec<Planned>,
	/// For each container of `held`, the requests it descends from ([`Ask::path`]).
	paths: Vec<Arc<[UnitRequest]>>,
	/// For each container of `held`, the frame and depth it was lowered for: `None` for the
	/// compile's.
	scopes: Vec<Option<UnitScope>>,
	/// The program of each slot of the unit table, once lowered.
	programs: Vec<Option<Arc<Program>>>,
	/// For each container of `held`, the slot of the unit table its program fills: none for a
	/// compile's root, which the caller holds.
	slots: Vec<Option<usize>>,
	/// For each container of `held`, the tables it was planned under the guard of ([`held_for`]):
	/// none for a compile's root.
	guards: Vec<Arc<[TableName]>>,
	/// The subquery, closure-body and recursion-body units lowered so far, each with the
	/// container that asked for it, in the order they were lowered.
	folds: Vec<Fold>,
	/// The session the statement being compiled is planned under, which its units are asked
	/// under ([`Ask::session`]).
	session: PlanSession,
}

/// A unit that gives its requester what its lowering knows, and the container whose program
/// evaluates it.
#[derive(Clone, Copy)]
struct Fold {
	by: ContainerId,
	unit: UnitId,
	/// Whether the unit is a subquery, which also gives its required context and row-order flag;
	/// a closure body or a recursion body gives its access mode alone.
	subquery: bool,
}

impl<'c, P: UnitPlanner> Compiler<'c, P, Direct> {
	/// A driver for a compile over `env`, planning through `planner`: the entry a request's
	/// compile takes, which lowers with the environment's index resolver and limits.
	pub(crate) fn over(env: &CompileEnv<'c>, planner: P) -> Self {
		Self::new(env.context(), planner, env.session.clone())
	}

	/// A driver lowering under `cx` and planning through `planner`, whose root is planned under
	/// `session`.
	pub(crate) fn new(cx: LowerCx<'c>, planner: P, session: PlanSession) -> Self {
		Self {
			cx,
			planner,
			lower: Direct,
			held: Vec::new(),
			paths: Vec::new(),
			scopes: Vec::new(),
			programs: Vec::new(),
			slots: Vec::new(),
			guards: Vec::new(),
			folds: Vec::new(),
			session,
		}
	}
}

impl<'c, P: UnitPlanner, L: Lower> Compiler<'c, P, L> {
	/// This driver, lowering with `lower`.
	#[cfg(test)]
	pub(super) fn lowering<M: Lower>(self, lower: M) -> Compiler<'c, P, M> {
		Compiler {
			cx: self.cx,
			planner: self.planner,
			lower,
			held: self.held,
			paths: self.paths,
			scopes: self.scopes,
			programs: self.programs,
			slots: self.slots,
			guards: self.guards,
			folds: self.folds,
			session: self.session,
		}
	}

	/// Lowers what the compile lowers on behalf of an actor whose enforcement is `actor` where it
	/// differs from the frame's ([`LowerCx::actor_suppressed`]); a unit planned for an actor of
	/// its own is lowered for that one.
	pub(crate) fn on_behalf_of(&mut self, actor: Option<bool>) {
		self.cx.reactor(actor);
	}

	/// Compiles `root` as the program an operator runs, holding every unit it needs and the
	/// dispatch over the tables it reads by id. `known` are the tables a caller knows the records
	/// by id to be of.
	///
	/// The compiler holds no unit before the call: the ids of the program index its own table.
	pub(crate) async fn program(
		&mut self,
		root: Planned,
		known: BTreeSet<TableName>,
	) -> Result<Compiled, NotCovered> {
		if self.cx.slots() != 0 {
			return Err(NotCovered::internal("a program compiled over a table that holds units"));
		}
		self.compile(root, Shape::Serving, true, known).await
	}

	/// Compiles `root`, a container planned as a live subscription's notification unit, as the unit
	/// the subscription runs for each record of its table: the program of its host pipelines,
	/// holding every unit it needs and the dispatch over the tables it reads by id, and the plan
	/// that names the pipelines. The lowering plans nothing; the units it asks for are planned
	/// through the planning callback before the unit is answered.
	///
	/// The compiler holds no unit before the call: the ids of the program index its own table.
	pub(crate) async fn subscription(&mut self, root: Planned) -> Result<LiveUnit, NotCovered> {
		if self.cx.slots() != 0 {
			return Err(NotCovered::internal("a program compiled over a table that holds units"));
		}
		let checkpoint = self.checkpoint();
		let unit = self.subscription_in(root).await;
		if unit.is_err() {
			self.restore(checkpoint);
		}
		unit
	}

	async fn subscription_in(&mut self, root: Planned) -> Result<LiveUnit, NotCovered> {
		let root = self.take(root, Arc::from([]), None, Arc::from([]))?;
		let planned = self
			.held
			.get(root.0 as usize)
			.ok_or_else(|| NotCovered::internal("a container the compile does not hold"))?;
		self.cx.enter(root);
		self.cx.set_unit_inputs(planned.unit_inputs);
		let live = lower_subscription(&mut self.cx, &planned.ir)?;
		let mut program = live.lowered.program;
		let mut served: Vec<Option<Arc<LogicalOp>>> = Vec::new();
		self.close(root, live.lowered.needs, &mut served).await?;
		if !served.is_empty() {
			return Err(NotCovered::internal("a subscription's unit serves a statement"));
		}
		while self.fold_subqueries(root, &mut program)? | self.fold_functions(0, &mut program)? {}
		self.folds.clear();
		program.units = self.table();
		self.seal(&mut program, root, BTreeSet::new()).await?;
		Ok(LiveUnit {
			program: Arc::new(program),
			plan: live.plan,
		})
	}

	/// Compiles `root`, planned under `session`, as the next statement of a request; every unit
	/// it requests is asked for under `session` too. The statement holds none of its units: its
	/// ids index the table of the program that holds the request ([`Self::finish_request`]).
	pub(crate) async fn statement(
		&mut self,
		root: Planned,
		session: &PlanSession,
	) -> Result<Compiled, NotCovered> {
		self.session = session.clone();
		self.compile(root, Shape::Statement, false, BTreeSet::new()).await
	}

	/// The program of the request: `steps`, and the programs of its statements, with the table
	/// of the units they share.
	pub(crate) fn finish_request(
		self,
		steps: Vec<RequestStep>,
		statements: Vec<Arc<Program>>,
	) -> Result<Program, NotCovered> {
		request_program(
			self.table(),
			RequestCode {
				steps,
				statements,
			},
		)
	}

	/// Compiles the statements of `request`, a request container, into the program of the
	/// request, walking them in the order they were written from `start`.
	///
	/// A statement is compiled here only where it can be compiled ahead of the run: it is
	/// planned (at or after `start.first`), it runs in the transaction of the first statement
	/// planned, no statement planned before it may have changed the catalog, and it reads no
	/// parameter that a `LET` before it binds to a value only the run computes. A statement may
	/// have changed the catalog where it is a `USE`, or where its ops may write data, write
	/// outside the transaction, advance state, notify, defer work, change the catalog or run a body
	/// whose effects are not stated. Each statement compiled is planned alone by the planner
	/// ([`UnitPlanner::plan_statement`]) under the session the statements before it leave
	/// ([`PlanSession::after`]); after a `USE` whose target the session cannot state, nothing is
	/// planned against a guessed session. Every other statement, and one whose planning or
	/// lowering answers a late bind, is served when the driver reaches it: its step names no
	/// program.
	///
	/// Any other refusal of a statement's planning or lowering refuses the request.
	pub(crate) async fn request(
		mut self,
		request: &Ir,
		start: RequestStart,
	) -> Result<CompiledRequest, NotCovered> {
		let slots = request_slots(request)?;
		let RequestStart {
			mut session,
			first,
			mut in_block,
		} = start;
		let mut same_txn = true;
		let mut changed = false;
		let mut stated = true;
		let mut statements = RequestStatements::default();
		for (at, slot) in slots.iter().enumerate() {
			let planning = at >= first;
			if planning && at > first && !in_block {
				same_txn &=
					matches!(slot, Slot::Begin | Slot::Commit | Slot::Cancel | Slot::Option { .. });
			}
			let inline = planning && same_txn && !changed && stated;
			match slot {
				Slot::Begin => {
					if planning && at > first && !in_block {
						same_txn = false;
					}
					in_block = true;
				}
				Slot::Commit | Slot::Cancel => {
					in_block = false;
					same_txn &= !planning;
				}
				Slot::Option {
					statement,
					..
				} => {
					self.request_statement(
						request,
						at,
						*statement,
						inline,
						&session,
						&mut statements,
					)
					.await?;
					if planning {
						session.after(slot, None);
					}
				}
				Slot::Use {
					statement,
					..
				} => {
					self.request_statement(
						request,
						at,
						*statement,
						inline,
						&session,
						&mut statements,
					)
					.await?;
					if planning {
						changed = true;
						stated &= session.after(slot, None);
					}
				}
				Slot::Let {
					kind,
					statement,
					..
				} => {
					self.request_statement(
						request,
						at,
						*statement,
						inline,
						&session,
						&mut statements,
					)
					.await?;
					let constant = match planning {
						true => let_constant(request, *statement, kind.as_ref()),
						false => None,
					};
					session.after(slot, constant);
					changed |=
						planning && may_change_catalog(request, self.cx.registry(), *statement);
				}
				Slot::Run {
					statement,
				} => {
					self.request_statement(
						request,
						at,
						*statement,
						inline,
						&session,
						&mut statements,
					)
					.await?;
					changed |=
						planning && may_change_catalog(request, self.cx.registry(), *statement);
				}
			}
		}
		let RequestStatements {
			mut indices,
			programs,
			served,
		} = statements;
		let steps = request_steps(request, |_| {
			indices.pop_front().ok_or(IrError::Invariant {
				what: "a request statement the walk did not reach",
			})
		})?;
		let program = self.finish_request(steps, programs)?;
		Ok(CompiledRequest {
			program,
			served,
		})
	}

	/// Compiles the statement at `position` of `request`, whose region is `region`, where it is
	/// `inline` and reads no parameter `session` holds as computed, and records the index of its
	/// program, or `None` for a statement served when it is reached.
	async fn request_statement(
		&mut self,
		request: &Ir,
		position: usize,
		region: RegionId,
		inline: bool,
		session: &PlanSession,
		statements: &mut RequestStatements,
	) -> Result<(), NotCovered> {
		let index = match inline && !reads_any(request, region, &session.computed) {
			true => self.compile_request_statement(position, session, statements).await?,
			false => None,
		};
		statements.indices.push_back(index);
		Ok(())
	}

	/// Plans and compiles the statement at `position` under `session`: the index of its program,
	/// or `None` where planning or lowering it answers a late bind.
	async fn compile_request_statement(
		&mut self,
		position: usize,
		session: &PlanSession,
		statements: &mut RequestStatements,
	) -> Result<Option<u32>, NotCovered> {
		let position = u32::try_from(position)
			.map_err(|_| NotCovered::internal("more statements than a request holds"))?;
		let planned = match self.planner.plan_statement(position, session).await {
			Ok(planned) => planned,
			Err(refused) if refused.kind == NotCoveredKind::LateBind => return Ok(None),
			Err(refused) => return Err(refused),
		};
		let compiled = match self.statement(planned, session).await {
			Ok(compiled) => compiled,
			Err(refused) if refused.kind == NotCoveredKind::LateBind => return Ok(None),
			Err(refused) => return Err(refused),
		};
		let index = u32::try_from(statements.programs.len())
			.map_err(|_| NotCovered::internal("more statement programs than a request holds"))?;
		statements.programs.push(Arc::new(compiled.program));
		statements.served.push(compiled.served);
		Ok(Some(index))
	}

	/// The unit table: the program of every slot, which a compile that returned has all filled.
	fn table(&self) -> crate::exec::vm::UnitTable {
		self.programs.iter().filter_map(Option::clone).collect()
	}

	fn checkpoint(&self) -> Checkpoint {
		Checkpoint {
			cx: self.cx.mark(),
			held: self.held.len(),
			folds: self.folds.len(),
		}
	}

	fn restore(&mut self, checkpoint: Checkpoint) {
		self.programs.truncate(checkpoint.cx.slots());
		self.cx.rewind(checkpoint.cx);
		self.held.truncate(checkpoint.held);
		self.paths.truncate(checkpoint.held);
		self.scopes.truncate(checkpoint.held);
		self.slots.truncate(checkpoint.held);
		self.guards.truncate(checkpoint.held);
		self.folds.truncate(checkpoint.folds);
	}

	/// Compiles `root`, and leaves the compiler as it was if that fails. A root that
	/// `own_units` holds the table the compile filled, which its reach is judged over.
	async fn compile(
		&mut self,
		root: Planned,
		shape: Shape,
		own_units: bool,
		known: BTreeSet<TableName>,
	) -> Result<Compiled, NotCovered> {
		let checkpoint = self.checkpoint();
		let compiled = self.compile_in(root, shape, own_units, known).await;
		if compiled.is_err() {
			self.restore(checkpoint);
		}
		compiled
	}

	async fn compile_in(
		&mut self,
		root: Planned,
		shape: Shape,
		own_units: bool,
		known: BTreeSet<TableName>,
	) -> Result<Compiled, NotCovered> {
		// The slots this compile fills: a statement of a request reuses the units of the
		// statements before it, whose programs are final.
		let first_slot = self.cx.slots();
		let root = self.take(root, Arc::from([]), None, Arc::from([]))?;
		self.link_stages(root).await?;
		let lowered = self.lower_held(root, shape)?;
		let mut program = lowered.program;
		let mut served: Vec<Option<Arc<LogicalOp>>> = Vec::new();
		self.close(root, lowered.needs, &mut served).await?;
		// A subquery unit may call a function and a function body may hold a subquery, so the two
		// folds repeat until neither widens anything; each only widens, so they end.
		while self.fold_subqueries(root, &mut program)?
			| self.fold_functions(first_slot, &mut program)?
		{}
		self.folds.clear();
		let served = served
			.into_iter()
			.collect::<Option<Vec<_>>>()
			.ok_or_else(|| NotCovered::internal("a served statement the planner never resolved"))?;
		if own_units {
			program.units = self.table();
		}
		self.seal(&mut program, root, known).await?;
		Ok(Compiled {
			program,
			served,
		})
	}

	/// Takes `planned`, which descends from the requests `path`, is lowered for `scope` and was
	/// planned under the guard of `guards`, as the next container of the compile.
	fn take(
		&mut self,
		planned: Planned,
		path: Arc<[UnitRequest]>,
		scope: Option<UnitScope>,
		guards: Arc<[TableName]>,
	) -> Result<ContainerId, NotCovered> {
		let id = u32::try_from(self.held.len())
			.map_err(|_| NotCovered::internal("more containers than a compile holds"))?;
		self.held.push(planned);
		self.paths.push(path);
		self.scopes.push(scope);
		self.slots.push(None);
		self.guards.push(guards);
		Ok(ContainerId(id))
	}

	/// Resolves, through the planner, the bundle of the stages of each write of container `id`, the
	/// root of the compile, whose stages are linked, and holds it for the container's lowering
	/// ([`LowerCx::link_stages`]): the bundles are in the program before it runs, so its run
	/// loads none.
	async fn link_stages(&mut self, id: ContainerId) -> Result<(), NotCovered> {
		let sets = {
			let planned = self
				.held
				.get(id.0 as usize)
				.ok_or_else(|| NotCovered::internal("a container the compile does not hold"))?;
			super::ir_lower::linked_stage_sets(&planned.ir)?
		};
		for (table, set) in sets {
			if self.cx.links_stages(&table, &set) {
				continue;
			}
			let request = UnitRequest::LinkedStages {
				table: table.clone(),
				set: Arc::new(set.clone()),
				actor: self.cx.actor(),
			};
			let resolved = {
				let (held, path, guards) = self
					.held
					.get(id.0 as usize)
					.zip(self.paths.get(id.0 as usize))
					.zip(self.guards.get(id.0 as usize))
					.map(|((held, path), guards)| (held, path, guards))
					.ok_or_else(|| NotCovered::internal("a container the compile does not hold"))?;
				let ask = Ask {
					request: &request,
					by: Requester {
						id,
						ir: &held.ir,
						facts: &held.facts,
					},
					over: held.planned_over.as_ref(),
					path,
					session: &self.session,
					held: guards,
				};
				self.planner.plan(ask).await?
			};
			let Resolved::Linked(bundle) = resolved else {
				return Err(NotCovered::internal(
					"a linked write's stages answered as another unit",
				));
			};
			self.cx.link_stages(table, set, bundle);
		}
		Ok(())
	}

	/// [`Self::lower_held`] for the frame and depth `scope` names, where it names one; the
	/// compile's are restored however the lowering ends.
	fn lower_scoped(
		&mut self,
		id: ContainerId,
		shape: Shape,
		scope: Option<UnitScope>,
	) -> Result<super::ir_lower::Lowered, NotCovered> {
		let Some(scope) = scope else {
			return self.lower_held(id, shape);
		};
		let (frame, reads_checked, depth) =
			self.cx.rescope(scope.frame, scope.reads_checked, scope.depth);
		let actor = self.cx.reactor(scope.actor_suppressed);
		let sites = scope.depth_sites.map(|sites| self.cx.resite(Some(sites)));
		let lowered = self.lower_held(id, shape);
		self.cx.rescope(frame, reads_checked, depth);
		self.cx.reactor(actor);
		if let Some(sites) = sites {
			self.cx.resite(sites);
		}
		lowered
	}

	/// Lowers the container `id` as `shape`, which is the container being lowered while it does.
	fn lower_held(
		&mut self,
		id: ContainerId,
		shape: Shape,
	) -> Result<super::ir_lower::Lowered, NotCovered> {
		let planned = self
			.held
			.get(id.0 as usize)
			.ok_or_else(|| NotCovered::internal("a container the compile does not hold"))?;
		self.cx.enter(id);
		self.cx.set_unit_inputs(planned.unit_inputs);
		self.lower.lower(&mut self.cx, planned, shape)
	}

	/// Plans and lowers each of `needs`, requested by container `root`, and each request their
	/// lowerings make in turn, until none is left. The unit of a request that holds a slot is
	/// lowered into it. A statement a block serves is recorded at its position in `served`.
	///
	/// Answers once every slot the lowerings reserved holds a program: nothing is left to lower
	/// when the program runs.
	async fn close(
		&mut self,
		root: ContainerId,
		needs: Vec<Needed>,
		served: &mut Vec<Option<Arc<LogicalOp>>>,
	) -> Result<(), NotCovered> {
		let mut queue: VecDeque<(ContainerId, Needed)> =
			needs.into_iter().map(|need| (root, need)).collect();
		while let Some((by, need)) = queue.pop_front() {
			let (resolved, inherited, path, scoped, guards) = {
				let (held, path) = self
					.held
					.get(by.0 as usize)
					.zip(self.paths.get(by.0 as usize))
					.ok_or_else(|| NotCovered::internal("a request of a container not held"))?;
				let guards = self
					.guards
					.get(by.0 as usize)
					.ok_or_else(|| NotCovered::internal("a request of a container not held"))?;
				let guards = held_for(&held.ir, &need.request, guards)?;
				let ask = Ask {
					request: &need.request,
					by: Requester {
						id: by,
						ir: &held.ir,
						facts: &held.facts,
					},
					over: held.planned_over.as_ref(),
					path,
					session: &self.session,
					held: &guards,
				};
				let scoped = self.scopes.get(by.0 as usize).cloned().flatten();
				let resolved = self.planner.plan(ask).await?;
				(resolved, held.planned_over.clone(), Arc::clone(path), scoped, guards)
			};
			let (mut planned, shape, scope) = match resolved {
				Resolved::Unit {
					planned,
					sig,
					scope,
				} => (planned, Shape::Unit(sig), scope),
				Resolved::Closure {
					planned,
					template,
				} => (planned, Shape::Closure(template), None),
				Resolved::Served(op) => {
					let UnitRequest::ServedStatement {
						position,
						..
					} = need.request
					else {
						return Err(NotCovered::internal(
							"a served statement answered another request",
						));
					};
					if by != root {
						return Err(NotCovered::internal("a served statement of a unit"));
					}
					let position = position as usize;
					if served.len() <= position {
						served.resize(position + 1, None);
					}
					served[position] = Some(op);
					continue;
				}
				Resolved::Table(_) => {
					return Err(NotCovered::internal("a table unit answered a request for a unit"));
				}
				Resolved::Linked(_) => {
					return Err(NotCovered::internal(
						"a linked bundle answered a request for a unit",
					));
				}
			};
			let Some(id) = need.id else {
				return Err(NotCovered::internal("a unit answered a request with no slot"));
			};
			if planned.planned_over.is_none() {
				planned.planned_over = inherited;
			}
			let path: Arc<[UnitRequest]> =
				path.iter().cloned().chain([need.request.clone()]).collect();
			let mut scope = scope.or(scoped);
			// A region copied out of its requester is code of the requester's: it spends the
			// computation-depth budget as the stored code the requester was lowering when it asked
			// for the region, an event clause or a function body.
			if need.request.is_relative()
				&& let Some(sites) = self.cx.requested_sites(id)
				&& let Some(scope) = scope.as_mut()
			{
				scope.depth_sites = Some(sites);
			}
			let container = self.take(*planned, path, scope.clone(), guards)?;
			// A unit of a statement in an expression stands where the region it is copied from
			// stood when it was asked for; any other unit stands at its own top.
			let nesting = if matches!(need.request, UnitRequest::Subquery { .. }) {
				self.cx.requested_nesting(id)
			} else {
				Default::default()
			};
			// Only a statement of a request links a write's stages; a unit that holds one
			// declines, and the statement is compiled again with its stages inline.
			let planned = self
				.held
				.get(container.0 as usize)
				.ok_or_else(|| NotCovered::internal("a container the compile does not hold"))?;
			if !super::ir_lower::linked_stage_sets(&planned.ir)?.is_empty() {
				return Err(NotCovered::internal("a unit of a statement holds a linked write"));
			}
			let outer = self.cx.renest(nesting);
			let lowered = self.lower_scoped(container, shape, scope);
			self.cx.renest(outer);
			let lowered = lowered?;
			let slot = id.0 as usize;
			if self.programs.len() <= slot {
				self.programs.resize(slot + 1, None);
			}
			if self.programs[slot].is_some() {
				return Err(NotCovered::internal("a unit lowered into a slot twice"));
			}
			self.programs[slot] = Some(Arc::new(lowered.program));
			self.slots[container.0 as usize] = Some(slot);
			let subquery = matches!(need.request, UnitRequest::Subquery { .. });
			if subquery
				|| matches!(
					need.request,
					UnitRequest::ClosureBody { .. } | UnitRequest::LoopBody { .. }
				) {
				self.folds.push(Fold {
					by,
					unit: id,
					subquery,
				});
			}
			queue.extend(lowered.needs.into_iter().map(|need| (container, need)));
		}
		let closed =
			(0..self.cx.slots()).all(|slot| matches!(self.programs.get(slot), Some(Some(_))));
		if !closed {
			return Err(NotCovered::internal("a unit was asked for and never lowered"));
		}
		Ok(())
	}

	/// Writes what each subquery unit takes of its requester into the requester's program: the
	/// unit's access mode, required context and row-order flag into the [`SubquerySpec`] that
	/// names it, and into the requester's own. A closure body or a recursion body writes its
	/// access mode alone, into the [`ClosureValueSpec`] that names it and the requester's own, and
	/// a unit that writes clears `read_only` on every [`NestedSpec`] of the requester.
	/// `root_program` is the program of container `root`, which the caller holds.
	///
	/// Units fold from the last lowered to the first, because a unit's requester is always lowered
	/// before it. Answers whether anything widened.
	///
	/// [`SubquerySpec`]: super::program::SubquerySpec
	/// [`ClosureValueSpec`]: super::program::ClosureValueSpec
	/// [`NestedSpec`]: super::program::NestedSpec
	fn fold_subqueries(
		&mut self,
		root: ContainerId,
		root_program: &mut Program,
	) -> Result<bool, NotCovered> {
		let mut widened = false;
		let folds = self.folds.clone();
		for fold in folds.iter().rev() {
			let unit =
				self.programs.get(fold.unit.0 as usize).and_then(Option::as_deref).ok_or_else(
					|| NotCovered::internal("a subquery unit folded before it was lowered"),
				)?;
			let (access_mode, required_context, keeps_row_order) = match fold.subquery {
				true => (unit.access_mode, unit.required_context, unit.keeps_row_order),
				false => (unit.access_mode, crate::exec::ContextLevel::Root, false),
			};
			let requester = if fold.by == root {
				&mut *root_program
			} else {
				let slot =
					self.slots.get(fold.by.0 as usize).copied().flatten().ok_or_else(|| {
						NotCovered::internal("a subquery requested by no program")
					})?;
				let held =
					self.programs.get_mut(slot).and_then(Option::as_mut).ok_or_else(|| {
						NotCovered::internal("a subquery requested by a unit never lowered")
					})?;
				Arc::get_mut(held).ok_or_else(|| {
					NotCovered::internal("a unit program shared before the compile closed")
				})?
			};
			for spec in requester.subqueries.iter_mut().filter(|spec| spec.unit == fold.unit) {
				let before = (spec.access_mode, spec.required_context, spec.keeps_row_order);
				spec.access_mode = spec.access_mode.combine(access_mode);
				spec.required_context = spec.required_context.max(required_context);
				spec.keeps_row_order |= keeps_row_order;
				widened |=
					before != (spec.access_mode, spec.required_context, spec.keeps_row_order);
			}
			for spec in
				requester.closure_values.iter_mut().filter(|spec| spec.body == Some(fold.unit))
			{
				let before = spec.access_mode;
				spec.access_mode = spec.access_mode.combine(access_mode);
				widened |= before != spec.access_mode;
			}
			if access_mode == AccessMode::ReadWrite {
				for spec in requester.nested.iter_mut().filter(|spec| spec.read_only) {
					spec.read_only = false;
					widened = true;
				}
			}
			let before =
				(requester.access_mode, requester.required_context, requester.keeps_row_order);
			requester.access_mode = requester.access_mode.combine(access_mode);
			requester.required_context = requester.required_context.max(required_context);
			requester.keeps_row_order |= keeps_row_order;
			widened |= before
				!= (requester.access_mode, requester.required_context, requester.keeps_row_order);
		}
		Ok(widened)
	}

	/// Writes what the body unit of each call of a user-defined function does into the call and
	/// the program that makes it: the body's access mode into the call's `body_access_mode` and
	/// the program's, and its row-order flag into the program's. `root_program` is the program of
	/// the compile's root, which the caller holds.
	///
	/// Only the programs this compile lowered are written, from slot `first_slot` on: a unit of a
	/// statement before is final. A body reports what the bodies it calls do once they are
	/// written into it, so the pass repeats until nothing widens; each repetition only widens, so
	/// it ends. Answers whether anything widened.
	fn fold_functions(
		&mut self,
		first_slot: usize,
		root_program: &mut Program,
	) -> Result<bool, NotCovered> {
		let mut any = false;
		loop {
			let reports: Vec<Option<(AccessMode, bool)>> = self
				.programs
				.iter()
				.map(|held| held.as_deref().map(|unit| (unit.access_mode, unit.keeps_row_order)))
				.collect();
			let mut widened = widen_calls(root_program, &reports);
			for held in self.programs.iter_mut().skip(first_slot) {
				let Some(held) = held.as_mut() else {
					return Err(NotCovered::internal(
						"a function body folded before it was lowered",
					));
				};
				let program = Arc::get_mut(held).ok_or_else(|| {
					NotCovered::internal("a unit program shared before the compile closed")
				})?;
				widened |= widen_calls(program, &reports);
			}
			if !widened {
				return Ok(any);
			}
			any = true;
		}
	}

	/// Gives `program` the dispatch over the tables it and its units read records of by id, and
	/// of `known`, and of the tables those units read in turn.
	///
	/// Each table has a read unit for the caller and, where the caller's reads are checked
	/// ([`LowerCx::reads_checked`]), one for a frame that waives the checks (a permission
	/// predicate, an event). A reach that may name any table names the tables its sites name and
	/// the tables written: the run loads the rest.
	async fn seal(
		&mut self,
		program: &mut Program,
		root: ContainerId,
		known: BTreeSet<TableName>,
	) -> Result<(), NotCovered> {
		let reach = match reach_of(program) {
			reach if reaches_nothing(&reach) => reach,
			reach => reach.union(Reach::Tables(known.clone())),
		};
		if reaches_nothing(&reach) {
			return Ok(());
		}
		let waived = self.cx.reads_checked();
		let mut named = tables_named(reach);
		named.extend(known);
		let mut tables: Vec<TableName> = Vec::new();
		let mut absent: BTreeSet<TableName> = BTreeSet::new();
		loop {
			let mut grew = false;
			for table in named.clone() {
				if tables.contains(&table) || absent.contains(&table) {
					continue;
				}
				match self.table_unit(&table, false, root).await? {
					Some(reach) => named.extend(tables_named(reach)),
					None => {
						absent.insert(table);
						continue;
					}
				}
				if waived && let Some(reach) = self.table_unit(&table, true, root).await? {
					named.extend(tables_named(reach));
				}
				tables.push(table);
				grew = true;
			}
			if !grew {
				tables.sort();
				program.dispatch = Some(Arc::new(TableDispatch {
					tables,
				}));
				return Ok(());
			}
		}
	}

	/// The reach of the read unit of `table` the planner has put in the compile cache, or `None`
	/// where the catalog holds no such table.
	async fn table_unit(
		&mut self,
		table: &TableName,
		unchecked: bool,
		root: ContainerId,
	) -> Result<Option<Reach>, NotCovered> {
		let (held, guards) =
			self.held.get(root.0 as usize).zip(self.guards.get(root.0 as usize)).ok_or_else(
				|| NotCovered::internal("a table unit asked for by a container not held"),
			)?;
		let request = UnitRequest::TableUnit {
			table: table.clone(),
			unchecked,
		};
		let ask = Ask {
			request: &request,
			by: Requester {
				id: root,
				ir: &held.ir,
				facts: &held.facts,
			},
			over: held.planned_over.as_ref(),
			path: &[],
			session: &self.session,
			held: guards,
		};
		match self.planner.plan(ask).await? {
			Resolved::Table(reach) => Ok(reach),
			Resolved::Unit {
				..
			}
			| Resolved::Closure {
				..
			}
			| Resolved::Served(_)
			| Resolved::Linked(_) => {
				Err(NotCovered::internal("a request for a table unit answered another unit"))
			}
		}
	}
}

/// Widens each call of `program` to a user-defined function, and `program` itself, by what the
/// call's body units report in `reports` (access mode and row-order flag, by slot). Answers whether
/// anything widened.
fn widen_calls(program: &mut Program, reports: &[Option<(AccessMode, bool)>]) -> bool {
	let mut widened = false;
	let (mut access_mode, mut keeps_row_order) = (program.access_mode, program.keeps_row_order);
	for spec in &mut program.functions {
		for unit in [spec.body, spec.unchecked_body].into_iter().flatten() {
			let Some(Some((mode, ordered))) = reports.get(unit.0 as usize) else {
				continue;
			};
			let body = spec.body_access_mode.combine(*mode);
			widened |= body != spec.body_access_mode;
			spec.body_access_mode = body;
			access_mode = access_mode.combine(*mode);
			keeps_row_order |= *ordered;
		}
	}
	widened |= access_mode != program.access_mode || keeps_row_order != program.keeps_row_order;
	program.access_mode = access_mode;
	program.keeps_row_order = keeps_row_order;
	widened
}

/// The tables the unit `request` asks for is planned under the guard of: `inherited`, the
/// requester's, and the table of each write whose clause holds the request's site.
///
/// A site holds the written table where the lowering of the write holds it on the planner's
/// cycle guard: inside a `PermissionFrame` or `FieldFrame` region of a stage of a write that
/// names one table, which is a gate's predicate, a field's clause or permission, or a computed
/// field a stage derives. The site of a [`UnitRequest::Subquery`] or a [`UnitRequest::LoopBody`]
/// is its region, and the sites of a [`UnitRequest::ClosureBody`] are the `expr.closure_create`
/// ops of its digest. The events of a table are planned under `inherited` and that table. Every
/// other request has no site of its own and is planned under `inherited`. A write whose targets are
/// evaluated, or name more than one table, holds nothing.
///
/// A site under a clause of a write to a table already held, by the requester or by an enclosing
/// clause of the same site, is refused with the error text fixed for that case.
pub(crate) fn held_for(
	ir: &Ir,
	request: &UnitRequest,
	inherited: &Arc<[TableName]>,
) -> Result<Arc<[TableName]>, NotCovered> {
	let sites: Vec<RegionId> = match request {
		UnitRequest::Subquery {
			region,
		}
		| UnitRequest::LoopBody {
			region,
		} => vec![*region],
		UnitRequest::ClosureBody {
			digest,
			captures,
		} => {
			let mut sites = Vec::new();
			ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
				if let Some(data) = ir.op(op)
					&& let OpKind::Expr(ExprOp::ClosureCreate(attrs)) = &data.kind
					&& let Some(literal) = attrs.literal.0.as_ref()
					&& ClosureDigest::of(literal) == *digest
					&& captures_named(attrs, captures)
				{
					sites.push(ir.enclosing_region(op));
				}
				Walk::Continue
			});
			sites
		}
		UnitRequest::Events {
			table,
			..
		} => {
			// The table's events are lowered with the table held, as a compilation of its
			// permissions or computed fields holds it, so a clause that reads the table plans
			// nothing of it again.
			let mut held = inherited.to_vec();
			if !held.contains(table) {
				held.push(table.clone());
			}
			return Ok(held.into());
		}
		UnitRequest::FunctionBody {
			..
		}
		| UnitRequest::FunctionGuard {
			..
		}
		| UnitRequest::TableUnit {
			..
		}
		| UnitRequest::ViewRows {
			..
		}
		| UnitRequest::ViewRecalc {
			..
		}
		| UnitRequest::StageBundle {
			..
		}
		| UnitRequest::LinkedStages {
			..
		}
		| UnitRequest::Cascade {
			..
		}
		| UnitRequest::Subscription {
			..
		}
		| UnitRequest::ServedStatement {
			..
		} => return Ok(Arc::clone(inherited)),
	};
	// The subject loop of each write that names one table, with that table.
	let mut written: HashMap<OpId, TableName> = HashMap::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(data) = ir.op(op)
			&& let OpKind::Phys(PhysOp::Write(attrs)) = &data.kind
			&& let Some(table) = &attrs.write.stored.table
			&& let Some(looped) = write_loop(ir, data)
		{
			written.insert(looped, table.clone());
		}
		Walk::Continue
	});
	let mut held: Vec<TableName> = inherited.to_vec();
	for site in sites {
		let mut path: Vec<&TableName> = Vec::new();
		let mut region = site;
		while let Some(owner) = ir.parent_op(region) {
			let framed = matches!(
				ir.region(region).map(|data| data.kind),
				Some(RegionKind::PermissionFrame | RegionKind::FieldFrame(_))
			);
			let staged = matches!(
				ir.op(owner).map(|data| &data.kind),
				Some(OpKind::Phys(PhysOp::WriteStage { .. }))
			);
			let outer = ir.enclosing_region(owner);
			let table = match framed && staged {
				true => ir.parent_op(outer).and_then(|looped| written.get(&looped)),
				false => None,
			};
			if let Some(table) = table {
				if inherited.contains(table) || path.contains(&table) {
					return Err(NotCovered::unsupported(
						"a clause holding a statement is lowered under its own table",
					));
				}
				path.push(table);
			}
			region = outer;
		}
		for table in path {
			if !held.contains(table) {
				held.push(table.clone());
			}
		}
	}
	Ok(held.into())
}

/// The unit a request that names a region of the requesting container stands for, copied out of
/// that container: `None` for a request that names no region.
///
/// - [`UnitRequest::Subquery`]: the statement region, run rowless, as the unit of a statement in an
///   expression.
/// - [`UnitRequest::LoopBody`]: the body region of a recursion, run over its element as the row.
///   The region takes the arguments of a loop body that outputs: the element, then the depth's
///   position, which the body of a recursion never reads, so nothing binds it.
///
/// The copy holds the literals the region reads from outside it, and takes each other value
/// defined outside it as an input after the arguments named, which the requester's instruction
/// hands over ([`Planned::unit_inputs`]). A region that takes arguments other than those named
/// and its inputs is an invariant break. The copy keeps the requester's phase and the placement
/// of its pipelines, and starts with no facts.
pub(crate) fn region_unit(ask: &Ask<'_>) -> Result<Option<Resolved>, NotCovered> {
	let (region, args, sig) = match ask.request {
		UnitRequest::Subquery {
			region,
		} => (
			*region,
			0,
			UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
		),
		UnitRequest::LoopBody {
			region,
		} => (
			*region,
			loop_body_args(&LoopSink::Output),
			UnitSig {
				params: Vec::new(),
				takes_row: true,
			},
		),
		UnitRequest::FunctionBody {
			..
		}
		| UnitRequest::FunctionGuard {
			..
		}
		| UnitRequest::TableUnit {
			..
		}
		| UnitRequest::ViewRows {
			..
		}
		| UnitRequest::ViewRecalc {
			..
		}
		| UnitRequest::StageBundle {
			..
		}
		| UnitRequest::LinkedStages {
			..
		}
		| UnitRequest::Events {
			..
		}
		| UnitRequest::Cascade {
			..
		}
		| UnitRequest::ClosureBody {
			..
		}
		| UnitRequest::Subscription {
			..
		}
		| UnitRequest::ServedStatement {
			..
		} => return Ok(None),
	};
	let unit = ask.by.ir.copy_region(region)?;
	let inputs = u16::try_from(unit.inputs.len()).map_err(|_| IrError::Capacity("unit inputs"))?;
	let top = unit.ir.top();
	let held = unit.ir.region(top).ok_or(IrError::Stale("ir_compile: unit region"))?;
	let taken = held
		.blocks
		.first()
		.and_then(|block| unit.ir.block(*block))
		.map_or(0, |block| block.args.len());
	if taken != args + usize::from(inputs) {
		return Err(IrError::Invariant {
			what: "a unit region that takes other arguments than its request names and its inputs",
		}
		.into());
	}
	if matches!(ask.request, UnitRequest::LoopBody { .. }) && held.kind != RegionKind::Loop {
		return Err(IrError::Invariant {
			what: "a loop body unit copied from a region that is no loop body",
		}
		.into());
	}
	let mut planned = Planned::new(unit.ir, FactCache::new());
	planned.unit_inputs = inputs;
	Ok(Some(Resolved::Unit {
		planned: Box::new(planned),
		sig,
		scope: None,
	}))
}

/// The `AUTH LIMIT` the region a [`UnitRequest::Subquery`] or [`UnitRequest::LoopBody`] names
/// runs under, where the frame of the op that owns it carries one the requesting container's own
/// frame does not: the clause of a definition that caps the principal. The unit runs for the
/// principal that limit caps, so its planner answers the frame of that principal. `None` for any
/// other request and for a region that runs in the container's principal.
pub(crate) fn region_limit(ask: &Ask<'_>) -> Result<Option<crate::iam::AuthLimit>, NotCovered> {
	let region = match ask.request {
		UnitRequest::Subquery {
			region,
		}
		| UnitRequest::LoopBody {
			region,
		} => *region,
		UnitRequest::FunctionBody {
			..
		}
		| UnitRequest::FunctionGuard {
			..
		}
		| UnitRequest::TableUnit {
			..
		}
		| UnitRequest::ViewRows {
			..
		}
		| UnitRequest::ViewRecalc {
			..
		}
		| UnitRequest::StageBundle {
			..
		}
		| UnitRequest::LinkedStages {
			..
		}
		| UnitRequest::Events {
			..
		}
		| UnitRequest::Cascade {
			..
		}
		| UnitRequest::ClosureBody {
			..
		}
		| UnitRequest::Subscription {
			..
		}
		| UnitRequest::ServedStatement {
			..
		} => return Ok(None),
	};
	let ir = ask.by.ir;
	let owner = ir
		.parent_op(region)
		.and_then(|owner| ir.op(owner))
		.ok_or(IrError::Stale("ir_compile: the owner of a unit region"))?;
	let frame = ir.frame(owner.frame).ok_or(IrError::Stale("ir_compile: a unit region's frame"))?;
	let base =
		ir.frame(FrameId::new(0)).ok_or(IrError::Stale("ir_compile: the container's frame"))?;
	Ok(frame.eval.auth.clone().filter(|limit| base.eval.auth.as_ref() != Some(limit)))
}

/// Whether the closure creation `attrs` captures exactly `captures`, in order.
fn captures_named(attrs: &ClosureAttrs, captures: &[String]) -> bool {
	attrs.captures.len() == captures.len()
		&& attrs.captures.iter().zip(captures).all(|(held, named)| held.as_str() == named)
}

/// The body of the closure literal a [`UnitRequest::ClosureBody`] names, copied out of the
/// requesting container, with what the unit's head names: `None` for any other request.
/// `stamps` and `version` are the catalog stamps and the `VERSION` stamp of the transaction the
/// planner plans in.
///
/// The literal is the `expr.closure_create` whose literal has the request's digest and whose
/// captures are the request's. Every such op of the container must agree on the literal compared
/// whole and on whether its body reads the invoking row, because one request stands for all of
/// them and a run takes the unit only for a value whose closure, captures and rows its head
/// names. Where two disagree (two literals that print alike) the body is refused, not planned for
/// one of them. A container that makes no closure of the digest
/// is an internal error.
///
/// The body region is isolated: it reads no value of the requester beyond the literals copied in,
/// and takes the timestamp, the parameters, the captures and the captured rows as its arguments.
pub(crate) fn closure_unit(
	ask: &Ask<'_>,
	stamps: u64,
	version: Option<u64>,
) -> Result<Option<Resolved>, NotCovered> {
	let UnitRequest::ClosureBody {
		digest,
		captures,
	} = ask.request
	else {
		return Ok(None);
	};
	let ir = ask.by.ir;
	let mut made: Vec<(OpId, ClosureTemplate)> = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(data) = ir.op(op)
			&& let OpKind::Expr(ExprOp::ClosureCreate(attrs)) = &data.kind
			&& let Some(literal) = attrs.literal.0.as_ref()
			&& ClosureDigest::of(literal) == *digest
			&& captures_named(attrs, captures)
		{
			made.push((
				op,
				ClosureTemplate {
					closure: Arc::clone(literal),
					digest: *digest,
					captures: attrs.captures.iter().map(|name| name.as_str().to_owned()).collect(),
					row: attrs.reads_row,
					root: attrs.reads_root,
					stamps,
					version,
				},
			));
		}
		Walk::Continue
	});
	let Some((op, template)) = made.first() else {
		return Err(NotCovered::internal(
			"a closure body asked for by a container that makes no closure of its digest",
		));
	};
	let agree = made.iter().all(|(_, other)| {
		*other.closure == *template.closure
			&& other.captures == template.captures
			&& other.row == template.row
	});
	if !agree {
		return Err(NotCovered::unsupported(
			"closure literals of one container that share a digest and differ in what they are or capture",
		));
	}
	let region = ir
		.op(*op)
		.and_then(|data| data.regions.first().copied())
		.ok_or(IrError::Stale("ir_compile: closure body region"))?;
	let unit = ir.copy_region(region)?;
	if !unit.inputs.is_empty() {
		return Err(IrError::Invariant {
			what: "a closure body that reads a value defined outside it",
		}
		.into());
	}
	let top = unit.ir.region(unit.ir.top()).ok_or(IrError::Stale("ir_compile: unit region"))?;
	if top.kind != RegionKind::ClosureBody {
		return Err(IrError::Invariant {
			what: "a closure body unit copied from a region that is no closure body",
		}
		.into());
	}
	Ok(Some(Resolved::Closure {
		planned: Box::new(Planned::new(unit.ir, FactCache::new())),
		template: Box::new(template.clone()),
	}))
}

/// Whether `reach` names no table.
fn reaches_nothing(reach: &Reach) -> bool {
	matches!(reach, Reach::Tables(tables) if tables.is_empty())
}

/// The tables `reach` names, whether or not it may reach any other.
fn tables_named(reach: Reach) -> BTreeSet<TableName> {
	match reach {
		Reach::Tables(tables) | Reach::Any(tables) => tables,
	}
}

/// Whether a statement the VM declined for `not_covered` is planned again late-bound: after the
/// schema statements of its block, each catalog object it names is loaded and the session's names
/// are resolved again when it runs. `changes_the_catalog` is whether the statement may run a
/// schema statement, and `planned_late` whether this planning already was late-bound.
///
/// An internal refusal is an invariant the planning broke, and is never planned again.
pub(crate) fn binds_late(
	not_covered: &NotCovered,
	changes_the_catalog: bool,
	planned_late: bool,
) -> bool {
	not_covered.kind != NotCoveredKind::Internal
		&& !planned_late
		&& (not_covered.kind == NotCoveredKind::LateBind || changes_the_catalog)
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;
	use std::sync::Mutex;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::iface::ids::RegionId;
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::vm::ir_lower::{Lowered, UseOperand, UseOperands};
	use crate::exec::vm::program::{CallDepth, DefHash, FunctionSpec, SubquerySpec, UnitHead};
	use crate::exec::{AccessMode, ContextLevel};
	use crate::expr::logical::{LogicalSource, SourceRef};

	fn container() -> Planned {
		Planned::new(
			Ir::new(
				IrScope::Request {
					fragment: None,
				},
				RegionKind::Request,
			),
			FactCache::new(),
		)
	}

	fn function(name: &str) -> UnitRequest {
		UnitRequest::FunctionBody {
			name: name.to_owned(),
			unchecked: false,
			limit: None,
		}
	}

	fn subquery(region: u64) -> UnitRequest {
		UnitRequest::Subquery {
			region: RegionId::from(slotmap::KeyData::from_ffi(region)),
		}
	}

	fn sig() -> UnitSig {
		UnitSig {
			params: Vec::new(),
			takes_row: true,
		}
	}

	fn table(name: &str) -> TableName {
		TableName::from(name)
	}

	/// The events of a table are planned with the table held on top of what the requester holds,
	/// and held once where the requester holds it already.
	#[test]
	fn the_events_of_a_table_are_planned_under_the_table_held() {
		let events = |name: &str| UnitRequest::Events {
			table: table(name),
			action: crate::doc::Action::Create,
			frame: UnitFrame {
				class_digest: 0,
				ns: None,
				db: None,
				perms_suppressed: false,
				checked: true,
			},
		};
		let ir = container().ir;
		let none: Arc<[TableName]> = Arc::from(Vec::new());
		let held = held_for(&ir, &events("person"), &none).expect("the events are planned");
		assert_eq!(&*held, [table("person")]);
		let outer: Arc<[TableName]> = Arc::from(vec![table("audit")]);
		let held = held_for(&ir, &events("person"), &outer).expect("the events are planned");
		assert_eq!(&*held, [table("audit"), table("person")]);
		let held = held_for(&ir, &events("audit"), &outer).expect("the events are planned");
		assert_eq!(&*held, [table("audit")]);
	}

	/// Lowers every container to an empty program that asks for the requests scripted for its
	/// container, as the real lowering does where a construct runs code of its own. A program
	/// whose container has a reach reads records of those tables by id.
	#[derive(Default)]
	struct Script {
		asks: HashMap<u32, Vec<UnitRequest>>,
		reach: HashMap<u32, BTreeSet<TableName>>,
		/// What the program of a container reports of itself: its access mode, required context
		/// and row-order flag.
		reports: HashMap<u32, (AccessMode, ContextLevel, bool)>,
		/// Containers whose lowering fails, after it recorded its frame.
		fails: BTreeSet<u32>,
		lowered: Mutex<Vec<u32>>,
		/// The frame and depth each container was lowered under, in lowering order.
		frames: Mutex<Vec<(u32, UnitFrame, u32)>>,
	}

	impl Lower for &Script {
		fn lower(
			&self,
			cx: &mut LowerCx<'_>,
			_planned: &Planned,
			shape: Shape,
		) -> Result<Lowered, NotCovered> {
			let container = cx.container();
			self.lowered.lock().unwrap().push(container.0);
			self.frames.lock().unwrap().push((container.0, cx.frame().clone(), cx.depth()));
			if self.fails.contains(&container.0) {
				return Err(NotCovered::unsupported("a container the script refuses"));
			}
			let mut specs = Vec::new();
			let mut calls = Vec::new();
			for request in self.asks.get(&container.0).into_iter().flatten() {
				if request.takes_slot() {
					let unit = cx.unit_for(request.clone())?;
					// A program that asks for a subquery unit holds the spec that names it, with
					// nothing yet stated of what the unit takes.
					// A program that asks for a function body holds the call that names it, which
					// states nothing of what the body does.
					if let UnitRequest::FunctionBody {
						name,
						..
					} = request
					{
						calls.push(call_of(name, unit));
					}
					if matches!(request, UnitRequest::Subquery { .. }) {
						specs.push(SubquerySpec {
							unit,
							inherits_row: false,
							access_mode: AccessMode::ReadOnly,
							required_context: ContextLevel::Root,
							keeps_row_order: false,
							inputs: Vec::new(),
						});
					}
				} else {
					cx.need(request.clone())?;
				}
			}
			let mut lowered = super::super::ir_lower::empty_lowered(cx)?;
			lowered.program.subqueries = specs;
			lowered.program.functions = calls;
			if let Some((access_mode, required_context, keeps_row_order)) =
				self.reports.get(&container.0)
			{
				lowered.program.access_mode = *access_mode;
				lowered.program.required_context = *required_context;
				lowered.program.keeps_row_order = *keeps_row_order;
			}
			if let Some(tables) = self.reach.get(&container.0) {
				lowered.program.reach = Some(Reach::Tables(tables.clone()));
			}
			// A unit's program names the frame it was lowered for, as `lower_unit` stamps it.
			if let Shape::Unit(sig) = shape {
				lowered.program.unit = Some(UnitHead {
					sig,
					frame: cx.frame().clone(),
					entry: None,
					closure: None,
				});
			}
			Ok(lowered)
		}
	}

	/// The call of the user-defined function `name` whose body is the unit `body`, with nothing
	/// stated of what the body does.
	fn call_of(name: &str, body: UnitId) -> FunctionSpec {
		FunctionSpec {
			name: name.to_owned(),
			qualified: format!("fn::{name}"),
			plan_depth: 0,
			body_access_mode: AccessMode::ReadOnly,
			defined: true,
			per_call: false,
			def_hash: DefHash(0),
			frame: UnitFrame {
				class_digest: 0,
				ns: None,
				db: None,
				perms_suppressed: false,
				checked: true,
			},
			checked: false,
			body: Some(body),
			unchecked_body: None,
			guard: None,
			depth: CallDepth {
				site: 0,
				writes: false,
				body: 0,
				deepest: 0,
			},
			light: false,
		}
	}

	/// A planner answering from a table of requests, and counting what it was asked.
	#[derive(Default)]
	struct Answers {
		units: HashMap<UnitRequest, Option<NotCovered>>,
		served: HashMap<UnitRequest, Arc<LogicalOp>>,
		tables: HashMap<(TableName, bool), Option<BTreeSet<TableName>>>,
		/// The frame and depth a unit is answered with, where not the compile's.
		scopes: HashMap<UnitRequest, UnitScope>,
		asked: Mutex<Vec<UnitRequest>>,
		/// The path each request was asked under, in asking order.
		paths: Mutex<Vec<Vec<UnitRequest>>>,
		overlays: Mutex<Vec<bool>>,
	}

	impl UnitPlanner for &Answers {
		fn plan<'a>(&'a mut self, ask: Ask<'a>) -> BoxFut<'a, Result<Resolved, NotCovered>> {
			Box::pin(async move {
				self.asked.lock().unwrap().push(ask.request.clone());
				self.overlays.lock().unwrap().push(ask.over.is_some());
				self.paths.lock().unwrap().push(ask.path.to_vec());
				match ask.request {
					UnitRequest::TableUnit {
						table,
						unchecked,
					} => {
						let held = self.tables.get(&(table.clone(), *unchecked));
						Ok(Resolved::Table(held.and_then(|reach| reach.clone()).map(Reach::Tables)))
					}
					request @ (UnitRequest::FunctionBody {
						..
					}
					| UnitRequest::FunctionGuard {
						..
					}
					| UnitRequest::Subquery {
						..
					}
					| UnitRequest::ViewRows {
						..
					}
					| UnitRequest::ViewRecalc {
						..
					}
					| UnitRequest::StageBundle {
						..
					}
					| UnitRequest::LinkedStages {
						..
					}
					| UnitRequest::Events {
						..
					}
					| UnitRequest::Cascade {
						..
					}
					| UnitRequest::ClosureBody {
						..
					}
					| UnitRequest::LoopBody {
						..
					}
					| UnitRequest::Subscription {
						..
					}
					| UnitRequest::ServedStatement {
						..
					}) => {
						if let Some(op) = self.served.get(request) {
							return Ok(Resolved::Served(Arc::clone(op)));
						}
						match self.units.get(request) {
							Some(Some(refusal)) => Err(refusal.clone()),
							Some(None) => Ok(Resolved::Unit {
								planned: Box::new(container()),
								sig: sig(),
								scope: self.scopes.get(request).cloned(),
							}),
							None => Err(NotCovered::internal("a request nothing answers")),
						}
					}
				}
			})
		}
	}

	fn compiler<'a>(
		script: &'a Script,
		answers: &'a Answers,
	) -> Compiler<'static, &'a Answers, &'a Script> {
		Compiler::new(LowerCx::detached(), answers, session()).lowering(script)
	}

	fn run<T>(future: impl std::future::Future<Output = T>) -> T {
		tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.expect("a runtime")
			.block_on(future)
	}

	#[test]
	fn a_unit_that_needs_another_is_lowered_to_a_fixpoint() {
		// The root calls f, f calls g, and g calls f again: three lowerings and two units, the
		// last request answered by the slot it already holds.
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.asks.insert(1, vec![function("g")]);
		script.asks.insert(2, vec![function("f")]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(function("g"), None);
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		assert_eq!(compiled.program.units.len(), 2);
		assert_eq!(*script.lowered.lock().unwrap(), vec![0, 1, 2]);
		assert_eq!(*answers.asked.lock().unwrap(), vec![function("f"), function("g")]);
	}

	#[test]
	fn a_request_made_twice_is_planned_and_lowered_once() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f"), function("f")]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		assert_eq!(compiled.program.units.len(), 1);
		assert_eq!(answers.asked.lock().unwrap().len(), 1);
		assert_eq!(script.lowered.lock().unwrap().len(), 2);
	}

	#[test]
	fn what_the_planner_refuses_reaches_the_caller_and_leaves_the_compiler_as_it_was() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f"), function("g")]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(function("g"), Some(NotCovered::late_bind()));
		let mut compiler = compiler(&script, &answers);
		let refused = match run(compiler.program(container(), BTreeSet::new())) {
			Err(refused) => refused,
			Ok(_) => panic!("the program compiles"),
		};
		assert_eq!(refused, NotCovered::late_bind());
		assert_eq!(compiler.cx.slots(), 0);
		assert!(compiler.programs.is_empty() && compiler.held.is_empty());
	}

	#[test]
	fn a_program_is_closed_when_the_compile_returns() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.asks.insert(1, vec![function("g")]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(function("g"), None);
		let mut compiler = compiler(&script, &answers);
		let compiled = run(compiler.program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		let lowered = script.lowered.lock().unwrap().len();
		// Every slot the lowerings reserved holds a program, and nothing is pending.
		assert_eq!(compiled.program.units.len(), compiler.cx.slots());
		assert!(compiler.programs.iter().all(Option::is_some));
		assert_eq!(script.lowered.lock().unwrap().len(), lowered);
		// A second program over the same compiler is refused: its ids would index the first's
		// table.
		let again = run(compiler.program(container(), BTreeSet::new()));
		assert!(again.is_err());
	}

	#[test]
	fn a_served_statement_is_kept_as_written_at_its_position() {
		let op = Arc::new(LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(table("person")),
			schema: None,
			with: None,
			for_update: false,
		}));
		let served = |position| UnitRequest::ServedStatement {
			position,
			written: position,
		};
		let mut script = Script::default();
		script.asks.insert(0, vec![served(1), served(0)]);
		let mut answers = Answers::default();
		answers.served.insert(served(0), Arc::clone(&op));
		answers.served.insert(served(1), Arc::clone(&op));
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		assert_eq!(compiled.served.len(), 2);
		assert!(Arc::ptr_eq(&compiled.served[0], &op));
		// A unit serves none: a served statement it asks for is an internal error.
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.asks.insert(1, vec![served(0)]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.served.insert(served(0), op);
		let refused = run(compiler(&script, &answers).program(container(), BTreeSet::new()));
		assert!(matches!(
			refused,
			Err(NotCovered {
				kind: NotCoveredKind::Internal,
				..
			})
		));
	}

	#[test]
	fn the_dispatch_names_the_tables_a_program_reaches_and_those_their_units_reach() {
		let mut script = Script::default();
		script.reach.insert(0, BTreeSet::from([table("person"), table("ghost")]));
		let mut answers = Answers::default();
		answers.tables.insert((table("person"), false), Some(BTreeSet::from([table("city")])));
		answers.tables.insert((table("person"), true), Some(BTreeSet::from([table("country")])));
		answers.tables.insert((table("city"), false), Some(BTreeSet::new()));
		answers.tables.insert((table("city"), true), Some(BTreeSet::new()));
		answers.tables.insert((table("country"), false), Some(BTreeSet::new()));
		answers.tables.insert((table("country"), true), Some(BTreeSet::new()));
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		let dispatch = compiled.program.dispatch.as_ref().expect("the program is sealed");
		// `ghost` is held by no catalog and is no candidate.
		assert_eq!(dispatch.tables, vec![table("city"), table("country"), table("person")]);
		// A program that reads nothing by id holds no dispatch.
		let script = Script::default();
		let answers = Answers::default();
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		assert!(compiled.program.dispatch.is_none());
		assert!(answers.asked.lock().unwrap().is_empty());
	}

	#[test]
	fn the_statements_of_a_request_share_one_table() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.asks.insert(2, vec![function("f"), function("g")]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(function("g"), None);
		let mut compiler = compiler(&script, &answers);
		let first = run(compiler.statement(container(), &session()))
			.map_err(|e| e.reason)
			.expect("the statement compiles");
		let second = run(compiler.statement(container(), &session()))
			.map_err(|e| e.reason)
			.expect("the statement compiles");
		// A statement holds none of its units; the second reuses f's slot and adds g's.
		assert!(first.program.units.is_empty() && second.program.units.is_empty());
		let request = compiler
			.finish_request(
				vec![RequestStep::Run {
					statement: Some(0),
				}],
				vec![Arc::new(first.program), Arc::new(second.program)],
			)
			.map_err(|e| e.reason)
			.expect("the request finishes");
		assert_eq!(request.units.len(), 2);
		assert_eq!(request.request.as_ref().map(|code| code.statements.len()), Some(2));
		assert_eq!(answers.asked.lock().unwrap().len(), 2);
	}

	#[test]
	fn a_unit_is_planned_over_the_overlay_of_the_container_that_asked() {
		let overlay = Arc::new(Overlay::default());
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.asks.insert(1, vec![function("g")]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(function("g"), None);
		run(compiler(&script, &answers).program(container().over(Some(overlay)), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		// f was asked for by the root, planned over the overlay, and g by f, which inherits it.
		assert_eq!(*answers.overlays.lock().unwrap(), vec![true, true]);
	}

	/// What a subquery unit takes of its requester is written into the requester's spec and
	/// widens the requester's own program, from the last unit lowered to the first: the root takes
	/// what the unit it evaluates takes of the unit that unit evaluates.
	#[test]
	fn a_subquery_unit_gives_its_requester_what_only_its_lowering_knows() {
		let mut script = Script::default();
		script.asks.insert(0, vec![subquery(1), subquery(2)]);
		script.asks.insert(1, vec![subquery(3)]);
		script.reports.insert(2, (AccessMode::ReadWrite, ContextLevel::Namespace, false));
		script.reports.insert(3, (AccessMode::ReadOnly, ContextLevel::Database, true));
		let mut answers = Answers::default();
		for region in 1..=3 {
			answers.units.insert(subquery(region), None);
		}
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		// The root evaluates the units of regions 1 and 2, in that order.
		let program = &compiled.program;
		let [first, second] = &program.subqueries[..] else {
			panic!("the root holds one spec per unit it evaluates");
		};
		// Region 1's unit evaluates region 3's, which requires the database and keeps row order.
		assert_eq!(
			(first.access_mode, first.required_context, first.keeps_row_order),
			(AccessMode::ReadOnly, ContextLevel::Database, true)
		);
		// Region 2's unit writes and requires a namespace.
		assert_eq!(
			(second.access_mode, second.required_context, second.keeps_row_order),
			(AccessMode::ReadWrite, ContextLevel::Namespace, false)
		);
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
		assert_eq!(program.required_context, ContextLevel::Database);
		assert!(program.keeps_row_order);
		// The unit of region 1 took what its own unit takes, which is how the root learned of it.
		let unit = compiled.program.units[first.unit.0 as usize].as_ref();
		assert_eq!(unit.required_context, ContextLevel::Database);
		assert!(unit.keeps_row_order);
	}

	/// A recursion body gives its requester its access mode alone: the root that runs a body that
	/// writes writes, and takes neither the body's context level nor its row-order flag.
	#[test]
	fn a_recursion_body_gives_its_requester_its_access_mode() {
		let body = UnitRequest::LoopBody {
			region: RegionId::from(slotmap::KeyData::from_ffi(1)),
		};
		let mut script = Script::default();
		script.asks.insert(0, vec![body.clone()]);
		script.reports.insert(1, (AccessMode::ReadWrite, ContextLevel::Namespace, true));
		let mut answers = Answers::default();
		answers.units.insert(body, None);
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		assert_eq!(compiled.program.access_mode, AccessMode::ReadWrite);
		assert_eq!(compiled.program.required_context, ContextLevel::Root);
		assert!(!compiled.program.keeps_row_order);
	}

	/// Every call of a function takes what its body unit does, and what the bodies that body calls
	/// do: two bodies that call each other each end up writing when one of them writes.
	#[test]
	fn a_function_body_gives_every_call_of_it_what_it_does() {
		let mut script = Script::default();
		// The root calls f; f's body (container 1) calls g; g's body (container 2) calls f, which
		// is the unit already asked for, and writes, keeping row order.
		script.asks.insert(0, vec![function("f")]);
		script.asks.insert(1, vec![function("g")]);
		script.asks.insert(2, vec![function("f")]);
		script.reports.insert(2, (AccessMode::ReadWrite, ContextLevel::Database, true));
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(function("g"), None);
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		let program = &compiled.program;
		let [call] = &program.functions[..] else {
			panic!("the root holds the call of f");
		};
		assert_eq!(call.body_access_mode, AccessMode::ReadWrite, "f calls g, which writes");
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
		assert!(program.keeps_row_order);
		let f = program.units[call.body.expect("f has a body").0 as usize].as_ref();
		let [g_call] = &f.functions[..] else {
			panic!("f's body holds the call of g");
		};
		assert_eq!(g_call.body_access_mode, AccessMode::ReadWrite);
		assert_eq!(f.access_mode, AccessMode::ReadWrite);
		let g = program.units[g_call.body.expect("g has a body").0 as usize].as_ref();
		let [f_call] = &g.functions[..] else {
			panic!("g's body holds the call of f");
		};
		assert_eq!(f_call.body, call.body, "g calls the unit the root calls");
		assert_eq!(f_call.body_access_mode, AccessMode::ReadWrite, "f writes through g");
	}

	/// A program that evaluates no subquery keeps the context level its lowering reports: a
	/// function body gives its calls its access mode and row order, never its context level.
	#[test]
	fn a_program_without_a_subquery_unit_keeps_what_it_reports() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.reports.insert(0, (AccessMode::ReadOnly, ContextLevel::Namespace, false));
		script.reports.insert(1, (AccessMode::ReadWrite, ContextLevel::Database, true));
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		let program = &compiled.program;
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
		assert_eq!(program.required_context, ContextLevel::Namespace);
		assert!(program.keeps_row_order);
		let [call] = &program.functions[..] else {
			panic!("the root holds the call of f");
		};
		assert_eq!(call.body_access_mode, AccessMode::ReadWrite);
	}

	/// A frame of `class` that checks permissions, in no database.
	fn frame_of_class(class: u64) -> UnitFrame {
		UnitFrame {
			class_digest: class,
			ns: None,
			db: None,
			perms_suppressed: false,
			checked: true,
		}
	}

	/// A unit the planner answers with a frame and depth of its own is lowered under them, and the
	/// units lowered after it, which the planner answers with none, under the compile's.
	#[test]
	fn a_unit_is_lowered_for_the_frame_its_planner_answers() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f"), subquery(1)]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(subquery(1), None);
		let body = UnitScope {
			frame: frame_of_class(7),
			reads_checked: true,
			depth: 1,
			actor_suppressed: None,
			depth_sites: None,
		};
		answers.scopes.insert(function("f"), body.clone());
		let compiled = run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		let compile = LowerCx::detached().frame().clone();
		assert_eq!(
			*script.frames.lock().unwrap(),
			vec![(0, compile.clone(), 0), (1, body.frame.clone(), 1), (2, compile, 0)]
		);
		// The head of the body names the frame it was lowered for, which the run admits it by.
		let [call] = &compiled.program.functions[..] else {
			panic!("the root holds the call of f");
		};
		let head = compiled.program.units[call.body.expect("f has a body").0 as usize]
			.unit
			.clone()
			.expect("a unit has a head");
		assert_eq!(head.frame, body.frame);
	}

	/// A unit asked for by a unit lowered for a frame of its own, which the planner answers with
	/// none, is lowered for the frame of the unit that asked: a statement copied out of a function
	/// body runs inside the body.
	#[test]
	fn a_unit_inherits_the_frame_of_the_unit_that_asked_for_it() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.asks.insert(1, vec![subquery(4)]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(subquery(4), None);
		let body = UnitScope {
			frame: frame_of_class(7),
			reads_checked: true,
			depth: 1,
			actor_suppressed: None,
			depth_sites: None,
		};
		answers.scopes.insert(function("f"), body.clone());
		run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		let compile = LowerCx::detached().frame().clone();
		assert_eq!(
			*script.frames.lock().unwrap(),
			vec![(0, compile, 0), (1, body.frame.clone(), 1), (2, body.frame, 1)]
		);
	}

	/// A unit whose lowering under a frame of its own fails leaves the compile's frame in place:
	/// what the compiler lowers next is lowered for the compile.
	#[test]
	fn a_failed_lowering_under_a_unit_frame_restores_the_compile_frame() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.fails.insert(1);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.scopes.insert(
			function("f"),
			UnitScope {
				frame: frame_of_class(7),
				reads_checked: true,
				depth: 3,
				actor_suppressed: None,
				depth_sites: None,
			},
		);
		let mut compiler = compiler(&script, &answers);
		assert!(run(compiler.program(container(), BTreeSet::new())).is_err());
		assert_eq!(compiler.cx.frame(), LowerCx::detached().frame());
		assert_eq!(compiler.cx.depth(), 0);
	}

	/// The planner is told the requests each requesting container descends from.
	#[test]
	fn a_request_is_asked_under_the_requests_its_container_descends_from() {
		let mut script = Script::default();
		script.asks.insert(0, vec![function("f")]);
		script.asks.insert(1, vec![subquery(4)]);
		let mut answers = Answers::default();
		answers.units.insert(function("f"), None);
		answers.units.insert(subquery(4), None);
		run(compiler(&script, &answers).program(container(), BTreeSet::new()))
			.map_err(|e| e.reason)
			.expect("the program compiles");
		assert_eq!(
			*answers.asked.lock().unwrap(),
			vec![function("f"), subquery(4)],
			"the root asks for f, whose body asks for a subquery"
		);
		assert_eq!(*answers.paths.lock().unwrap(), vec![vec![], vec![function("f")]]);
	}

	#[test]
	fn late_binding_follows_the_kind_of_the_refusal() {
		let unsupported = NotCovered::unsupported("a construct");
		let internal = NotCovered::internal("an invariant");
		let late = NotCovered::late_bind();
		// A late-bind answer is planned again, once.
		assert!(binds_late(&late, false, false));
		assert!(!binds_late(&late, false, true));
		// A refusal is planned again only for a statement that may change the catalog.
		assert!(!binds_late(&unsupported, false, false));
		assert!(binds_late(&unsupported, true, false));
		assert!(!binds_late(&unsupported, true, true));
		// An internal error never is.
		assert!(!binds_late(&internal, true, false));
	}

	fn definition(
		table: &str,
		name: &str,
		cols: &[&str],
		kind: crate::catalog::Index,
	) -> crate::catalog::IndexDefinition {
		use std::str::FromStr;

		use crate::catalog::{IndexDefinition, IndexId};
		use crate::expr::Idiom;
		IndexDefinition {
			index_id: IndexId(1),
			name: Strand::from(name),
			table_name: table.into(),
			cols: cols.iter().map(|col| Idiom::from_str(col).expect("a valid idiom")).collect(),
			index: kind,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
			nulls_not_distinct: false,
		}
	}

	fn indexed(table: &str, name: &str, cols: &[&str]) -> TableAnswers {
		TableAnswers::plain(table.into(), true).with_indexes(vec![definition(
			table,
			name,
			cols,
			crate::catalog::Index::Idx,
		)])
	}

	fn full_text(table: &str, name: &str, cols: &[&str]) -> crate::catalog::IndexDefinition {
		use crate::catalog::{FullTextParams, Index, Scoring};
		definition(
			table,
			name,
			cols,
			Index::FullText(FullTextParams {
				analyzer: "simple".into(),
				highlight: false,
				scoring: Scoring::Bm {
					k1: 1.2,
					b: 0.75,
				},
			}),
		)
	}

	/// The reference an op carries to the index `name` of `table`, at `position` among the indexes
	/// its read may use.
	fn reference_of(table: &str, name: &str, position: u32) -> IndexRef {
		IndexRef {
			table: table.into(),
			index: Strand::from(name),
			position,
			scalar: Default::default(),
		}
	}

	#[test]
	fn an_index_is_resolved_once_a_container_that_opens_it_is_planned() {
		let indexes = SharedIndexes::new();
		assert!(indexes.resolve(&reference_of("t", "by_a", 0)).is_none());
		let answers = indexed("t", "by_a", &["a"]);
		indexes
			.learn(&answers, crate::exec::ir::analysis::cx::builtin_registry())
			.expect("the index is learned");
		// The same container planned twice agrees with itself.
		indexes
			.learn(&answers, crate::exec::ir::analysis::cx::builtin_registry())
			.expect("the same reference is learned again");
		let resolved = indexes.resolve(&reference_of("t", "by_a", 0)).expect("the index resolves");
		assert_eq!(resolved.definition().name.as_str(), "by_a");
		assert!(indexes.resolve(&reference_of("t", "by_b", 0)).is_none());
		assert!(indexes.resolve(&reference_of("u", "by_a", 0)).is_none());
		assert!(indexes.resolve(&reference_of("t", "by_a", 1)).is_none());
	}

	#[test]
	fn two_containers_that_resolve_an_index_differently_are_a_late_bind() {
		let indexes = SharedIndexes::new();
		indexes
			.learn(&indexed("t", "by_a", &["a"]), crate::exec::ir::analysis::cx::builtin_registry())
			.expect("the index is learned");
		let conflict = indexes
			.learn(&indexed("t", "by_a", &["b"]), crate::exec::ir::analysis::cx::builtin_registry())
			.expect_err("another definition under the same reference cannot share the resolver");
		assert_eq!(conflict.kind, NotCoveredKind::LateBind);
		// What was learned first still resolves.
		let resolved = indexes.resolve(&reference_of("t", "by_a", 0)).expect("the index resolves");
		assert_eq!(resolved.definition().cols.len(), 1);
	}

	/// A versioned read admits only the full-text indexes, so the same index sits at another
	/// position in its answers than in a current read's. Both reads of one request resolve.
	#[test]
	fn a_versioned_read_resolves_beside_the_current_read_of_its_table() {
		use crate::catalog::Index;
		use crate::exec::ir::phys::access::ReadVersion;
		let definitions = || {
			vec![definition("t", "by_a", &["a"], Index::Idx), full_text("t", "search", &["body"])]
		};
		let current = TableAnswers::plain("t".into(), true).with_indexes(definitions());
		let versioned = TableAnswers {
			version: ReadVersion::Deferred,
			..TableAnswers::plain("t".into(), true)
		}
		.with_indexes(definitions());
		let indexes = SharedIndexes::new();
		indexes
			.learn(&current, crate::exec::ir::analysis::cx::builtin_registry())
			.expect("the current read is learned");
		indexes
			.learn(&versioned, crate::exec::ir::analysis::cx::builtin_registry())
			.expect("the versioned read is learned beside it");
		let now = indexes.resolve(&reference_of("t", "search", 1)).expect("the current read");
		let then = indexes.resolve(&reference_of("t", "search", 0)).expect("the versioned read");
		assert_eq!((now.idx, then.idx), (1, 0));
		// The versioned read admits no b-tree index.
		assert!(indexes.resolve(&reference_of("t", "by_a", 0)).is_some());
		assert!(indexes.resolve(&reference_of("t", "by_a", 1)).is_none());
	}

	#[test]
	fn a_compile_over_an_environment_lowers_with_its_indexes_and_limits() {
		let indexes = SharedIndexes::new();
		indexes
			.learn(&indexed("t", "by_a", &["a"]), crate::exec::ir::analysis::cx::builtin_registry())
			.expect("the index is learned");
		let config = ExecConfig {
			generation_allocation_limit: 7,
			..ExecConfig::default()
		};
		let env = CompileEnv {
			registry: crate::exec::ir::analysis::cx::builtin_registry(),
			capabilities: Arc::new(Capabilities::default()),
			frame: UnitFrame::unprincipled(),
			reads_checked: true,
			depth: 3,
			config: &config,
			indexes: &indexes,
			compiled: None,
			session: session(),
			redact_volatile: false,
			surface: &crate::exec::surface::FULL,
		};
		let cx = env.context();
		assert_eq!(cx.limits(), LowerLimits::of(&config));
		assert_eq!(cx.limits().generation_allocation_limit, 7);
		assert_eq!(cx.depth(), 3);
		assert!(cx.indexes().resolve(&reference_of("t", "by_a", 0)).is_some());
		assert!(cx.indexes().resolve(&reference_of("t", "other", 0)).is_none());
	}
	fn session() -> PlanSession {
		PlanSession::new(Options::new(&ExecConfig::default()), Arc::new(Value::None))
	}

	fn use_slot(operands: UseOperands, computed: bool) -> Slot {
		let written = |operand: &UseOperand| {
			if let UseOperand::Written(name) = operand
				&& !computed
			{
				Some(name.clone())
			} else {
				None
			}
		};
		Slot::Use {
			ns: written(&operands.ns),
			db: written(&operands.db),
			computed,
			operands,
			statement: container().ir.top(),
		}
	}

	#[test]
	fn an_option_sets_import_and_force_and_nothing_else() {
		let mut session = session();
		session.option("import", true);
		assert!(session.opt.import);
		session.option("FORCE", true);
		assert!(matches!(session.opt.force, Force::All));
		session.option("FORCE", false);
		assert!(matches!(session.opt.force, Force::None));
		session.option("OTHER", true);
		assert!(session.opt.import);
	}

	#[test]
	fn a_use_of_written_names_selects_them_for_the_statements_after() {
		let mut session = session();
		let slot = use_slot(
			UseOperands {
				ns: UseOperand::Written("a".to_owned()),
				db: UseOperand::Written("b".to_owned()),
			},
			false,
		);
		assert!(session.after(&slot, None));
		assert_eq!(session.opt.ns.as_deref(), Some("a"));
		assert_eq!(session.opt.db.as_deref(), Some("b"));
		let selected = session.session.pick(crate::expr::paths::NS.as_ref());
		assert_eq!(selected, Value::String("a".into()));
	}

	#[test]
	fn a_use_of_a_parameter_the_session_holds_is_stated_and_any_other_is_not() {
		let mut session = session();
		let slot = Slot::Let {
			name: "target".to_owned(),
			kind: None,
			statement: container().ir.top(),
		};
		assert!(session.after(&slot, Some(Value::String("a".into()))));
		let by_param = use_slot(
			UseOperands {
				ns: UseOperand::Param("target".to_owned()),
				db: UseOperand::Absent,
			},
			true,
		);
		assert!(session.after(&by_param, None));
		assert_eq!(session.opt.ns.as_deref(), Some("a"));
		// A parameter the session does not hold, an expression and `USE DEFAULT` are not stated,
		// and leave the session as it was.
		for unstated in [
			use_slot(
				UseOperands {
					ns: UseOperand::Param("missing".to_owned()),
					db: UseOperand::Absent,
				},
				true,
			),
			use_slot(
				UseOperands {
					ns: UseOperand::Computed,
					db: UseOperand::Absent,
				},
				true,
			),
			use_slot(
				UseOperands {
					ns: UseOperand::Absent,
					db: UseOperand::Absent,
				},
				true,
			),
		] {
			let mut before = session.clone();
			assert!(!before.after(&unstated, None));
			assert_eq!(before.opt.ns, session.opt.ns);
		}
	}

	#[test]
	fn a_let_binds_a_constant_or_leaves_the_name_computed() {
		let mut session = session();
		session.bind_let("a", Some(Value::from(1_i64)));
		assert!(session.computed.is_empty());
		session.bind_let("a", None);
		assert!(session.computed.contains("a"));
		assert!(session.bound.is_empty());
		session.bind_let("a", Some(Value::from(2_i64)));
		assert!(session.computed.is_empty());
		assert_eq!(session.bound.len(), 1);
	}

	/// The context statements are bound in is built once per request and kept up to date in
	/// place: a `LET` of a constant or a `USE` binds one value in it, a `LET` the run computes
	/// changes nothing, and none of them copies the variables the request holds. It always reads
	/// as a fresh build would.
	#[tokio::test]
	async fn the_bound_context_is_built_once_and_kept_up_to_date() {
		use crate::dbs::Variables;

		let ds = crate::kvs::Datastore::new("memory").await.expect("a datastore");
		let txn = Arc::new(
			ds.transaction(crate::kvs::TransactionType::Read).await.expect("a transaction"),
		);
		let mut request = Context::new_test();
		let mut vars = Variables::new();
		for i in 0..1000_i64 {
			vars.insert(format!("v{i}"), Value::from(i));
		}
		request.attach_variables(vars).expect("variables attach");
		let request = request.freeze();
		let mut session = session();

		let first = session.bound_context(&request, &txn);
		let values = first.values_addr();
		drop(first);
		session.bind_let("c", None);
		session.bind_let("k", Some(Value::from(5_i64)));
		session.use_names(Some("ns"), Some("db"));
		session.bind_let("k", Some(Value::from(6_i64)));
		let kept = session.bound_context(&request, &txn);
		assert_eq!(kept.values_addr(), values, "a binding copied the request's variables");
		assert_eq!(kept.value("k"), Some(&Value::from(6_i64)));
		assert_eq!(kept.value("c"), None);
		assert_eq!(kept.value("v999"), Some(&Value::from(999_i64)));
		let selected = kept.value("session").expect("the session is bound");
		assert_eq!(selected.pick(crate::expr::paths::NS.as_ref()), Value::from("ns"));

		// Held elsewhere, the context is not edited under its holder: the next one is built
		// afresh, and reads as the kept one would.
		session.bind_let("h", Some(Value::from(1_i64)));
		assert_eq!(kept.value("h"), None, "a binding reached a context someone holds");
		let fresh = session.bound_context(&request, &txn);
		assert!(!Arc::ptr_eq(&fresh, &kept));
		assert_eq!(fresh.value("h"), Some(&Value::from(1_i64)));
		assert_eq!(fresh.value("k"), Some(&Value::from(6_i64)));
		drop(kept);

		// A constant that becomes computed leaves the name to what the request binds.
		drop(fresh);
		session.bind_let("k", None);
		assert_eq!(session.bound_context(&request, &txn).value("k"), None);

		// Another transaction is another context.
		let other = Arc::new(
			ds.transaction(crate::kvs::TransactionType::Read).await.expect("a transaction"),
		);
		let held = session.bound_context(&request, &txn);
		assert!(!Arc::ptr_eq(&session.bound_context(&request, &other), &held));
	}
}
