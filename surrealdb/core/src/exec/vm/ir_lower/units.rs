//! The lowering context, the units a lowering asks for, and the entries that lower a container to
//! a program.
//!
//! A construct that runs code of its own (a function body, a subquery, a stage bundle, a
//! cascade, a closure body, a subscription) lowers to an instruction naming a [`UnitId`] of the
//! program's unit table. Lowering never plans the code behind the id: [`LowerCx::unit_for`]
//! interns a [`UnitRequest`], answers the id it holds in the table, and reports the request as a
//! [`Needed`] of the lowering's [`Lowered`]. The compile driver (`vm::ir_compile`) plans each
//! request, lowers the unit it answers into that slot, and stops when no lowering has a request
//! left.
//!
//! # Interning
//!
//! A request is interned once per compile. Two requests that are equal are one unit, except that
//! a request that names a position inside the container that asked for it (a region, a served
//! statement, a closure body) is keyed by that container as well: region 3 of one container is
//! not region 3 of another. The table never shrinks within a compile, and a request that fails to
//! plan is taken back with [`LowerCx::rewind`], which restores the table to a [`Mark`].
//!
//! # What the context holds
//!
//! The function registry, the capabilities, the compile cache handle, the frame the units are
//! lowered for, the re-entry depth and the instruction-set version, and the table of interned
//! requests. It holds no planner and no catalog: a decision that needs either is an attribute of
//! the container, or the planning callback's.

use std::collections::HashMap;

use surrealdb_strand::TableName;

use super::super::program::{
	ClosureDigest, HostScalar, ISA_VERSION, Program, RowBits, UnitFrame, UnitHead, UnitId, UnitSig,
};
use super::super::units::CompiledUnits;
use super::ddl::Reservations;
use super::emit::Emitter;
#[cfg(test)]
use super::explain_hook::FULL_EXPLAIN_LOWERING;
use super::explain_hook::{ExplainLowering, Explained, Purpose, explained_served};
use super::expr::{LowerLimits, Needs};
use super::info::InfoLowered;
use super::pipe::{self, PoolBase, ReadScope};
use super::place::Lowered;
use super::read::{self, IndexResolver, NoIndexes};
#[cfg(test)]
use super::schema::FULL_SCHEMA_LOWERING;
use super::schema::{SchemaLowering, served};
use super::stage::StatementNesting;
use super::subscription::{self, LoweredLive};
use super::write_stmt::{self, LoweredWrite};
use super::{dispatch, events};
use crate::doc::Action;
use crate::exec::function::FunctionRegistry;
#[cfg(test)]
use crate::exec::ir::analysis::cx::builtin_registry;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::vm::NotCovered;

/// The identity of one container within one compile: the order in which the compile driver took
/// the containers it lowers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ContainerId(pub(crate) u32);

/// A statement a deletion performs on its own behalf, as a unit of its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum CascadeKind {
	/// The edges joined to a deleted record.
	Edges,
	/// Deleting a record of `table` that references the deleted record.
	Delete {
		table: TableName,
	},
	/// Reading a record of `table`, as a repair sees it.
	Read {
		table: TableName,
	},
	/// Unsetting `field` of a record of `table` whose `field` holds the deleted record.
	Unset {
		table: TableName,
		field: crate::expr::Idiom,
	},
	/// Subtracting the deleted record from `field` of a record of `table` that holds it among a
	/// collection.
	Subtract {
		table: TableName,
		field: crate::expr::Idiom,
	},
	/// Adding a referencing record to the list `dest` of a record of `table`, or taking it out
	/// (`MATERIALIZE INTO table.dest`).
	MaterializeWrite {
		table: TableName,
		dest: crate::expr::Idiom,
	},
	/// Judging the `WHERE` condition of the `MATERIALIZE INTO` clause of `field` of `table`, the
	/// referencing table, on an image of one of its records.
	MaterializeFilter {
		table: TableName,
		field: crate::expr::Idiom,
	},
}

/// What a lowering asks the compile driver to plan and lower into the program being compiled.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum UnitRequest {
	/// The body of the user-defined function `name`, without `fn::`, bound for the caller's
	/// principal capped by the definition's `AUTH LIMIT`, in a frame that checks that principal's
	/// permissions, or that waives them where `unchecked`.
	FunctionBody {
		name: String,
		unchecked: bool,
		/// The `AUTH LIMIT` the call runs under, where the frame of the call carries one the
		/// requesting container's own frame does not: a clause of a definition that caps the
		/// principal. The body is then bound for the caller's principal capped by that limit
		/// first.
		limit: Option<crate::iam::AuthLimit>,
	},
	/// The `PERMISSIONS` predicate of the user-defined function `name`, without `fn::`, from the
	/// definition the [`FunctionBody`](Self::FunctionBody) units of the compile are planned from.
	/// Pipeline `0` of the unit evaluates the predicate once, over the row it is handed, and emits
	/// its value. The predicate is definer code: it is planned for the caller's principal capped by
	/// the definition's `AUTH LIMIT`, one re-entry deeper than the call, over an isolated view of
	/// its context that holds no name the caller binds (a statement it holds reads the session's
	/// own values and the database's parameters alone), and lowered for the frame a permission
	/// predicate is judged in, whose reads are not checked and which refuses writes. The run admits
	/// the unit with `admitted_unit` in `permission_frame(limited, GuardKind::Read, None)` as the
	/// call enters (`vm::function::enter`), where `limited` holds the narrowed principal and skips
	/// permission checks: its `UnitHead` frame must satisfy `frame_admits` there.
	FunctionGuard {
		name: String,
		/// The `AUTH LIMIT` the call runs under, as [`Self::FunctionBody`] states it.
		limit: Option<crate::iam::AuthLimit>,
	},
	/// The statement region `region` of the requesting container, run as a unit of its own.
	Subquery {
		region: RegionId,
	},
	/// The read unit of `table` that the dispatch over reads of records by id loads by name,
	/// for a caller whose reads are checked unless `unchecked`. It is held by the compile cache,
	/// not by the program's unit table, and only the driver asks for it.
	TableUnit {
		table: TableName,
		unchecked: bool,
	},
	/// The order of the view table `table`'s rows: the view-row write of the table, one pipeline
	/// per action a view's maintenance settles (create, update and delete, in that order), each
	/// answering whether the order kept the row. The backfill of a view runs its create pipeline
	/// over each row it stores.
	ViewRows {
		table: TableName,
	},
	/// The recalculation of the aggregated view table `view`: the read a view's maintenance runs
	/// when a group loses the row that held one of its extremes. It reads the view's sources,
	/// narrowed by the view's condition to the group whose key values are the names
	/// `__view_group_<n>` of the frame it runs in, and yields one row, `[extremes…, count]`, the
	/// extremes in the order of the view's aggregations
	/// (`operators::write::stages::view::recalculation_of`). It is planned for the view frame,
	/// whose enforcement is suppressed.
	ViewRecalc {
		view: TableName,
	},
	/// The stages a write that may reach any table applies to the records of `table` for
	/// `action`.
	StageBundle {
		table: TableName,
		action: Action,
	},
	/// The bundle of the stages `set` a write whose stages are linked applies to the records of
	/// `table`, for the frame the requesting container is lowered for and on behalf of `actor`:
	/// served from the compile cache under the table's catalog version, or compiled on a miss,
	/// before the container is lowered. It holds no slot: the write's stage tables hold the bundle.
	LinkedStages {
		table: TableName,
		set: std::sync::Arc<crate::exec::vm::units::StageSetUnit>,
		/// The enforcement of the actor the requesting container runs on behalf of, where it
		/// differs from its frame's ([`LowerCx::actor`]).
		actor: Option<bool>,
	},
	/// The events of `table` that fire for `action`: the `WHEN` and `THEN` clauses of each event,
	/// as host pipelines over the image the event is about. The unit's head names `frame`, the
	/// frame of the write that fires the events, which the run admits the unit against, so the
	/// events of one table for one action are one unit for each frame that fires them.
	Events {
		table: TableName,
		action: Action,
		frame: UnitFrame,
	},
	/// A statement a deletion performs on its own behalf, lowered for a frame that suppresses
	/// permission enforcement where `suppressed`, and whose actor's own enforcement is suppressed
	/// where `caller`.
	Cascade {
		kind: CascadeKind,
		suppressed: bool,
		caller: bool,
	},
	/// The body of the closure literals of the requesting container whose digest is `digest` and
	/// that capture `captures`, in the order their bodies read them: one literal written where
	/// different names are bound captures differently, and each capture shape is a unit of its own.
	ClosureBody {
		digest: ClosureDigest,
		captures: Vec<String>,
	},
	/// The body region `region` of a loop of the requesting container, run as a unit over the one
	/// value it is given as its element: the body of a recursion.
	LoopBody {
		region: RegionId,
	},
	/// The unit that runs a live query's `WHERE`, projection and permissions for records of
	/// `table`.
	Subscription {
		table: TableName,
	},
	/// The late-bound statement at index `position` of the requesting program's `served` list,
	/// which the program plans when it reaches it: the statement as written that its container's
	/// statement op names by `written`. It holds no slot of the unit table.
	ServedStatement {
		position: u32,
		written: u32,
	},
}

impl UnitRequest {
	/// Whether the request names a position inside the container that asked for it, so that two
	/// equal requests of different containers are different requests.
	pub(crate) fn is_relative(&self) -> bool {
		match self {
			UnitRequest::Subquery {
				..
			}
			| UnitRequest::ClosureBody {
				..
			}
			| UnitRequest::LoopBody {
				..
			}
			| UnitRequest::ServedStatement {
				..
			} => true,
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
			| UnitRequest::Subscription {
				..
			} => false,
		}
	}

	/// Whether the request holds a slot of the program's unit table.
	pub(crate) fn takes_slot(&self) -> bool {
		match self {
			UnitRequest::FunctionBody {
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
			} => true,
			UnitRequest::TableUnit {
				..
			}
			| UnitRequest::LinkedStages {
				..
			}
			| UnitRequest::ServedStatement {
				..
			} => false,
		}
	}
}

/// A request a lowering made for the first time, with the slot of the unit table that holds its
/// unit: `Some` exactly when [`UnitRequest::takes_slot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Needed {
	pub(crate) request: UnitRequest,
	pub(crate) id: Option<UnitId>,
}

/// The state of a [`LowerCx`] at a point, to restore with [`LowerCx::rewind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Mark {
	slots: usize,
}

impl Mark {
	/// How many slots the unit table held at the mark.
	pub(crate) fn slots(self) -> usize {
		self.slots
	}
}

/// The identity a request is interned under.
/// It holds three parts: the container the request names a position of, when it is relative; the
/// frame of the code that makes it, when it is a call of a function, whose body and guard are
/// bound for the principal of that code; and the request itself.
type Key = (Option<ContainerId>, Option<UnitFrame>, UnitRequest);

/// What lowering may read, and the table of the units it has asked for.
pub(crate) struct LowerCx<'a> {
	registry: &'a FunctionRegistry,
	capabilities: std::sync::Arc<crate::dbs::Capabilities>,
	compiled: Option<&'a CompiledUnits>,
	/// The resolved reference of each index a source opens, as planning resolved it.
	indexes: &'a dyn IndexResolver,
	/// The request's configuration limits that instructions enforce.
	limits: LowerLimits,
	/// Whether the rendering of `EXPLAIN ANALYZE` leaves out elapsed time and batch counts.
	redact_volatile: bool,
	/// Whether the program being lowered serves the late-bound statements of its blocks when it
	/// reaches them.
	serves_late: bool,
	/// Whether the container being lowered is a unit of an enclosing program rather than a
	/// statement of a request.
	unit: bool,
	frame: UnitFrame,
	/// Whether the principal the units are lowered for has its reads permission-checked, which
	/// decides the permission views and guards lowering builds. It is not part of the frame a
	/// unit admits: the frame's principal class, names and suppression already fix it.
	reads_checked: bool,
	depth: u32,
	/// What the stored code being lowered spends of the computation-depth budget where it is
	/// evaluated one expression at a time ([`crate::exec::vm::depth`]): an event clause or a
	/// function body. `None` for a statement the session runs.
	depth_sites: Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>>,
	/// Where the container being lowered stands below the statement the session runs: nowhere for
	/// a statement of the request or a unit of its own place (a body, an event's clauses), and
	/// where the region it is copied from stands for the unit of a statement in an expression.
	nesting: StatementNesting,
	/// The enforcement of the actor the container being lowered runs on behalf of, where it
	/// differs from the frame's ([`LowerCx::actor_suppressed`]).
	actor_suppressed: Option<bool>,
	/// How many trailing arguments of the top block of the container being lowered are the
	/// values the program that runs it hands it ([`LowerCx::with_unit_inputs`]).
	unit_inputs: u16,
	/// The container being lowered, which relative requests are keyed by.
	container: ContainerId,
	/// The lowering of the statements that are not data statements, or `None` where the entry
	/// serves data statements alone. Every unit lowered under this context (a function body, a
	/// closure, a subquery, an event) is lowered with the same hook, so stored code is refused
	/// a statement the entry does not serve.
	schema: Option<&'static (dyn SchemaLowering + Sync)>,
	/// The lowering of `EXPLAIN`, or `None` where the entry does not serve it. Every unit lowered
	/// under this context is lowered with the same hook, so stored code is refused an `EXPLAIN`
	/// the entry does not serve.
	explain: Option<&'static (dyn ExplainLowering + Sync)>,
	/// The key of each slot of the unit table, in slot order.
	slots: Vec<Key>,
	/// For each slot, in slot order, the measure of the stored code being lowered when the slot
	/// was asked for ([`LowerCx::requested_sites`]).
	slot_sites: Vec<Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>>>,
	/// For each slot, in slot order, where the region the slot's unit is copied from stands when
	/// it was asked for ([`LowerCx::requested_nesting`]).
	slot_nesting: Vec<StatementNesting>,
	/// The slot of each interned request that holds one.
	index: HashMap<Key, UnitId>,
	/// The requests interned since the last [`LowerCx::take_needed`].
	fresh: Vec<Needed>,
	/// The requests interned that hold no slot, which dedupe as slotted ones do.
	slotless: std::collections::HashSet<Key>,
	/// The bundle of the stages of each write whose stages are linked, by its table and the stages
	/// its sites run, resolved by the compile driver before the container that holds the write is
	/// lowered ([`LowerCx::link_stages`]).
	linked: HashMap<(TableName, crate::exec::vm::units::StageSetUnit), std::sync::Arc<Program>>,
}

impl<'a> LowerCx<'a> {
	/// A context lowering for `frame` at re-entry depth `depth`, with an empty unit table.
	pub(crate) fn new(
		registry: &'a FunctionRegistry,
		capabilities: std::sync::Arc<crate::dbs::Capabilities>,
		frame: UnitFrame,
		depth: u32,
	) -> Self {
		Self {
			registry,
			capabilities,
			compiled: None,
			indexes: &NoIndexes,
			limits: LowerLimits::default(),
			redact_volatile: false,
			serves_late: false,
			unit: false,
			frame,
			reads_checked: true,
			depth,
			depth_sites: None,
			nesting: StatementNesting::default(),
			actor_suppressed: None,
			unit_inputs: 0,
			container: ContainerId(0),
			schema: None,
			explain: None,
			slots: Vec::new(),
			slot_sites: Vec::new(),
			slot_nesting: Vec::new(),
			index: HashMap::new(),
			fresh: Vec::new(),
			slotless: std::collections::HashSet::new(),
			linked: HashMap::new(),
		}
	}

	/// A context over the full builtin registry and the default capabilities, lowering for a frame
	/// that assumes permissions apply and serves any principal, and every statement family.
	#[cfg(test)]
	pub(crate) fn detached() -> LowerCx<'static> {
		LowerCx::detached_over(builtin_registry())
	}

	/// [`Self::detached`], resolving builtins against `registry`.
	#[cfg(test)]
	pub(crate) fn detached_over(registry: &'a FunctionRegistry) -> Self {
		LowerCx::new(
			registry,
			std::sync::Arc::new(crate::dbs::Capabilities::default()),
			UnitFrame {
				class_digest: 0,
				ns: None,
				db: None,
				perms_suppressed: false,
				checked: true,
			},
			0,
		)
		.with_schema(Some(&FULL_SCHEMA_LOWERING))
		.with_explain(Some(&FULL_EXPLAIN_LOWERING))
	}

	/// This context lowering the statements that are not data statements through `schema`.
	/// Without it, each of them is refused.
	pub(crate) fn with_schema(
		mut self,
		schema: Option<&'static (dyn SchemaLowering + Sync)>,
	) -> Self {
		self.schema = schema;
		self
	}

	/// The lowering of the statements that are not data statements, if this context serves them.
	pub(crate) fn schema(&self) -> Option<&'static (dyn SchemaLowering + Sync)> {
		self.schema
	}

	/// This context lowering `EXPLAIN` through `explain`. Without it, each `EXPLAIN` is refused.
	pub(crate) fn with_explain(
		mut self,
		explain: Option<&'static (dyn ExplainLowering + Sync)>,
	) -> Self {
		self.explain = explain;
		self
	}

	/// The lowering of `EXPLAIN`, if this context serves it.
	pub(crate) fn explain(&self) -> Option<&'static (dyn ExplainLowering + Sync)> {
		self.explain
	}

	/// Holds `bundle` as the bundle of the stages `set` runs over the records of `table`, which a
	/// write of a container this context lowers links.
	pub(crate) fn link_stages(
		&mut self,
		table: TableName,
		set: crate::exec::vm::units::StageSetUnit,
		bundle: std::sync::Arc<Program>,
	) {
		self.linked.insert((table, set), bundle);
	}

	/// Whether the bundle of the stages `set` runs over the records of `table` is held.
	pub(crate) fn links_stages(
		&self,
		table: &TableName,
		set: &crate::exec::vm::units::StageSetUnit,
	) -> bool {
		self.linked.contains_key(&(table.clone(), set.clone()))
	}

	/// The bundle of the stages `set` runs over the records of `table`, held by
	/// [`Self::link_stages`].
	pub(crate) fn linked_stages(
		&self,
		table: &TableName,
		set: &crate::exec::vm::units::StageSetUnit,
	) -> Option<std::sync::Arc<Program>> {
		self.linked.get(&(table.clone(), set.clone())).cloned()
	}

	/// Whether the container being lowered is a unit (a function or closure body, a subquery, a
	/// stage bundle, a cascade, a subscription), never a statement of a request. A statement that
	/// writes the request's session is lowered only where this is false.
	pub(crate) fn in_unit(&self) -> bool {
		self.unit
	}

	/// This context with the compile cache the planning callback serves units from.
	pub(crate) fn with_compiled(mut self, compiled: &'a CompiledUnits) -> Self {
		self.compiled = Some(compiled);
		self
	}

	/// This context resolving the indexes its sources open through `indexes`. Without it, a
	/// container that reads an index is refused.
	pub(crate) fn with_indexes(mut self, indexes: &'a dyn IndexResolver) -> Self {
		self.indexes = indexes;
		self
	}

	/// This context lowering a unit whose top block takes, after the region's own arguments,
	/// `count` values of the program that runs it: the unit the region of a subquery or of a
	/// recursion's body is, copied by [`Ir::copy_region`] with `count` its
	/// [`RegionUnit::inputs`](crate::exec::ir::core::copy::RegionUnit). The program reads the
	/// `at`th of them as the parameter
	/// [`unit_input_name`](super::super::program::unit_input_name)`(at)`, which the instruction
	/// that runs the unit binds. Without it, a container reads no argument of its top block as an
	/// input.
	pub(crate) fn with_unit_inputs(mut self, count: u16) -> Self {
		self.set_unit_inputs(count);
		self
	}

	/// Sets how many trailing arguments of the top block of the container lowered next are inputs
	/// of the unit it is ([`LowerCx::with_unit_inputs`]); the compile driver sets it for every
	/// container it lowers.
	pub(crate) fn set_unit_inputs(&mut self, count: u16) {
		self.unit_inputs = count;
	}

	/// This context with the limits of the request's configuration, which the instructions that
	/// enforce a limit hold. Without it, the limits are the default configuration's.
	pub(crate) fn with_limits(mut self, limits: LowerLimits) -> Self {
		self.limits = limits;
		self
	}

	/// This context with the rendering of `EXPLAIN ANALYZE` leaving out elapsed time and batch
	/// counts, as the session redacts them. Without it, the rendering keeps them.
	pub(crate) fn with_redact_volatile(mut self, redact: bool) -> Self {
		self.redact_volatile = redact;
		self
	}

	/// Whether the rendering of `EXPLAIN ANALYZE` leaves out elapsed time and batch counts.
	pub(crate) fn redact_volatile(&self) -> bool {
		self.redact_volatile
	}

	/// Whether the program being lowered serves the late-bound statements of its blocks when it
	/// reaches them, which the statement of a request and a program an operator runs do. Any
	/// other program runs such a statement as lowered.
	pub(crate) fn serves_late(&self) -> bool {
		self.serves_late
	}

	/// The resolver of the indexes the sources of this compile open.
	pub(crate) fn indexes(&self) -> &'a dyn IndexResolver {
		self.indexes
	}

	/// The configuration limits the instructions of this compile enforce.
	#[allow(dead_code)] // Read by the expression lowering, whose instructions hold a limit.
	pub(crate) fn limits(&self) -> LowerLimits {
		self.limits
	}

	/// The function registry the lowering resolves calls against.
	#[allow(dead_code)] // Read by the call lowering, which resolves builtins through the context.
	pub(crate) fn registry(&self) -> &'a FunctionRegistry {
		self.registry
	}

	/// The capabilities the node runs under.
	#[allow(dead_code)] // Read by the call lowering, which folds what a capability denies.
	pub(crate) fn capabilities(&self) -> &std::sync::Arc<crate::dbs::Capabilities> {
		&self.capabilities
	}

	/// The compile cache the planning callback serves units from, when the node keeps one.
	#[allow(dead_code)] // Read by the compile driver's planning callback.
	pub(crate) fn compiled(&self) -> Option<&'a CompiledUnits> {
		self.compiled
	}

	/// The frame the units of this compile are lowered for.
	pub(crate) fn frame(&self) -> &UnitFrame {
		&self.frame
	}

	/// Whether the reads of the principal the units being lowered are lowered for are permission
	/// checked.
	pub(crate) fn reads_checked(&self) -> bool {
		self.reads_checked
	}

	/// Makes `reads_checked` whether the reads of the principal the units are lowered for are
	/// permission checked; a context starts out checked.
	pub(crate) fn with_reads_checked(mut self, reads_checked: bool) -> Self {
		self.reads_checked = reads_checked;
		self
	}

	/// The re-entry depth the units of this compile are lowered at.
	#[allow(dead_code)] // Read by the call lowering, which bounds the depth of what a call enters.
	pub(crate) fn depth(&self) -> u32 {
		self.depth
	}

	/// Makes `frame`, `reads_checked` and `depth` the frame, read checking and re-entry depth
	/// what is lowered next is lowered for, and answers those they replace. The compile driver
	/// lowers a unit planned for a frame of its own (a function body bound for the caller's
	/// principal capped by the definition's `AUTH LIMIT`) under it, and restores the compile's
	/// after.
	/// Makes `sites` the measure of the stored code what is lowered next is lowered from, and
	/// answers the one it replaces ([`Self::depth_sites`]).
	pub(crate) fn resite(
		&mut self,
		sites: Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>>,
	) -> Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>> {
		std::mem::replace(&mut self.depth_sites, sites)
	}

	/// What the stored code being lowered spends of the computation-depth budget where it is
	/// evaluated one expression at a time; `None` for a statement the session runs.
	pub(crate) fn depth_sites(&self) -> Option<&crate::exec::vm::depth::ExprDepth> {
		self.depth_sites.as_deref()
	}

	/// The measure of the stored code that was being lowered when the slot `id` was asked for:
	/// what a unit of a region of that code ([`UnitRequest::is_relative`]) spends is measured
	/// with the code it was copied from. `None` where the request came from a statement the
	/// session runs.
	pub(crate) fn requested_sites(
		&self,
		id: UnitId,
	) -> Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>> {
		self.slot_sites.get(id.0 as usize).cloned().flatten()
	}

	/// Where the container being lowered stands below the statement the session runs
	/// ([`StatementNesting`]).
	pub(crate) fn nesting(&self) -> StatementNesting {
		self.nesting
	}

	/// Makes `nesting` where what is lowered next stands, and answers where what was lowered
	/// stood ([`Self::nesting`]).
	pub(crate) fn renest(&mut self, nesting: StatementNesting) -> StatementNesting {
		std::mem::replace(&mut self.nesting, nesting)
	}

	/// Where the region the unit of slot `id` is copied from stood when the slot was asked for: a
	/// unit of a statement in an expression stands there.
	pub(crate) fn requested_nesting(&self, id: UnitId) -> StatementNesting {
		self.slot_nesting.get(id.0 as usize).copied().unwrap_or_default()
	}

	/// Makes `actor` the enforcement of the actor the container being lowered runs on behalf of,
	/// and answers the one it replaces ([`Self::actor_suppressed`]).
	pub(crate) fn reactor(&mut self, actor: Option<bool>) -> Option<bool> {
		std::mem::replace(&mut self.actor_suppressed, actor)
	}

	/// The enforcement of the actor the container being lowered runs on behalf of, where it
	/// differs from the frame's; `None` where the actor is the frame's.
	pub(crate) fn actor(&self) -> Option<bool> {
		self.actor_suppressed.filter(|actor| *actor != self.frame.perms_suppressed)
	}

	/// Whether the actor the container being lowered runs on behalf of suppresses enforcement:
	/// the frame's own unless the unit was planned for an actor whose enforcement differs, as a
	/// delete of a referencing record keeps the writer's for what it triggers.
	pub(crate) fn actor_suppressed(&self) -> bool {
		self.actor_suppressed.unwrap_or(self.frame.perms_suppressed)
	}

	pub(crate) fn rescope(
		&mut self,
		frame: UnitFrame,
		reads_checked: bool,
		depth: u32,
	) -> (UnitFrame, bool, u32) {
		(
			std::mem::replace(&mut self.frame, frame),
			std::mem::replace(&mut self.reads_checked, reads_checked),
			std::mem::replace(&mut self.depth, depth),
		)
	}

	/// The instruction-set version the units of this compile are lowered to.
	#[allow(dead_code)] // Read by the closure lowering, whose head names the version it was lowered to.
	pub(crate) fn isa(&self) -> u32 {
		ISA_VERSION
	}

	/// Makes `container` the one being lowered, which relative requests are keyed by.
	pub(crate) fn enter(&mut self, container: ContainerId) {
		self.container = container;
	}

	/// The container being lowered.
	pub(crate) fn container(&self) -> ContainerId {
		self.container
	}

	/// How many slots the unit table holds.
	pub(crate) fn slots(&self) -> usize {
		self.slots.len()
	}

	/// The state of the unit table now.
	pub(crate) fn mark(&self) -> Mark {
		Mark {
			slots: self.slots.len(),
		}
	}

	/// Restores the unit table to `mark`: the slots interned since are dropped, and a request
	/// that held one is asked for afresh. The requests not yet taken are dropped.
	pub(crate) fn rewind(&mut self, mark: Mark) {
		self.slots.truncate(mark.slots);
		self.slot_sites.truncate(mark.slots);
		self.slot_nesting.truncate(mark.slots);
		self.index.retain(|_, id| (id.0 as usize) < mark.slots);
		self.slotless.clear();
		self.fresh.clear();
	}

	/// The slot of the unit table that holds the unit `request` names, interned on its first
	/// appearance and reported in [`LowerCx::take_needed`].
	///
	/// A request that holds no slot ([`UnitRequest::takes_slot`]) is an internal error: it is
	/// asked for through [`LowerCx::need`].
	pub(crate) fn unit_for(&mut self, request: UnitRequest) -> Result<UnitId, IrError> {
		self.unit_within(request, StatementNesting::default())
	}

	/// [`Self::unit_for`] of a request asked where the regions `around` it place it within the
	/// container being lowered, which the slot records within where the container stands
	/// ([`Self::requested_nesting`]). A slot asked for again keeps where it was first asked.
	pub(crate) fn unit_within(
		&mut self,
		request: UnitRequest,
		around: StatementNesting,
	) -> Result<UnitId, IrError> {
		if !request.takes_slot() {
			return Err(IrError::Stale("ir_lower::units: a request that holds no unit slot"));
		}
		let key = self.key(request);
		if let Some(id) = self.index.get(&key) {
			return Ok(*id);
		}
		let slot = u32::try_from(self.slots.len()).map_err(|_| IrError::Capacity("vm units"))?;
		let id = UnitId(slot);
		self.slots.push(key.clone());
		self.slot_sites.push(self.depth_sites.clone());
		self.slot_nesting.push(self.nesting.within(around));
		self.index.insert(key.clone(), id);
		self.fresh.push(Needed {
			request: key.2,
			id: Some(id),
		});
		Ok(id)
	}

	/// Reports `request`, which holds no slot of the unit table, on its first appearance.
	///
	/// A request that holds one is an internal error: it is asked for through
	/// [`LowerCx::unit_for`].
	pub(crate) fn need(&mut self, request: UnitRequest) -> Result<(), IrError> {
		if request.takes_slot() {
			return Err(IrError::Stale("ir_lower::units: a request that holds a unit slot"));
		}
		let key = self.key(request);
		if self.slotless.insert(key.clone()) {
			self.fresh.push(Needed {
				request: key.2,
				id: None,
			});
		}
		Ok(())
	}

	/// The requests made for the first time since the last call, in the order they were made.
	pub(crate) fn take_needed(&mut self) -> Vec<Needed> {
		std::mem::take(&mut self.fresh)
	}

	/// The identity `request` is interned under in the container being lowered.
	fn key(&self, request: UnitRequest) -> Key {
		let by = request.is_relative().then_some(self.container);
		let caller =
			matches!(request, UnitRequest::FunctionBody { .. } | UnitRequest::FunctionGuard { .. })
				.then(|| self.frame.clone());
		(by, caller, request)
	}
}

/// What an expression lowering may ask of the unit table it lowers into: the slot of the unit a
/// request names.
pub(super) trait UnitSink {
	/// The slot of the unit table that holds the unit `request` names, as
	/// [`LowerCx::unit_for`] answers it.
	fn unit_for(&mut self, request: UnitRequest) -> Result<UnitId, IrError>;

	/// The slot of the unit table that holds the unit `request` names, asked where the regions
	/// `around` place it, as [`LowerCx::unit_within`] answers it.
	fn unit_within(
		&mut self,
		request: UnitRequest,
		around: StatementNesting,
	) -> Result<UnitId, IrError>;

	/// Reports `request`, which holds no slot of the unit table, as [`LowerCx::need`] does.
	fn need(&mut self, request: UnitRequest) -> Result<(), IrError>;

	/// Whether the program being lowered serves late-bound statements ([`LowerCx::serves_late`]).
	fn serves_late(&self) -> bool;

	/// The re-entry depth the program is lowered at ([`LowerCx::depth`]).
	fn depth(&self) -> u32;

	/// Whether the principal's reads are permission checked ([`LowerCx::reads_checked`]).
	fn reads_checked(&self) -> bool;

	/// The measure of the stored code being lowered ([`LowerCx::depth_sites`]).
	fn depth_sites(&self) -> Option<&crate::exec::vm::depth::ExprDepth>;

	/// Makes `sites` the measure of the stored code being lowered, answering the one it replaces
	/// ([`LowerCx::resite`]).
	fn resite(
		&mut self,
		sites: Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>>,
	) -> Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>>;

	/// How many trailing arguments of the container's top block are inputs of the unit it is
	/// ([`LowerCx::with_unit_inputs`]).
	fn unit_inputs(&self) -> u16;

	/// Lowers the read statement `region` holds into `em` as the group of a block's statement,
	/// numbering its pools from `base` ([`pipe::lower_scope`]).
	fn lower_statement(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		region: RegionId,
		base: PoolBase,
	) -> Result<ReadScope, IrError>;

	/// Lowers the write whose op is `write`, of the statement `region` holds, into `em` as the
	/// group of a block's statement, numbering its pools from `base`
	/// ([`write_stmt::lower_write_in`]).
	fn lower_write(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		write: OpId,
		region: RegionId,
		base: PoolBase,
	) -> Result<LoweredWrite, IrError>;

	/// Lowers `value` into `em` as a host scalar the run evaluates when the statement starts, and
	/// answers what it needs ([`read::host_scalar_of`]).
	fn host_scalar(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		value: ValueId,
	) -> Result<(HostScalar, Needs), IrError>;

	/// Lowers the `EXPLAIN` the statement `region` holds, whose op is `explain`, into `em` as the
	/// group of a block's statement, an analysed one's tree built for `purpose`
	/// ([`ExplainLowering::lower_explained`]).
	fn lower_explained(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		region: RegionId,
		explain: OpId,
		base: PoolBase,
		purpose: Purpose,
	) -> Result<Explained, IrError>;

	/// What `EXPLAIN ANALYZE` of the statement the `phys.explain` `explain` explains renders, its
	/// tree built for `purpose` ([`ExplainLowering::analysis`]).
	fn explain_analysis(
		&mut self,
		ir: &Ir,
		explain: OpId,
		purpose: Purpose,
	) -> Result<crate::exec::vm::program::Analysis, IrError>;

	/// The name of the operator at the top of the plan `EXPLAIN` renders for the statement
	/// `region` holds, where this context serves `EXPLAIN`
	/// ([`ExplainLowering::region_root_name`]). `None` where it does not: the name is only part
	/// of the text of an error.
	fn region_root_name(&self, ir: &Ir, region: RegionId) -> Option<String>;

	/// Lowers the schema statement whose op is `op` into `em` as one pipeline's code, with the
	/// operands it evaluates lowered in this context ([`SchemaLowering::ddl`]).
	fn lower_schema(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		facts: &FactCache,
		op: OpId,
		reservations: &mut Reservations,
	) -> Result<Needs, IrError>;

	/// Lowers the `INFO` statement whose op is `op` into `em` as one pipeline's code, with the
	/// operands it evaluates lowered in this context ([`SchemaLowering::info`]).
	fn lower_info(&mut self, em: &mut Emitter, ir: &Ir, op: OpId) -> Result<InfoLowered, IrError>;
}

impl UnitSink for LowerCx<'_> {
	fn unit_for(&mut self, request: UnitRequest) -> Result<UnitId, IrError> {
		LowerCx::unit_for(self, request)
	}

	fn unit_within(
		&mut self,
		request: UnitRequest,
		around: StatementNesting,
	) -> Result<UnitId, IrError> {
		LowerCx::unit_within(self, request, around)
	}

	fn need(&mut self, request: UnitRequest) -> Result<(), IrError> {
		LowerCx::need(self, request)
	}

	fn serves_late(&self) -> bool {
		self.serves_late
	}

	fn depth(&self) -> u32 {
		self.depth
	}

	fn reads_checked(&self) -> bool {
		self.reads_checked
	}

	fn depth_sites(&self) -> Option<&crate::exec::vm::depth::ExprDepth> {
		LowerCx::depth_sites(self)
	}

	fn resite(
		&mut self,
		sites: Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>>,
	) -> Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>> {
		LowerCx::resite(self, sites)
	}

	fn unit_inputs(&self) -> u16 {
		self.unit_inputs
	}

	fn lower_statement(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		region: RegionId,
		base: PoolBase,
	) -> Result<ReadScope, IrError> {
		pipe::lower_scope(em, self, ir, &mut FactCache::new(), region, base, true)
	}

	fn lower_write(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		write: OpId,
		region: RegionId,
		base: PoolBase,
	) -> Result<LoweredWrite, IrError> {
		write_stmt::lower_write_in(self, em, ir, write, region, base)
	}

	fn host_scalar(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		value: ValueId,
	) -> Result<(HostScalar, Needs), IrError> {
		let mut needs = Needs::none();
		let scalar = read::host_scalar_of(em, self, ir, value, &mut needs)?;
		Ok((scalar, needs))
	}

	fn lower_explained(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		region: RegionId,
		explain: OpId,
		base: PoolBase,
		purpose: Purpose,
	) -> Result<Explained, IrError> {
		explained_served(self)?.lower_explained(self, em, ir, region, explain, base, purpose)
	}

	fn explain_analysis(
		&mut self,
		ir: &Ir,
		explain: OpId,
		purpose: Purpose,
	) -> Result<crate::exec::vm::program::Analysis, IrError> {
		explained_served(self)?.analysis(self, ir, explain, purpose)
	}

	fn region_root_name(&self, ir: &Ir, region: RegionId) -> Option<String> {
		self.explain?.region_root_name(ir, region)
	}

	fn lower_schema(
		&mut self,
		em: &mut Emitter,
		ir: &Ir,
		facts: &FactCache,
		op: OpId,
		reservations: &mut Reservations,
	) -> Result<Needs, IrError> {
		let schema = served(self)?;
		schema.ddl(em, ir, facts, self, op, reservations)
	}

	fn lower_info(&mut self, em: &mut Emitter, ir: &Ir, op: OpId) -> Result<InfoLowered, IrError> {
		let schema = served(self)?;
		schema.info(em, self, ir, op)
	}
}

/// Lowers `ir`, a container past the physical boundary, to the program of a statement, or of the
/// stages one table runs for a write that names no one table where `ir` is a stage bundle
/// ([`write_stmt::lower_stage_bundle`](super::write_stmt::lower_stage_bundle)).
///
/// The program holds none of the units it asks for: they are the requests of the result, which
/// the compile driver plans and lowers into the table the program's ids index.
pub(crate) fn lower_program(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &FactCache,
) -> Result<Lowered, NotCovered> {
	if is_stage_bundle(ir) {
		return Ok(super::write_stmt::lower_stage_bundle(cx, ir)?);
	}
	Ok(dispatch::lower_with(cx, ir, facts)?)
}

/// Whether `ir` is the container of a stage bundle: its top region is a stage that holds the
/// statement of a write.
fn is_stage_bundle(ir: &Ir) -> bool {
	ir.region(ir.top()).map(|region| region.kind) == Some(RegionKind::WriteStage)
		&& super::write_stmt::bundled_write(ir).is_some()
}

/// [`lower_program`] for a program an operator runs, which serves the late-bound statements of
/// its blocks when it reaches them: those are requests of the result, as written by the
/// container that holds them.
pub(crate) fn lower_serving(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &FactCache,
) -> Result<Lowered, NotCovered> {
	serving(cx, ir, facts)
}

/// [`lower_program`] for a program that serves the late-bound statements of its blocks when it
/// reaches them; `cx` serves no more afterwards than it did before.
fn serving(cx: &mut LowerCx<'_>, ir: &Ir, facts: &FactCache) -> Result<Lowered, NotCovered> {
	let outer = std::mem::replace(&mut cx.serves_late, true);
	let lowered = lower_program(cx, ir, facts);
	cx.serves_late = outer;
	lowered
}

/// [`lower_program`] for a statement of a request, whose units are slots of the table `cx` holds
/// for the whole request: the program holds none of its own, and the ids it calls index the
/// units of the program that holds the request.
pub(crate) fn lower_request_statement(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &FactCache,
) -> Result<Lowered, NotCovered> {
	serving(cx, ir, facts)
}

/// [`lower_program`] for a container that is a unit of an enclosing program, whose head names
/// the signature `sig` and the frame `cx` lowers for. A head the lowering set itself, as a
/// closure's body does, stands.
///
/// A container whose top region is a loop's body is the body of a recursion copied out of its
/// requester, and lowers as one value over the row it is run with
/// ([`dispatch::lower_value_unit`]); one whose top region is a subquery's is a statement copied
/// out of an expression ([`dispatch::lower_subquery_unit`]); one whose top region is a function
/// body is the body of a user-defined function ([`dispatch::lower_body_unit`]); one whose top
/// region is a permission frame is the `PERMISSIONS` predicate of a user-defined function
/// ([`dispatch::lower_guard_unit`]); one whose top region is a stage holding a table's events is
/// the unit that runs them ([`events::lower_events`]).
pub(crate) fn lower_unit(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &FactCache,
	sig: UnitSig,
) -> Result<Lowered, NotCovered> {
	let outer = std::mem::replace(&mut cx.unit, true);
	let lowered = match ir.region(ir.top()).map(|top| top.kind) {
		Some(RegionKind::Loop) => {
			dispatch::lower_value_unit(cx, ir, true).map_err(NotCovered::from)
		}
		Some(RegionKind::Subquery) => {
			dispatch::lower_subquery_unit(cx, ir, facts).map_err(NotCovered::from)
		}
		Some(RegionKind::FunctionBody) => {
			dispatch::lower_body_unit(cx, ir).map_err(NotCovered::from)
		}
		Some(RegionKind::PermissionFrame) => {
			dispatch::lower_guard_unit(cx, ir).map_err(NotCovered::from)
		}
		Some(RegionKind::WriteStage) if events::is_events_unit(ir) => {
			events::lower_events(cx, ir).map_err(NotCovered::from)
		}
		_ => lower_program(cx, ir, facts),
	};
	cx.unit = outer;
	let mut lowered = lowered?;
	stamp(cx, &mut lowered.program, sig);
	Ok(lowered)
}

/// The program of a whole request: no pipeline of its own, the table `units` that the programs of
/// its statements index, and `request`, its steps and those programs.
pub(crate) fn request_program(
	units: crate::exec::vm::UnitTable,
	request: super::super::program::RequestCode,
) -> Result<Program, NotCovered> {
	let mut program = super::emit::Emitter::new()
		.finish_program(Vec::new(), crate::exec::AccessMode::ReadOnly)?;
	program.units = units;
	program.request = Some(Box::new(request));
	Ok(program)
}

/// A lowering of no pipeline, with the requests `cx` holds: what a test lowering that only makes
/// requests answers.
#[cfg(test)]
pub(crate) fn empty_lowered(cx: &mut LowerCx<'_>) -> Result<Lowered, NotCovered> {
	let program = super::emit::Emitter::new()
		.finish_program(Vec::new(), crate::exec::AccessMode::ReadOnly)?;
	Ok(Lowered {
		program,
		provenance: super::place::Provenance::default(),
		needs: cx.take_needed(),
	})
}

/// Gives `program` the head of a unit of the frame `cx` lowers for, unless it holds one.
fn stamp(cx: &LowerCx<'_>, program: &mut Program, sig: UnitSig) {
	if program.unit.is_none() {
		program.unit = Some(UnitHead {
			sig,
			frame: cx.frame.clone(),
			entry: None,
			closure: None,
		});
	}
}

/// What the head of a closure body unit names, which the planner states from the literal of the
/// requesting container and the transaction it plans in. A run links the unit for a closure value
/// only where the head matches the value exactly (`vm::closure`), so each field is the value's.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClosureTemplate {
	/// The literal as written, which the run compares whole.
	pub(crate) closure: std::sync::Arc<crate::expr::logical::LogicalClosure>,
	/// [`ClosureDigest::of`] the literal.
	pub(crate) digest: ClosureDigest,
	/// The names the literal captures, in the order of the body region's capture arguments.
	pub(crate) captures: Vec<String>,
	/// Whether the body reads the row it is invoked over, which the body region takes after its
	/// captures, and then the document root where `root` holds.
	pub(crate) row: bool,
	/// Whether the body reads the document root of the frame that invokes it apart from that row,
	/// which the body region takes after the row.
	pub(crate) root: bool,
	/// How many table catalog versions the transaction had stamped when the body was planned.
	pub(crate) stamps: u64,
	/// The `VERSION` stamp the body is planned at.
	pub(crate) version: Option<u64>,
}

impl ClosureTemplate {
	/// The names a value of the literal captures, sorted, as the run lists them.
	pub(crate) fn captured(&self) -> Vec<String> {
		let mut captured = self.captures.clone();
		captured.sort();
		captured.dedup();
		captured
	}

	/// The names bound in the unit's slots, in slot order: the literal's parameters, then the
	/// captured names that are not also parameters, sorted.
	pub(crate) fn slots(&self) -> Vec<String> {
		let mut slots: Vec<String> =
			self.closure.args.iter().map(|(arg, _)| arg.as_str().to_owned()).collect();
		for name in self.captured() {
			if !slots.contains(&name) {
				slots.push(name);
			}
		}
		slots
	}

	/// The rows the unit takes: the invoking row where the body reads one.
	pub(crate) fn rows(&self) -> RowBits {
		RowBits {
			row: self.row,
		}
	}

	/// The signature of the unit's head: its slots by name, run over the invoking row.
	pub(crate) fn sig(&self) -> UnitSig {
		UnitSig {
			params: self.slots().iter().map(|name| name.as_str().into()).collect(),
			takes_row: true,
		}
	}
}

/// Lowers `ir`, a container whose top region is the body of the closure literal `template`
/// describes, as the unit a value of that literal runs ([`dispatch::lower_closure_unit`]). The
/// unit's head names exactly the template's captures and row captures, or the lowering is an
/// internal error, because a closure value links only a unit whose head it matches.
pub(crate) fn lower_closure_body(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	template: &ClosureTemplate,
) -> Result<Lowered, NotCovered> {
	let lowered = dispatch::lower_closure_unit(cx, ir, template)?;
	let (captured, rows) = (template.captured(), template.rows());
	let named = lowered
		.program
		.unit
		.as_ref()
		.and_then(|head| head.closure.as_ref())
		.is_some_and(|closure| closure.captured == captured && closure.rows == rows);
	if !named {
		return Err(NotCovered::internal(
			"a closure body lowered with a head that does not name its captures",
		));
	}
	Ok(lowered)
}

/// Lowers `ir`, a container whose top region is a subscriber frame, to the unit that runs a live
/// query for records of one table ([`subscription::lower_subscriber`]).
///
/// The program holds none of the units it asks for: they are the requests of the result, which the
/// compile driver plans and lowers into the table the program's ids index before the unit is held
/// ([`LoweredLive::into_unit`]).
pub(crate) fn lower_subscription(cx: &mut LowerCx<'_>, ir: &Ir) -> Result<LoweredLive, NotCovered> {
	Ok(subscription::lower_subscriber(cx, ir)?)
}

/// [`lower_unit`] for a statement a deletion performs on its own behalf.
pub(crate) fn lower_cascade(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &FactCache,
	sig: UnitSig,
) -> Result<Lowered, NotCovered> {
	lower_unit(cx, ir, facts, sig)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::phys::ops::PhysOp;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::val::Value;

	fn table(name: &str) -> TableName {
		TableName::from(name)
	}

	fn function(name: &str) -> UnitRequest {
		UnitRequest::FunctionBody {
			name: name.to_owned(),
			unchecked: false,
			limit: None,
		}
	}

	/// A region id, taken from a container that holds one.
	fn region() -> RegionId {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let owner = b.add_phys(PhysOp::Eval, &[one]);
		b.open(owner, crate::exec::ir::iface::region_kind::RegionKind::Request, &[]);
		b.close();
		b.ir.op(owner).and_then(|data| data.regions.first().copied()).expect("the op owns a region")
	}

	#[test]
	fn a_request_is_one_slot_however_often_it_is_made() {
		let mut cx = LowerCx::detached();
		let first = cx.unit_for(function("f")).expect("a slot");
		let other = cx.unit_for(function("g")).expect("a slot");
		let again = cx.unit_for(function("f")).expect("a slot");
		assert_eq!(first, UnitId(0));
		assert_eq!(other, UnitId(1));
		assert_eq!(again, first);
		assert_eq!(cx.slots(), 2);
		// Only the first appearance of a request is reported.
		let needed = cx.take_needed();
		assert_eq!(
			needed,
			vec![
				Needed {
					request: function("f"),
					id: Some(UnitId(0)),
				},
				Needed {
					request: function("g"),
					id: Some(UnitId(1)),
				},
			]
		);
		assert!(cx.take_needed().is_empty());
	}

	#[test]
	fn a_request_is_keyed_by_what_distinguishes_it() {
		let mut cx = LowerCx::detached();
		let bundle = |action| UnitRequest::StageBundle {
			table: table("person"),
			action,
		};
		let create = cx.unit_for(bundle(Action::Create)).expect("a slot");
		let update = cx.unit_for(bundle(Action::Update)).expect("a slot");
		assert_ne!(create, update);
		let cascade = |suppressed, caller| UnitRequest::Cascade {
			kind: CascadeKind::Edges,
			suppressed,
			caller,
		};
		let a = cx.unit_for(cascade(true, true)).expect("a slot");
		let b = cx.unit_for(cascade(true, false)).expect("a slot");
		assert_ne!(a, b);
		assert_eq!(cx.unit_for(cascade(true, true)).expect("a slot"), a);
	}

	#[test]
	fn a_relative_request_is_keyed_by_the_container_that_asked() {
		let mut cx = LowerCx::detached();
		let subquery = UnitRequest::Subquery {
			region: region(),
		};
		cx.enter(ContainerId(0));
		let in_first = cx.unit_for(subquery.clone()).expect("a slot");
		assert_eq!(cx.unit_for(subquery.clone()).expect("a slot"), in_first);
		// The same region of another container is another unit.
		cx.enter(ContainerId(1));
		let in_second = cx.unit_for(subquery).expect("a slot");
		assert_ne!(in_first, in_second);
		// A request that names no position is shared across containers.
		cx.enter(ContainerId(0));
		let shared = cx.unit_for(function("f")).expect("a slot");
		cx.enter(ContainerId(1));
		assert_eq!(cx.unit_for(function("f")).expect("a slot"), shared);
	}

	#[test]
	fn slotless_requests_hold_no_slot_and_are_reported_once() {
		let mut cx = LowerCx::detached();
		let served = UnitRequest::ServedStatement {
			position: 0,
			written: 0,
		};
		assert!(cx.unit_for(served.clone()).is_err());
		assert!(cx.need(function("f")).is_err());
		cx.need(served.clone()).expect("reported");
		cx.need(served.clone()).expect("reported");
		assert_eq!(cx.slots(), 0);
		assert_eq!(
			cx.take_needed(),
			vec![Needed {
				request: served,
				id: None,
			}]
		);
		// A table unit is the driver's request alone.
		let unit = UnitRequest::TableUnit {
			table: table("person"),
			unchecked: false,
		};
		assert!(cx.unit_for(unit).is_err());
	}

	#[test]
	fn rewinding_restores_the_table() {
		let mut cx = LowerCx::detached();
		let kept = cx.unit_for(function("kept")).expect("a slot");
		let mark = cx.mark();
		let dropped = cx.unit_for(function("dropped")).expect("a slot");
		assert_eq!(dropped, UnitId(1));
		cx.rewind(mark);
		assert_eq!(cx.slots(), 1);
		assert!(cx.take_needed().is_empty());
		// What was kept still answers its slot, and a dropped request is asked for afresh.
		assert_eq!(cx.unit_for(function("kept")).expect("a slot"), kept);
		assert_eq!(cx.unit_for(function("dropped")).expect("a slot"), UnitId(1));
	}

	#[test]
	fn a_unit_is_stamped_with_the_frame_unless_it_holds_a_head() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Eval, &[one]);
		b.physical();
		let mut cx = LowerCx::detached();
		let sig = UnitSig {
			params: Vec::new(),
			takes_row: true,
		};
		let facts = FactCache::new();
		let lowered = lower_unit(&mut cx, &b.ir, &facts, sig.clone()).expect("the unit lowers");
		let head = lowered.program.unit.as_ref().expect("a unit has a head");
		assert_eq!(head.sig, sig);
		assert_eq!(head.frame, *cx.frame());
		assert!(head.entry.is_none() && head.closure.is_none());
		// A program lowered as a program is no unit.
		let program = lower_program(&mut cx, &b.ir, &facts).expect("the program lowers");
		assert!(program.program.unit.is_none());
	}

	/// The template of the literal `text`, which captures `captures` in that order.
	fn template_of(text: &str, captures: &[&str], row: bool) -> ClosureTemplate {
		let crate::expr::logical::LogicalExpr::Closure(closure) =
			crate::exec::operators::test_util::parse_expr(text)
		else {
			panic!("a closure");
		};
		let closure = std::sync::Arc::new(*closure);
		ClosureTemplate {
			digest: ClosureDigest::of(&closure),
			closure,
			captures: captures.iter().map(|name| (*name).to_owned()).collect(),
			row,
			root: false,
			stamps: 0,
			version: None,
		}
	}

	#[test]
	fn a_closure_unit_names_its_parameters_then_its_captures_sorted() {
		// The captures are in the order the body first reads them; the run lists them sorted.
		let template = template_of("|$b, $a| $z + $y + $a + $b", &["z", "y"], true);
		assert_eq!(template.captured(), ["y", "z"]);
		assert_eq!(template.slots(), ["b", "a", "y", "z"]);
		assert_eq!(
			template.rows(),
			RowBits {
				row: true,
			}
		);
		let sig = template.sig();
		assert!(sig.takes_row);
		assert_eq!(sig.params.len(), 4);
		// A literal that reads no row holds no row capture.
		assert_eq!(template_of("|| 1", &[], false).rows(), RowBits::default());
	}
}
