//! The operations of the logical dialect.
//!
//! One [`LogOp`] variant per statement, transaction, loop, write stage and terminator. A variant's
//! payload is the op's attribute set: plain data that is part of the op's identity. Evaluated
//! clauses are operands or regions, never payload, and no payload holds an expression, a plan or
//! a statement.
//!
//! A payload larger than 24 bytes is boxed, so the op storage of a container stays dense.
//!
//! [`LogTag`] is the payload-free twin of [`LogOp`], and [`LogOp::tag`] is total.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use surrealdb_strand::{Strand, TableName};

use crate::catalog::aggregation::AggregateFields;
use crate::catalog::{EventDefinition, IndexDefinition, TableDefinition, ViewDefinition};
use crate::doc::Action;
use crate::exec::ir::expr::ops::KeyRange;
use crate::exec::ir::iface::statements::{
	AccessOp, AlterObject, AlterTemplate, BindStepShape, ContentMode, DdlChecks, DdlGuard,
	DefineObject, DefinitionTemplate, InfoSubject, LiveShape, MatchBindings, MatchData,
	MatchInsertEdge, MatchInsertNode, MatchOutputShape, MatchPattern, OperandSlot, OutputShape,
	Place, RemoveObject, ShowSince, WriteFlags, WriteForm,
};
use crate::exec::ir::iface::{ChooseSpec, ClauseRole, FieldRole, RegionKind, StatementCommit};
use crate::exec::operators::SplitTarget;
use crate::exec::operators::write::stage::{LightweightReason, RecordImage, TableTypeFor};
pub(crate) use crate::expr::logical::Emission;
use crate::expr::logical::{
	GuardKind, JoinKind, OutputPath, RelationAlias, WindowFrame, WindowFunction,
};
use crate::expr::lookup::LookupKind;
use crate::expr::match_plan::{BindingId, DetachMode};
use crate::expr::part::Recurse;
use crate::expr::statements::define::DefineKind;
use crate::expr::{AssignOperator, Base, Explain, ExplainFormat, Idiom, Kind, Part, With};
use crate::val::Uuid;

define_op_tags! {
	/// The payload-free identity of a [`LogOp`].
	pub(crate) enum LogTag {
		Request => "logical.request",
		Txn => "logical.txn",
		Stmt => "logical.stmt",
		Statement => "logical.statement",
		Source => "logical.source",
		Values => "logical.values",
		Match => "logical.match",
		MatchRead => "logical.match_read",
		MatchUpdate => "logical.match_update",
		MatchDelete => "logical.match_delete",
		MatchInsert => "logical.match_insert",
		MatchOutput => "logical.match_output",
		BindStep => "logical.bind_step",
		Subquery => "logical.subquery",
		Write => "logical.write",
		Assign => "logical.assign",
		Unset => "logical.unset",
		Content => "logical.content",
		DdlGuard => "logical.ddl_guard",
		Define => "logical.define",
		Remove => "logical.remove",
		Alter => "logical.alter",
		Rebuild => "logical.rebuild",
		UnionAll => "logical.union_all",
		Join => "logical.join",
		Filter => "logical.filter",
		PermissionFilter => "logical.permission_filter",
		Split => "logical.split",
		Aggregate => "logical.aggregate",
		Distinct => "logical.distinct",
		Sort => "logical.sort",
		Window => "logical.window",
		Limit => "logical.limit",
		Project => "logical.project",
		Fetch => "logical.fetch",
		Timeout => "logical.timeout",
		VersionScope => "logical.version_scope",
		Only => "logical.only",
		Explain => "logical.explain",
		Eval => "logical.eval",
		Block => "logical.block",
		Let => "logical.let",
		Return => "logical.return",
		Loop => "logical.loop",
		If => "logical.if",
		Info => "logical.info",
		Sleep => "logical.sleep",
		Use => "logical.use",
		Option => "logical.option",
		Access => "logical.access",
		Kill => "logical.kill",
		Live => "logical.live",
		Subscriber => "logical.subscriber",
		Show => "logical.show",
		Choose => "logical.choose",
		WriteStage => "logical.write_stage",
		Events => "logical.events",
		Yield => "logical.yield",
		Next => "logical.next",
		Break => "logical.break",
	}
}

/// One operation of the logical dialect.
///
/// Carries no `Eq` or `Hash`: some payloads embed types of the language that implement neither,
/// so identity is the structural digest of the attributes.
#[derive(Clone, Debug)]
pub(crate) enum LogOp {
	/// The top region of a fragment. `fragment` is the index of the fragment in its stream; a
	/// `nested` request is the query a function or script runs, whose statements receive the
	/// calling statement's timestamp as an entry argument.
	Request {
		fragment: Option<u32>,
		nested: bool,
	},
	/// An explicit or session transaction; one `Transaction` region. Autocommit is not a
	/// transaction.
	Txn {
		mode: TxnRegionMode,
	},
	/// One statement; one `Statement` region whose block argument is the statement timestamp.
	/// `control` names the `BEGIN`, `COMMIT` or `CANCEL` a slot of the request stands for: the
	/// executor drives the transaction itself, so such a slot holds `NONE` and plans nothing.
	Stmt {
		commit: StatementCommit,
		control: Option<TxnControl>,
	},
	/// A statement of a block that runs as a group of its own: a read, a write, a schema statement
	/// or a `MATCH`. One `Statement` region holds the statement's ops and ends with its value.
	/// `changes_catalog` marks a statement that may change the catalog the statements after it in
	/// the block run against. `served` marks one the block plans when it reaches it, after a
	/// statement before it changed the catalog: the index of the statement as written among the
	/// container's late-bound statements, whose region yields `NONE` in its place.
	Statement {
		changes_catalog: bool,
		served: Option<u32>,
	},
	/// Rows read from storage. Operands: the source value when `source_ref` is not a table.
	Source(Box<SourceAttrs>),
	/// Rows produced from a value. Operand: the value. `written` is true when the value is an
	/// array written in `FROM`, which is a list of sources.
	Values {
		for_update: bool,
		written: bool,
	},
	/// A `MATCH` query. One region per stage, in order; every binding in scope is a block argument
	/// of each stage region.
	Match {
		bindings: MatchBindings,
	},
	/// A `MATCH` read stage. One `Clause(Where)` region per predicate.
	MatchRead {
		optional_group: Option<u32>,
		patterns: Box<[MatchPattern]>,
	},
	/// A `MATCH` update stage. One `Clause(SetValue)` region per name of a `Set`, or one
	/// `Clause(Content)` region.
	MatchUpdate(Box<MatchUpdateAttrs>),
	/// A `MATCH` delete stage.
	MatchDelete {
		target: BindingId,
		detach: DetachMode,
	},
	/// A `MATCH` insert stage. One `Clause(Content)` region per node and per edge, nodes first.
	MatchInsert(Box<MatchInsertAttrs>),
	/// A `MATCH` output stage. Operands: the skip and the limit, each when present. One
	/// `Clause(Projection)` region per column, then one `Clause(GroupKey)` per group key, then one
	/// `Clause(OrderKey)` per order key.
	MatchOutput(Box<MatchOutputShape>),
	/// One step of the folded binding table of a `MATCH` query. Operands: the rows the step
	/// extends (a join takes the build rows, then the probe rows). Clause regions as the shape
	/// lists them.
	BindStep(Box<BindStepShape>),
	/// A statement in relational position. One `Subquery` region.
	Subquery,
	/// A mutation. The subject loop is a `Loop` op of kind `Write`; `Clause(ReturnClause)`
	/// holds the projection when `output` is `Fields`.
	Write(Box<WriteAttrs>),
	/// The assignment of a value to a place, inside a `Clause(SetValue)` or
	/// `Clause(DuplicateUpdate)` region. Operands: the value, then the place's dynamic parts.
	Assign(Box<AssignAttrs>),
	/// The removal of a place, inside a `Clause(Content)` region. Operands: the place's dynamic
	/// parts.
	Unset {
		place: Place,
	},
	/// The data a write stores, inside a `Clause(Content)` region. Operand: the value.
	Content {
		mode: ContentMode,
	},
	/// The authority check that precedes a schema statement's operands or definition.
	DdlGuard {
		guard: DdlGuard,
	},
	/// A `DEFINE`. Operands and `StoredClause` regions as the form's attributes name them.
	Define(Box<DefineAttrs>),
	/// A `REMOVE`.
	Remove(Box<RemoveAttrs>),
	/// An `ALTER`.
	Alter(Box<AlterAttrs>),
	/// A `REBUILD INDEX`.
	Rebuild(Box<RebuildAttrs>),
	/// The concatenation of its inputs, in order.
	UnionAll,
	/// Each left row paired with the right rows whose keys match. Operands: the left and the right
	/// input. `Clause(Where)` regions: the keys, then the residual filter when present.
	Join {
		kind: JoinKind,
		alias: RelationAlias,
	},
	/// The rows a predicate admits. Operand: the input. One `Clause(Where)` region.
	Filter,
	/// The rows a permission admits, judged in the op's frame. Operand: the input. One
	/// `PermissionFrame` region, then one `FieldFrame(Computed)` region per field `computed`
	/// derives ([`ComputedReadsShape::derived`]).
	PermissionFilter {
		guard: GuardKind,
		/// The `COMPUTED` fields of the permitted table the predicate reads, derived onto a copy
		/// of the row it is judged against; `None` when it reads none or the rows come from no
		/// table whose schema the binder was given.
		computed: Option<Box<ComputedReadsShape>>,
	},
	/// `SPLIT ON`. Operand: the input. One `Clause(Split)` region per split.
	Split {
		/// Where each split reads its collection and writes each element back, in the order of
		/// the regions: the row is split on the targets, and the regions are not evaluated.
		targets: Box<[SplitTarget]>,
	},
	/// `GROUP BY`. Operand: the input. One `ErrorScope(ShapeToNone)` region per group key (as many
	/// as `groups` states), then one `Clause(Projection)` region per field.
	Aggregate(Box<AggregateShape>),
	/// One row per distinct value. Operand: the input.
	Distinct,
	/// `ORDER BY`. Operand: the input. One `Clause(OrderKey)` region per key.
	Sort(Box<SortAttrs>),
	/// Window functions over the input, which reaches it sorted on its partition and order keys.
	/// Operand: the input. One `Clause(OrderKey)` region per partition key, then one per order
	/// key, then one `Clause(Projection)` region per argument of each call, in call order.
	Window(Box<WindowShape>),
	/// `LIMIT` and `START`, evaluated once, `LIMIT` first. Operand: the input, then the slots
	/// named.
	Limit {
		limit: Option<OperandSlot>,
		start: Option<OperandSlot>,
	},
	/// The projection. Operand: the input. One `Clause(Projection)` region per field and, when
	/// `layer` is true, a `FieldFrame` region per column.
	Project(Box<ProjectAttrs>),
	/// `FETCH`. Operand: the input. One `Clause(Projection)` region per path.
	Fetch,
	/// `TIMEOUT`. Operand: the duration. One `Deadline` region holds the timed body.
	Timeout,
	/// `VERSION`. Operand: the version. Sets the snapshot of every op beneath it.
	VersionScope,
	/// `ONLY`. Operand: the input.
	Only,
	/// `EXPLAIN`. Operand: the input.
	Explain {
		analyze: bool,
		format: ExplainFormat,
	},
	/// A bare expression as a statement. Operand: the value.
	Eval,
	/// `{ ... }`. One `Block` or `ValueBlock` region, by position.
	Block,
	/// `LET`. Operand: the value.
	Let(Box<LetAttrs>),
	/// `RETURN`. Operand: the value. A terminator.
	Return,
	/// A loop over a source. Operand: the source. One body region with the element, the position
	/// and, for a `Fold` sink, the accumulator as block arguments. A loop that states a
	/// [`StepAttrs`] takes the further operands and regions that attribute names.
	Loop {
		kind: LoopKind,
		sink: LoopSink,
		order: SourceOrder,
		step: Option<Box<StepAttrs>>,
	},
	/// `IF` in statement position. Operand: the first condition. One `StatementIf` region per arm
	/// (`ValueIf` in value position); a further condition nests an `If` in the else arm.
	If,
	/// `INFO`.
	Info {
		subject: InfoSubject,
		structure: bool,
	},
	/// `SLEEP`. Operand: the duration.
	Sleep,
	/// `USE`. Operands: the namespace and database values the target names, in that order.
	Use {
		target: UseTarget,
	},
	/// `OPTION`.
	Option {
		name: Strand,
		value: bool,
	},
	/// `ACCESS`.
	Access(Box<AccessAttrs>),
	/// `KILL`. Operand: the live query id.
	Kill,
	/// `LIVE`. Operand: the target. `StoredClause(LiveFields)`, `StoredClause(LiveCond)` and
	/// `StoredClause(LiveFetch)` regions, each when present.
	Live(Box<LiveAttrs>),
	/// A live subscription's work on one record image of a table, the body of its notification
	/// unit. No operand and no result. The regions are the clauses the attributes state, in the
	/// order [`SubscriberAttrs::region_kinds`] lists.
	Subscriber(Box<SubscriberAttrs>),
	/// `SHOW CHANGES`.
	Show(Box<ShowAttrs>),
	/// A guarded choice between prepared alternatives. One `ChooseArm` region per arm.
	Choose(Box<ChooseSpec>),
	/// One stage of a write's per-record order. One `WriteStage` region.
	WriteStage {
		stage: WriteStageKind,
		mark: WriteMark,
	},
	/// A table's events for one action, the body of the stage bundle unit that runs them. No
	/// operand and no result. The regions are the clauses the attributes state, in the order
	/// [`EventsAttrs::region_kinds`] lists.
	Events(Box<EventsAttrs>),
	/// The terminator of a loop body that delivers its iteration's result.
	Yield,
	/// The terminator that ends the iteration and continues the loop (`CONTINUE`).
	Next,
	/// The terminator that leaves the loop (`BREAK`).
	Break,
}

impl LogOp {
	/// The payload-free identity of the op. Total: no variant is left unnamed.
	pub(crate) fn tag(&self) -> LogTag {
		match self {
			LogOp::Request {
				..
			} => LogTag::Request,
			LogOp::Txn {
				..
			} => LogTag::Txn,
			LogOp::Stmt {
				..
			} => LogTag::Stmt,
			LogOp::Statement {
				..
			} => LogTag::Statement,
			LogOp::Source(_) => LogTag::Source,
			LogOp::Values {
				..
			} => LogTag::Values,
			LogOp::Match {
				..
			} => LogTag::Match,
			LogOp::MatchRead {
				..
			} => LogTag::MatchRead,
			LogOp::MatchUpdate(_) => LogTag::MatchUpdate,
			LogOp::MatchDelete {
				..
			} => LogTag::MatchDelete,
			LogOp::MatchInsert(_) => LogTag::MatchInsert,
			LogOp::MatchOutput(_) => LogTag::MatchOutput,
			LogOp::BindStep(_) => LogTag::BindStep,
			LogOp::Subquery => LogTag::Subquery,
			LogOp::Write(_) => LogTag::Write,
			LogOp::Assign(_) => LogTag::Assign,
			LogOp::Unset {
				..
			} => LogTag::Unset,
			LogOp::Content {
				..
			} => LogTag::Content,
			LogOp::DdlGuard {
				..
			} => LogTag::DdlGuard,
			LogOp::Define(_) => LogTag::Define,
			LogOp::Remove(_) => LogTag::Remove,
			LogOp::Alter(_) => LogTag::Alter,
			LogOp::Rebuild(_) => LogTag::Rebuild,
			LogOp::UnionAll => LogTag::UnionAll,
			LogOp::Join {
				..
			} => LogTag::Join,
			LogOp::Filter => LogTag::Filter,
			LogOp::PermissionFilter {
				..
			} => LogTag::PermissionFilter,
			LogOp::Split {
				..
			} => LogTag::Split,
			LogOp::Aggregate(_) => LogTag::Aggregate,
			LogOp::Distinct => LogTag::Distinct,
			LogOp::Sort(_) => LogTag::Sort,
			LogOp::Window(_) => LogTag::Window,
			LogOp::Limit {
				..
			} => LogTag::Limit,
			LogOp::Project(_) => LogTag::Project,
			LogOp::Fetch => LogTag::Fetch,
			LogOp::Timeout => LogTag::Timeout,
			LogOp::VersionScope => LogTag::VersionScope,
			LogOp::Only => LogTag::Only,
			LogOp::Explain {
				..
			} => LogTag::Explain,
			LogOp::Eval => LogTag::Eval,
			LogOp::Block => LogTag::Block,
			LogOp::Let(_) => LogTag::Let,
			LogOp::Return => LogTag::Return,
			LogOp::Loop {
				..
			} => LogTag::Loop,
			LogOp::If => LogTag::If,
			LogOp::Info {
				..
			} => LogTag::Info,
			LogOp::Sleep => LogTag::Sleep,
			LogOp::Use {
				..
			} => LogTag::Use,
			LogOp::Option {
				..
			} => LogTag::Option,
			LogOp::Access(_) => LogTag::Access,
			LogOp::Kill => LogTag::Kill,
			LogOp::Live(_) => LogTag::Live,
			LogOp::Subscriber(_) => LogTag::Subscriber,
			LogOp::Show(_) => LogTag::Show,
			LogOp::Choose(_) => LogTag::Choose,
			LogOp::WriteStage {
				..
			} => LogTag::WriteStage,
			LogOp::Events(_) => LogTag::Events,
			LogOp::Yield => LogTag::Yield,
			LogOp::Next => LogTag::Next,
			LogOp::Break => LogTag::Break,
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------------------------

/// How a transaction region begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TxnStart {
	/// `BEGIN` in the request text.
	Begin,
	/// The region continues a transaction a previous batch of the same stream began. Occurs only
	/// in a batch of an internal stream.
	Continued,
}

/// The transaction statement a slot of the request stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TxnControl {
	Begin,
	Commit,
	Cancel,
}

/// How a transaction region ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TxnRegionEnd {
	Commit,
	Cancel,
	/// The request ends before a `COMMIT` or `CANCEL`: the transaction is cancelled.
	Unterminated,
	/// The transaction stays open for the next batch of the stream. Occurs only in a batch of an
	/// internal stream.
	Open,
}

/// The kind of a transaction region.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TxnRegionMode {
	/// A transaction written in the request.
	Explicit {
		start: TxnStart,
		end: TxnRegionEnd,
		emission: Emission,
	},
	/// A transaction the client holds across requests. `id` names it in the session.
	Session {
		id: u32,
	},
}

// ---------------------------------------------------------------------------------------------
// Statement attributes
// ---------------------------------------------------------------------------------------------

/// What a `logical.source` reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SourceRefShape {
	/// A whole table.
	Table(TableName),
	/// A record id or a key range; the operand holds it.
	RecordId,
	/// A generated record set; the operand holds it.
	Mock,
	/// A parameter that resolves to a source when the statement runs; the operand holds it.
	Param,
	/// Any other value that resolves to a source when the statement runs; the operand holds it.
	Dynamic,
}

/// The attributes of a source op.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SourceAttrs {
	pub(crate) source_ref: SourceRefShape,
	/// The `WITH INDEX` or `WITH NOINDEX` hint, which constrains access-path selection but not the
	/// rows produced.
	pub(crate) with: Option<With>,
	/// `FOR UPDATE`: the read locks the records it returns.
	pub(crate) for_update: bool,
	/// Whether the table the read names may be defined by the time it runs although the catalog
	/// the statement is planned against does not define it: the statement writes, may change the
	/// catalog, or runs after a change its planning cannot know.
	pub(crate) may_be_defined: bool,
	/// Whether the read is nested in a statement that may change the catalog before it runs, so
	/// it enforces the definitions it meets when it runs rather than any the bind inlined.
	pub(crate) may_meet_change: bool,
	/// Whether schema resolution gave the read its table's schema, so that the table's permission,
	/// computed fields and field guards stand as the nodes above it wherever the statement needs
	/// them, and the read itself enforces nothing.
	pub(crate) resolved: bool,
}

/// The attributes of a `MATCH` update stage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchUpdateAttrs {
	pub(crate) target: BindingId,
	pub(crate) data: MatchData,
}

/// The attributes of a `MATCH` insert stage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchInsertAttrs {
	pub(crate) nodes: Box<[MatchInsertNode]>,
	pub(crate) edges: Box<[MatchInsertEdge]>,
}

/// The attributes of a write op.
///
/// A statement's `TIMEOUT` is the operand of the `logical.timeout` op whose region holds the
/// write; a write carries none.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WriteAttrs {
	pub(crate) form: WriteForm,
	pub(crate) only: bool,
	pub(crate) output: OutputShape,
	pub(crate) with: Option<With>,
	pub(crate) explain: Option<Explain>,
	pub(crate) flags: WriteFlags,
	pub(crate) body: WriteBody,
	pub(crate) gates: WriteGates,
	pub(crate) image: WriteImage,
	pub(crate) stored: WriteStored,
	pub(crate) derived: WriteDerived,
	/// The `INTO` expression of an `INSERT` as written, which the refusal of a value that names no
	/// table quotes. `None` for a statement with no `INTO`.
	pub(crate) into_text: Option<Box<str>>,
	/// For a write that names no one table, or whose stages are [`Self::linked`], the statement as
	/// written, which the stage bundle of each table it writes binds again with that table
	/// ([`UnitKind::StageBundle`](crate::exec::ir::unit::UnitKind)). `None` for a write whose
	/// stages are its own ops.
	pub(crate) statement: Option<std::sync::Arc<crate::expr::logical::LogicalWrite>>,
	/// For a write that names its one table and runs the bundle of that table's stages the compile
	/// links before the statement runs, the table and the stages of the write's orders. The
	/// subject loop's body holds no stage op. `None` for any other write.
	pub(crate) linked: Option<Box<LinkedStages>>,
}

/// The stages of a write that runs them as the bundle the compile links for its one table
/// ([`WriteAttrs::linked`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LinkedStages {
	/// The table the write writes.
	pub(crate) table: TableName,
	/// The stages of the write's orders, which the positions of its body name.
	pub(crate) run: Box<[WriteStageKind]>,
}

/// The `COMPUTED` fields the stages of a write derive onto a copy of a record's image, by the
/// stage that derives them.
///
/// A stage that derives hosts one `FieldFrame(Computed)` region per field it lists, after the
/// regions it hosts for its own clause, in the order listed: the region's value is the field's
/// value over the copy, with the fields listed before it set. The order, and which fields a stage
/// derives, are decided at bind and stated here, and are never read back from the regions.
///
/// A permission predicate derives the computed fields it reads, and those they read in turn, in
/// the table's derivation order; the regions of a gate stage are evaluated in the gate's derived
/// frame (reads unchecked, writes refused, under the field's `AUTH LIMIT`). `observed` is every
/// computed field of the table, in derivation order, which `MaterialiseObservedFields` derives
/// onto both images of an observed record in the computed frame (reads checked, writes refused).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteDerived {
	/// The fields the predicate of `PERMISSIONS FOR create` derives.
	pub(crate) create: Box<[DerivedField]>,
	/// The fields the predicate of `PERMISSIONS FOR update` derives, before and after the update.
	pub(crate) update: Box<[DerivedField]>,
	/// The fields the predicate of `PERMISSIONS FOR delete` derives.
	pub(crate) delete: Box<[DerivedField]>,
	/// The fields the predicate of `PERMISSIONS FOR select` derives.
	pub(crate) select: Box<[DerivedField]>,
	/// Every computed field of the table, which the images of an observed record are given.
	/// `None` where the catalog does not state them: the write names no one table, the answer
	/// predates a catalog change, or a computed field has no logical form or no single name.
	pub(crate) observed: Option<Box<[DerivedField]>>,
}

/// One `COMPUTED` field a stage derives: the field its value lands under, and the facts of its
/// definition the value is derived by. The body and the `AUTH LIMIT` are the region and the frame
/// of the region that evaluates it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DerivedField {
	/// The field the value lands under: the one name a record holds the field by, which another
	/// clause reads it by too.
	pub(crate) name: Strand,
	/// The kind the value is coerced to, when the field declares one.
	pub(crate) kind: Option<Kind>,
	/// Whether the body holds a statement, a closure or a call of a user-defined function, each
	/// of which is lowered for the principal that runs it.
	pub(crate) caller_bound: bool,
}

/// What the `PERMISSIONS FOR` clauses of the table a write names say of each kind of record the
/// write touches.
///
/// The gate stages of the write judge these clauses, and the gate stages decide nothing else
/// about them: whether a clause is `FULL`, `NONE` or a predicate is a fact of the table's
/// definition at the version the request is bound to, stated here and never read back from the
/// presence of a region. A predicate is the `PermissionFrame` region its gate stage hosts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteGates {
	/// `PERMISSIONS FOR create`: judged on the record a write creates.
	pub(crate) create: GateClause,
	/// `PERMISSIONS FOR update`: judged on the record a write updates, before and after.
	pub(crate) update: GateClause,
	/// `PERMISSIONS FOR delete`: judged on the record a write deletes.
	pub(crate) delete: GateClause,
	/// `PERMISSIONS FOR select`: judged on the image a write shows.
	pub(crate) select: GateClause,
}

/// One `PERMISSIONS FOR` clause of the written table, as its gate stage judges it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum GateClause {
	/// Every caller is admitted.
	Full,
	/// No caller is admitted but one whose writes are not permission-checked.
	None,
	/// The table is not defined, so the write defines it on demand and every permission is
	/// `NONE`. A target-level refusal does not apply to such a table, because the target holds no
	/// record to refuse.
	Absent,
	/// A caller is admitted when the predicate the gate stage hosts is truthy over the record.
	/// `derives` is whether the predicate may read a `COMPUTED` field of the table, which the
	/// judgement derives onto a copy of the record first: `false` for a table with none, and for a
	/// predicate whose reads are all named and none is one.
	Predicate {
		derives: bool,
	},
	/// The write does not name one table at bind time, so its own container states no clause: the
	/// gate stages run in the unit planned for each table the write reaches.
	#[default]
	Dynamic,
	/// The catalog changed before this statement, so the definition read does not describe the
	/// table the write runs against: the statement is planned again when it is reached.
	LateBound,
	/// The stored clause has no logical form.
	Unlowerable,
}

/// What the field declarations of the table a write names say about the images of a record the
/// write shows and reads.
///
/// The stages that show a record, evaluate a clause over it or settle its id read these facts and
/// decide nothing about them: whether the table's field view derives a value, cuts a field or
/// leaves an image as it is, whether its record ids are made of named components, and whether
/// its `id` field declares a `DEFAULT` are facts of the definition at the version the request is
/// bound to, stated here. When [`WriteImage::view`] is [`ImageView::Dynamic`] or
/// [`ImageView::LateBound`] the other facts are not stated, and a lowering refuses before it reads
/// them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteImage {
	/// What the table's field view does to an image.
	pub(crate) view: ImageView,
	/// Whether the `id` field names the components of the table's record ids, so that a record is
	/// judged against its key.
	pub(crate) keyed: bool,
	/// Whether the `id` field declares a `DEFAULT`.
	pub(crate) id_default: bool,
}

/// What the table a write names stores that the stages of the write run: its field declarations,
/// the references those declare, its indexes, its events and its views.
///
/// A stage that maintains one of these decides nothing about whether the table has any: that is a
/// fact of the definitions at the version the request is bound to, stated here, and a stage never
/// reads it from the absence of a clause region. Only a table that stores nothing of a kind has a
/// stage that lowers without the clauses of that kind; a table that stores some
/// ([`Stored::Declared`]) has its stage's clauses lowered from regions of the stage op. When
/// [`WriteStored::table`] is `None` or a fact is [`Stored::Dynamic`] or [`Stored::LateBound`], the
/// facts are not stated, and a lowering refuses before it reads them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteStored {
	/// The one table the write names at bind time, which the instructions of its stages name.
	pub(crate) table: Option<TableName>,
	/// Whether the table is `SCHEMAFULL`. A table the catalog does not define is not.
	pub(crate) schemafull: bool,
	/// Whether the statement cannot change the catalog: nothing it runs defines or removes a
	/// definition, so the definitions read at bind time are the ones its records meet.
	pub(crate) catalog_fixed: bool,
	/// The field declarations, the virtual fields of a keyed table's id components among them.
	pub(crate) fields: Stored,
	/// The `REFERENCE` fields among the field declarations.
	pub(crate) references: Stored,
	/// The index definitions, those marked for removal among them.
	pub(crate) indexes: Stored,
	/// The event definitions.
	pub(crate) events: Stored,
	/// The view tables the table's views fill.
	pub(crate) views: Stored,
	/// The `REFERENCE` fields, of any table of the database, that can name a record of the table.
	/// Stated only by a write whose order purges a record, which repairs each of them: `Nothing`
	/// for every other write.
	pub(crate) referenced: Stored,
	/// The code the purge of a record runs through the edges it deletes: `Declared` when the
	/// table can hold edges itself (it is not `TYPE NORMAL`, which a table holding an edge cannot
	/// become), so the purge deletes records of the table, or when a table of the database that
	/// can hold edges stores code a delete runs
	/// ([`SemanticCatalog::edge_code`](crate::exec::ir::iface::catalog::SemanticCatalog::edge_code)).
	/// Stated only by a write whose order purges a record:
	/// `Nothing` for every other write.
	pub(crate) edges: Stored,
	/// What the stage that applies the field declarations runs. Empty unless `fields` is
	/// [`Stored::Declared`].
	pub(crate) schema: WriteSchema,
	/// The `REFERENCE` fields, by the idiom the catalog names each by, in the order the field
	/// declarations hold them. Empty unless `references` is [`Stored::Declared`].
	pub(crate) reference_fields: Box<[Idiom]>,
	/// The `REFERENCE` fields that keep a stored list of their referencing records
	/// (`MATERIALIZE INTO`), in the order the field declarations hold them. Empty unless
	/// `references` is [`Stored::Declared`].
	pub(crate) materialized: Box<[WriteMaterialized]>,
	/// What the stage that maintains the indexes runs. Empty unless `indexes` is
	/// [`Stored::Declared`].
	pub(crate) index_set: WriteIndexes,
	/// What the stage that maintains the views the table feeds runs. Empty unless `views` is
	/// [`Stored::Declared`].
	pub(crate) view_set: WriteViews,
	/// What the stage that purges a deleted record repairs. Empty unless `referenced` is
	/// [`Stored::Declared`].
	pub(crate) repair_set: WriteRepairs,
	/// How the table's field view is applied to an image the write shows or reads. Stated only
	/// for a table whose [`WriteImage::view`] is [`ImageView::Cut`] or [`ImageView::Derived`]:
	/// [`ImageLayers::Unstated`] for every other write.
	pub(crate) image_layers: ImageLayers,
}

/// A `REFERENCE` field with a `MATERIALIZE INTO` clause: the list of the records that reference
/// each record of `table` through `field`, kept in `dest`. The `WHERE` condition is read from the
/// field's definition when the unit that judges it is planned.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WriteMaterialized {
	/// The referencing field of the written table, by the idiom a reference key names it by.
	pub(crate) field: Idiom,
	/// The referenced table that holds the list.
	pub(crate) table: TableName,
	/// The top-level field of `table` that holds the list.
	pub(crate) dest: Idiom,
	/// Whether the clause has a `WHERE` condition.
	pub(crate) filtered: bool,
}

/// How the field view of the table a write names is applied to the images of a record the write
/// shows and reads, for the principal the request is planned for.
///
/// The layers are the resolver layers the planner binds over `SELECT * FROM t` for that
/// principal, bottom first. A write that has them hosts two regions for each column of each
/// layer on its own op, after its `RETURN` region and in the order of the layers: the read of the
/// field the column lands under, then the column's expression in its frame, as the regions of a
/// `phys.layer`. The regions are never read as layers the attribute does not state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum ImageLayers {
	/// Not stated: the view is plain, the write names no one table, or the definitions read
	/// predate a catalog change.
	#[default]
	Unstated,
	/// The layers. Empty when the view changes nothing for the principal: the table's cut fields
	/// belong to a caller whose reads are not permission-checked.
	Stack(Vec<SubscriberLayer>),
	/// The view has no layer form, so the host puts an image through the table's field
	/// declarations as it shows it.
	Enforced,
}

impl ImageLayers {
	/// The kind of each region the layers hold, in position order.
	pub(crate) fn region_kinds(&self) -> impl Iterator<Item = RegionKind> + '_ {
		let layers: &[SubscriberLayer] = match self {
			Self::Unstated | Self::Enforced => &[],
			Self::Stack(layers) => layers,
		};
		layers.iter().flat_map(|layer| {
			let columns = layer.fields.regions();
			let judged = layer.judged.as_deref().map_or(0, ComputedReadsShape::derived_regions);
			std::iter::repeat_n(RegionKind::Clause(ClauseRole::Projection), columns).chain(
				std::iter::repeat_n(RegionKind::FieldFrame(FieldRole::Computed), columns + judged),
			)
		})
	}
}

impl WriteAttrs {
	/// The kind of each region of the op, in position order: the `RETURN` clause's, then the
	/// field view's layers'.
	pub(crate) fn region_kinds(&self) -> Vec<RegionKind> {
		let mut kinds = Vec::new();
		if self.output == OutputShape::Fields {
			kinds.push(RegionKind::Clause(ClauseRole::ReturnClause));
		}
		kinds.extend(self.stored.image_layers.region_kinds());
		kinds
	}
}

/// The repairs `PurgeRecordData` makes of the records that reference a deleted record, as the
/// `REFERENCE` fields of the database say at the version the request is bound to.
///
/// The stage decides none of this. The tables and fields are the ones the instructions repair
/// from, each by the strategy its `ON DELETE` clause directs, and a field's `THEN` expression is
/// a region of the stage op, of kind `Clause(ReferenceRepair)`: for each field whose strategy is
/// [`RepairKind::Custom`], in the order of `tables` and then of each table's fields, one region.
/// A region is never read as a repair the fields do not state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteRepairs {
	/// Every table that defines a field which can name a record of the written table, in the
	/// order the database holds its tables, with those fields.
	pub(crate) tables: Box<[RepairTable]>,
	/// Whether a `THEN` expression has no form the lowering can run: it has no logical form, or it
	/// holds a write no operator runs. Such a stage states no region.
	pub(crate) unlowerable: bool,
}

impl WriteRepairs {
	/// How many regions of kind `Clause(ReferenceRepair)` the stage op that purges a record hosts
	/// after its own.
	pub(crate) fn region_count(&self) -> usize {
		if self.unlowerable {
			return 0;
		}
		self.tables
			.iter()
			.flat_map(|table| table.fields.iter())
			.filter(|field| field.strategy == RepairKind::Custom)
			.count()
	}
}

/// The `REFERENCE` fields one table defines that can name a record of the written table.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RepairTable {
	/// The table that defines the fields.
	pub(crate) table: TableName,
	/// The fields, in the order the table holds them.
	pub(crate) fields: Box<[RepairField]>,
}

/// One `REFERENCE` field and what its `ON DELETE` clause directs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RepairField {
	/// The field, by the idiom a reference key names it by.
	pub(crate) name: Idiom,
	/// What the clause directs.
	pub(crate) strategy: RepairKind,
}

/// What a `REFERENCE` field's `ON DELETE` clause does to a record that references the deleted one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RepairKind {
	/// `IGNORE`: the reference is left as it is.
	Ignore,
	/// `REJECT`: the delete is refused.
	Reject,
	/// `CASCADE`: the referencing record is deleted.
	Cascade,
	/// `UNSET`: the deleted record is taken out of the field.
	Unset,
	/// `THEN`: the expression of the clause's region is evaluated over the referencing record.
	Custom,
}

/// The indexes `StoreIndexData` keeps in line with the images of each record, as the table's
/// definitions say at the version the request is bound to.
///
/// The stage decides none of this. The definitions are the ones the instructions maintain from and
/// the run compares against the table's definitions when the record is written. The column
/// expressions and the `COUNT` guard of every index that is not marked for removal are regions of
/// the stage op, of kind `Clause(IndexColumn)`: for each such index in the order of `defs`, one
/// region per column, as the index reads a record id component, and then one for the guard of a
/// `COUNT` index that has one. A region is never read as an index the definitions do not state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteIndexes {
	/// Every index definition of the table, those marked for removal among them, in the order the
	/// catalog holds them.
	pub(crate) defs: IndexDefs,
	/// Whether an index has a column or a guard with no form the lowering can run: it has no
	/// logical form, or it holds a write no operator runs. Such a stage states no region.
	pub(crate) unlowerable: bool,
}

impl WriteIndexes {
	/// How many regions of kind `Clause(IndexColumn)` the stage op that maintains the indexes hosts
	/// after its own: the columns and guards of the indexes not marked for removal.
	pub(crate) fn region_count(&self) -> usize {
		if self.unlowerable {
			return 0;
		}
		self.defs
			.iter()
			.filter(|index| !index.prepare_remove)
			.map(|index| index.cols.len() + usize::from(index.count_cond.is_some()))
			.sum()
	}
}

/// The views `ProcessTableViews` keeps in step with the images of each record, as the definitions
/// of the view tables the written table feeds say at the version the request is bound to.
///
/// The stage decides none of this. The definitions are the ones the instructions maintain from and
/// the run compares against the table's views when the record is written. The clauses of each view
/// are regions of the stage op, of kind `Clause(ViewClause)`, bound over one image in the frame of
/// a view: for each definition in the order of `defs`, and for a view that is maintained:
///
/// - a materialized view hosts its condition when it has one, then its projection, whose region
///   holds the projection of the view's fields over the image;
/// - an aggregated view hosts its condition when it has one, then each `GROUP BY` expression in the
///   analysis's order, then each aggregate argument, then, for each field the analysis lands in the
///   group's row, the field's expression followed by one region for every subscript and filter its
///   name embeds, in the order the name's parts hold them;
/// - a view that is computed when read hosts nothing.
///
/// A region is never read as a clause the definitions do not state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteViews {
	/// Every view table the written table feeds, in the order the catalog holds them.
	pub(crate) defs: TableDefs,
	/// Whether a view has a clause with no form the lowering can run: it has no logical form, it
	/// holds a write no operator runs, or it selects a bare `VALUE`. Such a stage states no
	/// region.
	pub(crate) unlowerable: bool,
}

impl WriteViews {
	/// How many regions of kind `Clause(ViewClause)` the stage op that maintains the views hosts
	/// after its own.
	pub(crate) fn region_count(&self) -> usize {
		if self.unlowerable {
			return 0;
		}
		self.defs.iter().map(view_regions).sum()
	}
}

/// How many clause regions the maintenance of the view table `table` hosts.
fn view_regions(table: &TableDefinition) -> usize {
	match &table.view {
		Some(ViewDefinition::Materialized {
			condition,
			..
		}) => usize::from(condition.is_some()) + 1,
		Some(ViewDefinition::Aggregated {
			analysis,
			condition,
			..
		}) => {
			let fields = match &analysis.fields {
				AggregateFields::Fields(items) => {
					items.iter().map(|(name, _)| 1 + embedded_parts(name)).sum()
				}
				AggregateFields::Value(_) => 0,
			};
			usize::from(condition.is_some())
				+ analysis.group_expressions.len()
				+ analysis.aggregate_arguments.len()
				+ fields
		}
		Some(ViewDefinition::Select {
			..
		})
		| None => 0,
	}
}

/// How many parts of the place `name` embed an expression: a subscript or a filter.
pub(crate) fn embedded_parts(name: &Idiom) -> usize {
	name.0.iter().filter(|part| matches!(part, Part::Value(_) | Part::Where(_))).count()
}

/// The definitions of the view tables a table feeds, as the catalog holds them.
///
/// An op attribute reads by value: two sets are equal when their definitions are, and the set
/// hashes, prints and parses through the definitions' stored form, which is revisioned and holds
/// every field a definition has, as [`IndexDefs`] does.
#[derive(Clone, Debug, Default)]
pub(crate) struct TableDefs(Arc<[TableDefinition]>);

impl TableDefs {
	/// The set of the definitions `defs`.
	pub(crate) fn new(defs: Arc<[TableDefinition]>) -> Self {
		Self(defs)
	}

	/// The definitions, in the order the catalog holds them.
	pub(crate) fn definitions(&self) -> &Arc<[TableDefinition]> {
		&self.0
	}

	/// Iterates the definitions.
	pub(crate) fn iter(&self) -> std::slice::Iter<'_, TableDefinition> {
		self.0.iter()
	}

	/// The stored form of each definition, as bytes.
	fn stored(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
		self.0.iter().map(|table| {
			// A stored definition is plain data, so encoding it cannot fail; the empty encoding
			// stands for one that does, and no definition reads back from it.
			surrealdb_kvs::value::KVValue::kv_encode_value(&table.to_stored()).unwrap_or_default()
		})
	}

	/// The text of the set: the stored form of each definition in hexadecimal, separated by `,`.
	pub(crate) fn to_text(&self) -> String {
		use std::fmt::Write as _;
		let mut text = String::new();
		for (position, bytes) in self.stored().enumerate() {
			if position > 0 {
				text.push(',');
			}
			for byte in bytes {
				let _ = write!(text, "{byte:02x}");
			}
		}
		text
	}

	/// The set `text` holds, as [`TableDefs::to_text`] writes it.
	pub(crate) fn from_text(text: &str) -> Result<Self, String> {
		use crate::catalog::{FromStored, StoredTableDefinition};
		if text.is_empty() {
			return Ok(Self::default());
		}
		let mut defs = Vec::new();
		for part in text.split(',') {
			let digits = part.as_bytes();
			if digits.len() % 2 != 0 {
				return Err("a table definition has an odd number of hexadecimal digits".into());
			}
			let bytes = digits
				.chunks(2)
				.map(|pair| {
					std::str::from_utf8(pair)
						.ok()
						.and_then(|pair| u8::from_str_radix(pair, 16).ok())
						.ok_or_else(|| "a table definition is not hexadecimal".to_owned())
				})
				.collect::<Result<Vec<u8>, _>>()?;
			let stored: StoredTableDefinition =
				surrealdb_kvs::value::KVValue::kv_decode_value(&bytes, ())
					.map_err(|error| format!("a table definition does not decode: {error}"))?;
			defs.push(
				TableDefinition::from_stored(&stored)
					.map_err(|error| format!("a table definition does not compile: {error}"))?,
			);
		}
		Ok(Self(defs.into()))
	}
}

impl PartialEq for TableDefs {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
	}
}

impl Eq for TableDefs {}

impl std::hash::Hash for TableDefs {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.0.len().hash(state);
		for bytes in self.stored() {
			bytes.hash(state);
		}
	}
}

/// The index definitions of a table, as the catalog holds them.
///
/// An op attribute reads by value: two sets are equal when their definitions are, and the set
/// hashes, prints and parses through the definitions' stored form, which is revisioned and holds
/// every field a definition has. The runtime form is rebuilt from the stored one, so a set read
/// back from its text equals the set that was printed.
#[derive(Clone, Debug, Default)]
pub(crate) struct IndexDefs(Arc<[IndexDefinition]>);

impl IndexDefs {
	/// The set of the definitions `defs`.
	pub(crate) fn new(defs: Arc<[IndexDefinition]>) -> Self {
		Self(defs)
	}

	/// The definitions, in the order the catalog holds them.
	pub(crate) fn definitions(&self) -> &Arc<[IndexDefinition]> {
		&self.0
	}

	/// Iterates the definitions.
	pub(crate) fn iter(&self) -> std::slice::Iter<'_, IndexDefinition> {
		self.0.iter()
	}

	/// The stored form of each definition, as bytes.
	fn stored(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
		self.0.iter().map(|index| {
			// A stored definition is plain data, so encoding it cannot fail; the empty encoding
			// stands for one that does, and no definition reads back from it.
			surrealdb_kvs::value::KVValue::kv_encode_value(&index.to_stored()).unwrap_or_default()
		})
	}

	/// The text of the set: the stored form of each definition in hexadecimal, separated by `,`.
	pub(crate) fn to_text(&self) -> String {
		use std::fmt::Write as _;
		let mut text = String::new();
		for (position, bytes) in self.stored().enumerate() {
			if position > 0 {
				text.push(',');
			}
			for byte in bytes {
				let _ = write!(text, "{byte:02x}");
			}
		}
		text
	}

	/// The set `text` holds, as [`IndexDefs::to_text`] writes it.
	pub(crate) fn from_text(text: &str) -> Result<Self, String> {
		use crate::catalog::{FromStored, StoredIndexDefinition};
		if text.is_empty() {
			return Ok(Self::default());
		}
		let mut defs = Vec::new();
		for part in text.split(',') {
			let digits = part.as_bytes();
			if digits.len() % 2 != 0 {
				return Err("an index definition has an odd number of hexadecimal digits".into());
			}
			let bytes = digits
				.chunks(2)
				.map(|pair| {
					std::str::from_utf8(pair)
						.ok()
						.and_then(|pair| u8::from_str_radix(pair, 16).ok())
						.ok_or_else(|| "an index definition is not hexadecimal".to_owned())
				})
				.collect::<Result<Vec<u8>, _>>()?;
			let stored: StoredIndexDefinition =
				surrealdb_kvs::value::KVValue::kv_decode_value(&bytes, ())
					.map_err(|error| format!("an index definition does not decode: {error}"))?;
			defs.push(
				IndexDefinition::from_stored(&stored)
					.map_err(|error| format!("an index definition does not compile: {error}"))?,
			);
		}
		Ok(Self(defs.into()))
	}
}

impl PartialEq for IndexDefs {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
	}
}

impl Eq for IndexDefs {}

impl std::hash::Hash for IndexDefs {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.0.len().hash(state);
		for bytes in self.stored() {
			bytes.hash(state);
		}
	}
}

/// The event definitions of a table, as the catalog holds them.
///
/// An op attribute reads by value: two sets are equal when their definitions are, and the set
/// hashes, prints and parses through the definitions' stored form, which is revisioned and holds
/// every field a definition has, as [`IndexDefs`] does.
#[derive(Clone, Debug, Default)]
pub(crate) struct EventDefs(Arc<[EventDefinition]>);

impl EventDefs {
	/// The set of the definitions `defs`.
	pub(crate) fn new(defs: Arc<[EventDefinition]>) -> Self {
		Self(defs)
	}

	/// The definitions, in the order the catalog holds them.
	pub(crate) fn definitions(&self) -> &Arc<[EventDefinition]> {
		&self.0
	}

	/// Iterates the definitions.
	pub(crate) fn iter(&self) -> std::slice::Iter<'_, EventDefinition> {
		self.0.iter()
	}

	/// The stored form of each definition, as bytes.
	fn stored(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
		self.0.iter().map(|event| {
			// A stored definition is plain data, so encoding it cannot fail; the empty encoding
			// stands for one that does, and no definition reads back from it.
			surrealdb_kvs::value::KVValue::kv_encode_value(event.stored()).unwrap_or_default()
		})
	}

	/// The text of the set: the stored form of each definition in hexadecimal, separated by `,`.
	pub(crate) fn to_text(&self) -> String {
		use std::fmt::Write as _;
		let mut text = String::new();
		for (position, bytes) in self.stored().enumerate() {
			if position > 0 {
				text.push(',');
			}
			for byte in bytes {
				let _ = write!(text, "{byte:02x}");
			}
		}
		text
	}

	/// The set `text` holds, as [`EventDefs::to_text`] writes it.
	pub(crate) fn from_text(text: &str) -> Result<Self, String> {
		use crate::catalog::{FromStored, StoredEventDefinition};
		if text.is_empty() {
			return Ok(Self::default());
		}
		let mut defs = Vec::new();
		for part in text.split(',') {
			let digits = part.as_bytes();
			if digits.len() % 2 != 0 {
				return Err("an event definition has an odd number of hexadecimal digits".into());
			}
			let bytes = digits
				.chunks(2)
				.map(|pair| {
					std::str::from_utf8(pair)
						.ok()
						.and_then(|pair| u8::from_str_radix(pair, 16).ok())
						.ok_or_else(|| "an event definition is not hexadecimal".to_owned())
				})
				.collect::<Result<Vec<u8>, _>>()?;
			let stored: StoredEventDefinition =
				surrealdb_kvs::value::KVValue::kv_decode_value(&bytes, ())
					.map_err(|error| format!("an event definition does not decode: {error}"))?;
			defs.push(
				EventDefinition::from_stored(&stored)
					.map_err(|error| format!("an event definition does not compile: {error}"))?,
			);
		}
		Ok(Self(defs.into()))
	}
}

impl PartialEq for EventDefs {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
	}
}

impl Eq for EventDefs {}

impl std::hash::Hash for EventDefs {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.0.len().hash(state);
		for bytes in self.stored() {
			bytes.hash(state);
		}
	}
}

/// What evaluating one clause of an event is, beside the region that holds its body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct EventClause {
	/// The most the clause spends of the computation-depth budget at once anywhere in it: the
	/// budget left must cover this for the clause to be evaluated.
	pub(crate) deepest: u8,
	/// What the clause spends on the way to its shallowest write, which entering the clause
	/// spends and a record the clause writes resumes the count from; nothing for a clause that
	/// writes nothing itself.
	pub(crate) write: u8,
	/// Whether the body holds a statement, a closure or a call of a user-defined function, each
	/// of which is lowered for the principal that runs it.
	pub(crate) caller_bound: bool,
}

/// One event of a table: its `WHEN` clause and, for a synchronous event, its `THEN` clauses in
/// the order they run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct EventShape {
	/// The `WHEN` clause.
	pub(crate) when: EventClause,
	/// The `THEN` clauses. An `ASYNC` event's run in the drain, from the stored definition, and
	/// are not clauses of the program: it states none.
	pub(crate) then: Box<[EventClause]>,
	/// Whether the `WHEN` clause is never truthy for the action the op is for
	/// ([`when_never_fires`](super::stages::when_never_fires)), so that no record fires the event.
	/// Stated only by the physical write-prune pass: `false` before the physical phase. A
	/// dormant event's clauses stay regions of the op, whole, and its definition stays in
	/// [`EventsAttrs::defs`].
	pub(crate) dormant: bool,
}

/// The events of one table that fire for one action, as the table's definitions say at the version
/// the request is bound to.
///
/// The stage bundle unit that runs them decides none of this. The definitions are the ones the run
/// compares against the table's definitions when a record is written, so a program never runs
/// against events it was not planned from. The clauses are regions of the op, each of kind
/// `EventFrame` and bound to the names of an event's frame: for each event in the order of
/// `defs`, its `WHEN` and then each of its `THEN` clauses. A region is never read as a clause the
/// shape does not state.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct EventsAttrs {
	/// The action the order is for, which `$event` names and which decides the image an event is
	/// about.
	pub(crate) action: Action,
	/// Every event definition of the table, in the order the catalog holds them.
	pub(crate) defs: EventDefs,
	/// The clauses of each event of `defs`, in the same order. Empty when `unlowerable`.
	pub(crate) events: Box<[EventShape]>,
	/// Whether a clause has no form the lowering can run: it has no logical form, or it holds a
	/// write no operator runs. Such an op states no clause and no region.
	pub(crate) unlowerable: bool,
}

impl EventsAttrs {
	/// The kind of each region of the op, in position order.
	pub(crate) fn region_kinds(&self) -> Vec<RegionKind> {
		let clauses = self.events.iter().map(|event| 1 + event.then.len()).sum::<usize>();
		vec![RegionKind::EventFrame; clauses]
	}

	/// Why the clauses the op states are not the ones its definitions have, or `None` when they
	/// are: one shape for each definition, and a `THEN` clause for each of a synchronous event's.
	pub(crate) fn fault(&self) -> Option<&'static str> {
		if self.unlowerable {
			return match self.events.is_empty() {
				true => None,
				false => Some("events that no clause can be lowered from state clauses"),
			};
		}
		if self.events.len() != self.defs.definitions().len() {
			return Some("the events state a shape for a definition other than each of theirs");
		}
		let agree = self.defs.iter().zip(self.events.iter()).all(|(def, shape)| {
			shape.then.len()
				== if def.is_async() {
					0
				} else {
					def.then.len()
				}
		});
		match agree {
			true => None,
			false => Some("an event states other `THEN` clauses than its definition runs"),
		}
	}
}

/// The field schema `ProcessTableFields` applies to each record of the written table, in the order
/// it applies it, as the table's definitions say at the version the request is bound to.
///
/// The stage decides none of this: the field list, the order the fields are produced in, which
/// clauses a field has and what its `PERMISSIONS` say are facts of the definitions, stated here. A
/// clause is the region of the stage op that holds it, of the kinds and in the order
/// [`WriteSchema::region_counts`] lists, and a region is never read as a clause the schema does not
/// state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteSchema {
	/// Every field of the table, in the order the catalog holds them: the `COMPUTED` fields and
	/// the virtual fields of a keyed table's `id` among them, each by the path the catalog names
	/// it by. The instructions refuse a record when the definitions its write's context reads are
	/// not exactly these, so a field the stage has no clause for is listed too.
	pub(crate) fields: Box<[Strand]>,
	/// The position of the `id` field among `fields`.
	pub(crate) id_field: Option<u16>,
	/// The fields whose producing clauses read each other in a cycle, named as the refusal names
	/// them, when they do. Every record is refused, and `steps` is empty.
	pub(crate) cycle: Option<Box<str>>,
	/// What the stage runs for each field, in the order the fields are produced: after the fields
	/// each one's clauses read, and after its parent.
	pub(crate) steps: Box<[FieldStep]>,
	/// The `COMPUTED` fields, by the name a record holds them under, which the stage strips from
	/// the record once every field is produced.
	pub(crate) computed: Box<[Strand]>,
	/// Whether a clause of a field has no form the lowering can run: it has no logical form, it
	/// holds a write no operator runs, or a `COMPUTED` field is not named by one field. Such a
	/// schema states no step.
	pub(crate) unlowerable: bool,
	/// Whether the `DEFAULT` of the `id` field holds a statement or a call of a user-defined
	/// function, which is planned when the statement is planned, for the principal that runs it.
	pub(crate) id_default_bound: bool,
}

/// One step of the field schema.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldStep {
	/// A `COMPUTED` field another field's clause reads, derived onto the record at its place in
	/// the order. The body is a `FieldFrame(Computed)` region, in the computed frame.
	Derive(DerivedField),
	/// A stored field: its clauses and what its `PERMISSIONS` say.
	Field(FieldClauses),
}

/// The clauses of a stored field the stage runs, each a region of the stage op.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FieldClauses {
	/// The position of the field among [`WriteSchema::fields`].
	pub(crate) index: u16,
	/// The `DEFAULT`, or the `VALUE` of a field that declares no `DEFAULT` when that `VALUE` is
	/// static: a `FieldFrame(Default)` region.
	pub(crate) default: Option<FieldClause>,
	/// The `VALUE`: a `FieldFrame(Value)` region.
	pub(crate) value: Option<FieldClause>,
	/// The `ASSERT`: a `FieldFrame(Assert)` region.
	pub(crate) assert: Option<FieldClause>,
	/// `PERMISSIONS FOR create`, judged for a record being created.
	pub(crate) create: FieldGate,
	/// `PERMISSIONS FOR update`, judged for a record that exists.
	pub(crate) update: FieldGate,
}

/// What the definition says of a clause of a field whose body is a region.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FieldClause {
	/// Whether the body holds a statement, a closure or a call of a user-defined function, each of
	/// which is lowered for the principal that runs it.
	pub(crate) caller_bound: bool,
}

/// A field's `PERMISSIONS FOR create` or `update`, as the stage judges a change against it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldGate {
	/// `FULL`: every change stands.
	Allow,
	/// `NONE`: no change stands.
	Deny,
	/// `WHERE p`: a change stands when the predicate answers truthy over the post-image, with the
	/// place's parameters bound and its reads not permission-checked. The predicate is a
	/// `PermissionFrame` region, followed in the order listed by the `COMPUTED` fields it
	/// derives onto the image first, one `FieldFrame(Computed)` region each.
	Predicate {
		/// Whether the predicate holds a statement, a closure or a call of a user-defined
		/// function.
		caller_bound: bool,
		/// The computed fields the predicate reads, in the table's derivation order.
		derives: Box<[DerivedField]>,
	},
}

impl WriteSchema {
	/// How many regions of each kind the stage op that applies the schema hosts after its own,
	/// in the order [`SchemaRegions::kinds`] lists them: the regions of one kind stand together,
	/// in the order of the steps.
	///
	/// - one `FieldFrame(Default)`, `FieldFrame(Value)` and `FieldFrame(Assert)` region for each
	///   field clause the steps state, in the order of the steps;
	/// - one `PermissionFrame` region for each predicate among a stored field's `create` then
	///   `update` clause, in the order of the steps;
	/// - one `FieldFrame(Computed)` region for each `Derive` step, in the order of the steps, then
	///   the fields each of those predicates derives, in the order of the predicates.
	pub(crate) fn region_counts(&self) -> SchemaRegions {
		let mut counts = SchemaRegions::default();
		for step in self.steps.iter() {
			match step {
				FieldStep::Derive(_) => counts.derive += 1,
				FieldStep::Field(field) => {
					counts.default += usize::from(field.default.is_some());
					counts.value += usize::from(field.value.is_some());
					counts.assert += usize::from(field.assert.is_some());
					for gate in [&field.create, &field.update] {
						if let FieldGate::Predicate {
							derives,
							..
						} = gate
						{
							counts.permission += 1;
							counts.predicate_derive += derives.len();
						}
					}
				}
			}
		}
		counts
	}
}

/// How many regions of each kind the stage that applies a [`WriteSchema`] hosts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SchemaRegions {
	pub(crate) default: usize,
	pub(crate) value: usize,
	pub(crate) assert: usize,
	pub(crate) permission: usize,
	/// The `FieldFrame(Computed)` regions of the `Derive` steps.
	pub(crate) derive: usize,
	/// The `FieldFrame(Computed)` regions of the predicates, after those of the `Derive` steps.
	pub(crate) predicate_derive: usize,
}

impl SchemaRegions {
	/// The kinds of the regions, in the order the stage op hosts them.
	pub(crate) fn kinds(self) -> Vec<RegionKind> {
		let mut kinds = Vec::new();
		let mut push = |kind, count| kinds.extend(std::iter::repeat_n(kind, count));
		push(RegionKind::FieldFrame(FieldRole::Default), self.default);
		push(RegionKind::FieldFrame(FieldRole::Value), self.value);
		push(RegionKind::FieldFrame(FieldRole::Assert), self.assert);
		push(RegionKind::PermissionFrame, self.permission);
		push(RegionKind::FieldFrame(FieldRole::Computed), self.derive + self.predicate_derive);
		kinds
	}
}

/// Whether a table stores definitions of one kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum Stored {
	/// The table stores none, which is also what a table the catalog does not define stores.
	Nothing,
	/// The table stores some.
	Declared,
	/// The write names no one table at bind time, so its own container states nothing: the stages
	/// run in the unit planned for each table the write reaches.
	#[default]
	Dynamic,
	/// The catalog changed before this statement, or does not answer, so the definitions read do
	/// not describe the table the write runs against: the statement is planned again when it is
	/// reached.
	LateBound,
}

/// What a table's field view does to an image of a record it shows or reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum ImageView {
	/// No field derives a value and none is cut: an image is its record, for every caller. A table
	/// the catalog does not define has no field, so it is plain.
	Plain,
	/// No field derives a value, but some field's `PERMISSIONS FOR select` is not `FULL`: the image
	/// of a caller whose reads are permission-checked is cut to the fields it may select, and the
	/// image of any other caller is plain.
	Cut,
	/// Some field is `COMPUTED`, so every image is derived, and cut as well for a caller whose
	/// reads are permission-checked when a field's select permission is not `FULL`.
	Derived,
	/// The write does not name one table at bind time, so its own container states no view: the
	/// stages run in the unit planned for each table the write reaches.
	#[default]
	Dynamic,
	/// The catalog changed before this statement, so the definition read does not describe the
	/// table the write runs against: the statement is planned again when it is reached.
	LateBound,
}

/// How the per-record body of a write selects between its orders, and what it does when a creating
/// attempt fails.
///
/// The subject loop's body holds each stage of the write once, the first order's stages in the
/// order it runs them. A write whose form has two orders (a creating and an updating one) shares
/// the stages the orders have in common, so the stage ops alone do not say which stage belongs to
/// which order. A body names, for each order, the positions of its stages among the stage ops of
/// the loop body, in the order that order runs them. The first order's positions ascend; the
/// second's need not, as an updating order may run stages it shares with the creating one in
/// another relative order.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct WriteBody {
	/// How the orders are selected between.
	pub(crate) branching: WriteBranching,
	/// The stages of the only order, or of the creating order of a form with two.
	pub(crate) first: Box<[u16]>,
	/// The stages of the updating order of a form with two; empty for a form with one.
	pub(crate) second: Box<[u16]>,
	/// Whether a table among the subjects names a record to create rather than the records stored
	/// there to change. A record id names a record to create without reading it only for `CREATE`;
	/// every other form reads the record it names.
	pub(crate) deferable: bool,
}

/// How a write chooses between the orders of its body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum WriteBranching {
	/// One order and no selection: `CREATE`, `UPDATE`, `DELETE`.
	#[default]
	Sole,
	/// The order is chosen from the record the subject names, before any order-specific stage
	/// runs, and a failure is a failure: `RELATE`.
	UpFront,
	/// The creating order is attempted behind a savepoint, and a classified failure is rolled back
	/// and run as an update: `UPSERT` and `INSERT`.
	Retry(WriteRetry),
	/// The order is the action a view's maintenance settled for the row: the write of a view row.
	/// The first order names every stage op, and the order of an action is the stages among them
	/// that carry no action or carry that one ([`view_row_order`](super::stages::view_row_order)).
	ByAction,
}

/// How a failed creating attempt is handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WriteRetry {
	/// How many stages of the creating order run before the savepoint is pushed.
	pub(crate) savepoint_after: u16,
	/// Whether a failure naming an existing record is run as an update.
	pub(crate) retry_conflict: bool,
	/// Whether a conflict that is not retried is skipped rather than raised.
	pub(crate) skip_failures: bool,
	/// Whether a schema failure is run as an update that carries the original failure.
	pub(crate) carry_schema_failure: bool,
}

/// The attributes of an assignment.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AssignAttrs {
	pub(crate) place: Place,
	pub(crate) op: AssignOperator,
}

/// The attributes of a `DEFINE`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DefineAttrs {
	pub(crate) object: DefineObject,
	pub(crate) mode: DefineKind,
	pub(crate) template: DefinitionTemplate,
	pub(crate) checks: DdlChecks,
}

/// The attributes of a `REMOVE`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RemoveAttrs {
	pub(crate) object: RemoveObject,
	pub(crate) if_exists: bool,
	/// `EXPUNGE`, which is meaningful for a namespace, a database and a table.
	pub(crate) expunge: bool,
	/// What the statement checks against the catalog when it is planned, for the forms that
	/// check: `REMOVE FIELD`. [`DdlChecks::None`] for every other form.
	pub(crate) checks: DdlChecks,
}

/// The attributes of an `ALTER`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AlterAttrs {
	pub(crate) object: AlterObject,
	pub(crate) if_exists: bool,
	/// The definition the statement forms and the fields of it that replace the stored
	/// definition's, for a form whose replacement is an encoded stored value (`ALTER PARAM`,
	/// `FUNCTION`, `BUCKET`, `MODULE` and `ANALYZER`); the whole config definition that
	/// `ALTER CONFIG` writes; and, for `ALTER API`, one object of stored values per clause and the
	/// types the clauses name. `None` for every other form.
	pub(crate) template: Option<AlterTemplate>,
	/// What the statement checks against the catalog when it is planned, for the forms that
	/// check: `ALTER FIELD`. [`DdlChecks::None`] for every other form.
	pub(crate) checks: DdlChecks,
}

/// The attributes of a `REBUILD INDEX`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RebuildAttrs {
	pub(crate) index: Strand,
	pub(crate) table: TableName,
	pub(crate) if_exists: bool,
	pub(crate) concurrently: bool,
}

/// The shape of a projection list: how each field lands in the output. The expressions are
/// `Clause(Projection)` regions, one per field in order.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldsShape {
	/// `SELECT VALUE e`: each row is the bare value. The alias names it for grouping and is not
	/// projected.
	Value {
		alias: Option<OutputPath>,
	},
	/// An object per row with one entry per field.
	Select(Box<[FieldShape]>),
}

impl FieldsShape {
	/// The number of `Clause(Projection)` regions the list holds: one for a `VALUE`, otherwise one
	/// for each field that is not `*`.
	pub(crate) fn regions(&self) -> usize {
		match self {
			FieldsShape::Value {
				..
			} => 1,
			FieldsShape::Select(list) => list
				.iter()
				.filter(|field| match field {
					FieldShape::All => false,
					FieldShape::Single {
						..
					} => true,
				})
				.count(),
		}
	}
}

/// One entry of a [`FieldsShape::Select`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldShape {
	/// `*`: the whole row. Holds no region.
	All,
	/// An expression, by the name it lands under and the kind its value is coerced to.
	Single {
		alias: Option<OutputPath>,
		kind: Option<Kind>,
	},
}

/// The `COMPUTED` fields of a table a permission predicate reads, as the binder decides them
/// from the table's schema ([`crate::exec::planner::fold::computed_reads`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ComputedReadsShape {
	pub(crate) table: TableName,
	/// The fields read by name, sorted, or `None` when the predicate reads the row other than by
	/// name, which derives every computed field of the table.
	pub(crate) fields: Option<Vec<String>>,
	/// The computed fields derived onto the copy of the row the predicate judges, decided at bind
	/// from the table's field definitions.
	pub(crate) derived: DerivedReads,
}

impl ComputedReadsShape {
	/// The number of `FieldFrame(Computed)` regions the op that holds this shape hosts after its
	/// predicate: one per derived field, none where the derivation is not stated.
	pub(crate) fn derived_regions(&self) -> usize {
		match &self.derived {
			DerivedReads::Stated(fields) => fields.len(),
			DerivedReads::LateBound | DerivedReads::Unlowerable => 0,
		}
	}
}

/// What the catalog states of the computed fields a permission predicate derives.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DerivedReads {
	/// The fields, in the table's derivation order: the computed
	/// fields the predicate reads, and those they read in turn, each after the fields it reads. A
	/// `FieldFrame(Computed)` region holds the body of each, in the order listed; the order and
	/// the identity of each field are stated here and never read from the regions.
	Stated(Box<[DerivedField]>),
	/// The catalog answer predates a catalog change of the request, or was not loaded: the
	/// statement is planned again when it is reached.
	LateBound,
	/// A computed field of the table has no logical form or is not named by one field, which no
	/// derivation can key.
	Unlowerable,
}

/// The attributes of an aggregation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AggregateShape {
	/// The number of group keys, each an `ErrorScope(ShapeToNone)` region. With none, the rows
	/// fall into one group (`GROUP ALL`).
	pub(crate) groups: u16,
	pub(crate) fields: FieldsShape,
	/// The name each field lands under in the group's object, one per `FieldShape::Single` in the
	/// order the fields list them: the alias where the field has one, otherwise the path its
	/// expression names, which is the expression's rendering as one flat key where the expression
	/// is no path.
	pub(crate) names: Vec<OutputPath>,
	/// The fields, by position, that take the value of the group's first row: a `MATCH` column
	/// that is neither a group key nor holds an aggregate call. A `SELECT` lists none; it collects
	/// each row's value of such a column.
	pub(crate) first_value: Vec<usize>,
}

/// The attributes of a projection op.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProjectAttrs {
	pub(crate) fields: FieldsShape,
	/// Whether the projection is a resolver layer: `*` followed by computed or permissioned
	/// columns evaluated in frames of their own.
	pub(crate) layer: bool,
	/// The number of `OMIT` paths; their `Clause(Projection)` regions follow the field regions.
	pub(crate) omit: u16,
	/// The name each unaliased field lands under, one per `FieldShape::Single` with no alias, in
	/// the order the fields list them: the path an expression that is a path reads, otherwise the
	/// expression's rendering as one flat key.
	pub(crate) names: Vec<OutputPath>,
	/// For a layer, the fields the schema of its input declares `COMPUTED`, in the schema's order:
	/// a layer column under one of these names is on a computed field whatever frame it runs in.
	/// Empty for any other projection.
	pub(crate) computed: Vec<Strand>,
	/// The fields, by position, whose value is a key of the sort that hands the projection its
	/// rows, each with the key's position: an `ORDER BY` naming the field's alias sorts by the
	/// field's expression, and the row's one evaluation of it is both what the row sorts by and
	/// what it returns. Only a key that is not a path is shared, so the sort beneath stays.
	pub(crate) sort_keys: Vec<(u16, u16)>,
	/// For a layer whose guards read `COMPUTED` fields of its table, those fields, derived onto a
	/// copy of the row the guards are judged against; `None` for any other projection. The layer
	/// hosts one `FieldFrame(Computed)` region per field the shape derives, after the frame
	/// regions of its columns ([`ComputedReadsShape::derived_regions`]).
	pub(crate) judged: Option<Box<ComputedReadsShape>>,
}

impl ProjectAttrs {
	/// The number of regions the projection hosts after its columns' frames: one per field its
	/// judged reads derive.
	pub(crate) fn judged_regions(&self) -> usize {
		self.judged.as_deref().map_or(0, ComputedReadsShape::derived_regions)
	}
}

/// One `ORDER BY` key's flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SortKeyShape {
	pub(crate) collate: bool,
	pub(crate) numeric: bool,
	pub(crate) ascending: bool,
	/// Whether the key is a path written over the row that names no field of the projection,
	/// which the sort reads from the row as it stands: a record link the path crosses is not
	/// followed, so a step past it reads `NONE`.
	pub(crate) path: bool,
}

/// How a sort orders its input.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SortShape {
	/// `ORDER BY RAND()`.
	Random,
	/// One key per `Clause(OrderKey)` region, in order.
	Keys(Box<[SortKeyShape]>),
}

/// The attributes of a sort op.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SortAttrs {
	pub(crate) ordering: SortShape,
	/// Whether the sort may spill to temporary files.
	pub(crate) tempfiles: bool,
}

/// The attributes of a window, which the logical and the physical op share.
///
/// The op reads its input in partition-then-order sequence: the rows of one partition are
/// adjacent, and within it they are ordered by the order keys. It appends one field per call to
/// each row and passes every row on, in the order it read them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WindowShape {
	/// How many of the leading `Clause(OrderKey)` regions are partition keys.
	pub(crate) partitions: u16,
	/// The flags of each order key, one per order-key region, in order.
	pub(crate) order: Box<[SortKeyShape]>,
	/// The calls, in the order their fields are appended. At least one.
	pub(crate) calls: Box<[WindowCallShape]>,
}

impl WindowShape {
	/// The number of key regions: the partition keys, then the order keys.
	pub(crate) fn key_regions(&self) -> usize {
		usize::from(self.partitions) + self.order.len()
	}

	/// The number of argument regions, over every call.
	pub(crate) fn arg_regions(&self) -> usize {
		self.calls.iter().map(|call| usize::from(call.args)).sum()
	}
}

/// One call of a [`WindowShape`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WindowCallShape {
	/// The field the call's value lands under on each row.
	pub(crate) output: Strand,
	pub(crate) function: WindowFunction,
	/// How many argument regions the call owns.
	pub(crate) args: u16,
	pub(crate) frame: WindowFrame,
}

/// Whether `LET` binds a local value or a session parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LetBinding {
	/// A value of the enclosing region.
	Local,
	/// A parameter of the session, which later requests read.
	Session,
}

/// The attributes of a `LET`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LetAttrs {
	pub(crate) name: Strand,
	pub(crate) binding: LetBinding,
	/// The kind a typed `LET` coerces its value to.
	pub(crate) ty: Option<Kind>,
}

/// What a `USE` statement sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UseTarget {
	Ns,
	Db,
	NsDb,
	/// The default namespace and database.
	Default,
}

/// The attributes of an `ACCESS` statement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AccessAttrs {
	pub(crate) ac: Strand,
	pub(crate) base: Option<Base>,
	pub(crate) op: AccessOp,
}

/// The attributes of a `LIVE` statement.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LiveAttrs {
	pub(crate) id: Uuid,
	pub(crate) node: Uuid,
	pub(crate) fields: LiveShape,
	/// The subscription as it is stored.
	pub(crate) template: DefinitionTemplate,
}

/// Whether the table's `PERMISSIONS FOR select` shows a subscriber a record.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SubscriberAdmission {
	/// Every record: the permission is `FULL`, or the subscriber's reads are not checked.
	All,
	/// No record: the permission is `NONE`.
	Nothing,
	/// The records a predicate judges truthy. One `PermissionFrame` region holds it, and one
	/// `FieldFrame(Computed)` region for each of `derived`, in order, holds the body of a
	/// `COMPUTED` field the predicate reads, derived onto the copy of the record it judges.
	Judge {
		derived: Box<[DerivedField]>,
	},
}

/// What a subscriber's notification carries.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SubscriberProjection {
	/// `LIVE SELECT DIFF`: the patch from one image to the other. Holds no region.
	Diff,
	/// `LIVE SELECT *`: the record. Holds no region.
	Subject,
	/// `LIVE SELECT <fields>`. One `Clause(Projection)` region for each field that holds one, in
	/// order. `names` is the name each unaliased field lands under, in the order the fields list
	/// them.
	Fields {
		fields: FieldsShape,
		names: Vec<OutputPath>,
	},
}

/// One layer of a subscriber's field view.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SubscriberLayer {
	/// The layer's columns.
	pub(crate) fields: FieldsShape,
	/// The fields the layer's input declares `COMPUTED`, in its schema's order, as
	/// [`ProjectAttrs::computed`] carries them for a statement's layer. A column under one of
	/// these names is judged as on a computed field.
	pub(crate) computed: Vec<Strand>,
	/// The `COMPUTED` fields of the layer's table its guards read, as [`ProjectAttrs::judged`]
	/// states them for a statement's layer: the layer's regions are followed by one
	/// `FieldFrame(Computed)` region per field the shape derives.
	pub(crate) judged: Option<Box<ComputedReadsShape>>,
}

impl SubscriberLayer {
	/// The number of regions the layer holds: the read and the frame of each column, then one per
	/// field its judged reads derive.
	pub(crate) fn regions(&self) -> usize {
		let judged = self.judged.as_deref().map_or(0, ComputedReadsShape::derived_regions);
		self.fields.regions() * 2 + judged
	}
}

/// A live subscription's work on one record image of a table, for one subscriber.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SubscriberAttrs {
	/// Whether the subscriber's reads are permission-checked and not suppressed: the subscriber
	/// whose records the table's permissions and field view are judged for.
	pub(crate) checked: bool,
	/// Whether the table's `PERMISSIONS FOR select` shows the subscriber a record, judged against
	/// the record as stored.
	pub(crate) admission: SubscriberAdmission,
	/// What the table's field view does to an image the subscriber is shown.
	pub(crate) view: ImageView,
	/// The layers that apply the view, in the order they apply: a view that derives every
	/// `COMPUTED` field, or cuts the fields a checked subscriber may not select, is a stack of
	/// layers, and any other holds none. Each layer holds two regions for each of its columns:
	/// the read of the field the column lands under, then the column's own expression in its
	/// frame, as the regions of a `phys.layer`.
	pub(crate) layers: Vec<SubscriberLayer>,
	/// What a notification carries.
	pub(crate) projection: SubscriberProjection,
	/// Whether the subscription has a `WHERE`, held by one `Clause(Where)` region.
	pub(crate) cond: bool,
	/// How many `FETCH` paths the subscription has, each held by one `Clause(Projection)` region.
	pub(crate) fetch: u16,
}

impl SubscriberAttrs {
	/// The kind of each region of the op, in position order: the admission's, the view's layers',
	/// the `WHERE`'s, the projection's and then the `FETCH` paths'.
	pub(crate) fn region_kinds(&self) -> Vec<RegionKind> {
		let mut kinds = Vec::new();
		if let SubscriberAdmission::Judge {
			derived,
		} = &self.admission
		{
			kinds.push(RegionKind::PermissionFrame);
			kinds.extend(std::iter::repeat_n(
				RegionKind::FieldFrame(FieldRole::Computed),
				derived.len(),
			));
		}
		for layer in &self.layers {
			let columns = layer.fields.regions();
			kinds.extend(std::iter::repeat_n(RegionKind::Clause(ClauseRole::Projection), columns));
			kinds.extend(std::iter::repeat_n(
				RegionKind::FieldFrame(FieldRole::Computed),
				layer.regions() - columns,
			));
		}
		if self.cond {
			kinds.push(RegionKind::Clause(ClauseRole::Where));
		}
		kinds.extend(std::iter::repeat_n(
			RegionKind::Clause(ClauseRole::Projection),
			self.projected(),
		));
		kinds.extend(std::iter::repeat_n(
			RegionKind::Clause(ClauseRole::Projection),
			usize::from(self.fetch),
		));
		kinds
	}

	/// How many regions the admission holds: none unless it is a predicate, which holds one and
	/// then one for each field it derives.
	pub(crate) fn admitted(&self) -> usize {
		match &self.admission {
			SubscriberAdmission::All | SubscriberAdmission::Nothing => 0,
			SubscriberAdmission::Judge {
				derived,
			} => 1 + derived.len(),
		}
	}

	/// How many regions the view's layers hold.
	pub(crate) fn viewed(&self) -> usize {
		self.layers.iter().map(SubscriberLayer::regions).sum()
	}

	/// How many regions the projection holds: one for each field that is an expression, and one
	/// for a `VALUE`.
	pub(crate) fn projected(&self) -> usize {
		match &self.projection {
			SubscriberProjection::Diff | SubscriberProjection::Subject => 0,
			SubscriberProjection::Fields {
				fields,
				..
			} => fields.regions(),
		}
	}
}

/// The attributes of a `SHOW CHANGES` statement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ShowAttrs {
	pub(crate) table: Option<TableName>,
	pub(crate) since: ShowSince,
	pub(crate) limit: Option<u32>,
}

// ---------------------------------------------------------------------------------------------
// Loops
// ---------------------------------------------------------------------------------------------

/// What a loop iterates and how its body is entered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LoopKind {
	/// The rows of a `SELECT`.
	Select,
	/// The subjects of a write.
	Write,
	/// `FOR`.
	For,
	/// A closure applied to each element of an array.
	Closure,
	/// The filter of an element predicate over an array.
	ElementFilter,
	/// One hop of a graph or reference lookup, over the records its subjects reach. States a
	/// [`LookupAttrs`].
	GraphStep,
	/// A recursion; iterations are serial across depths. States a [`RecurseAttrs`].
	Recurse,
	/// A path applied to each element of an array. States [`StepAttrs::Flatten`] when it splices
	/// the arrays its body answers.
	Broadcast,
}

/// What a loop does with its iterations' results.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LoopSink {
	/// The results, in order.
	Output,
	/// The elements whose result is truthy.
	Filter,
	/// The first element that decides the answer.
	Decisive(DecisiveKind),
	/// A running accumulator, threaded through the iterations.
	Fold,
	/// Nothing: the body is run for its effects.
	Discard,
	/// The candidate images a write's stages apply. The stages are boxed so that a loop's
	/// attributes stay within the size an op's storage allows.
	Write(Box<WriteStages>),
}

/// How a decisive sink decides.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DecisiveKind {
	Any,
	All,
	Find,
	FindIndex,
}

/// The stages a write sink applies, in order.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WriteStages(pub(crate) Box<[WriteStageKind]>);

/// The order in which a loop's source is consumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SourceOrder {
	/// The order of the value itself: the array index, the set order, the integer range, the id
	/// list.
	Positional,
	/// The order the chosen access path delivers, as the physical plan records it.
	Delivery,
}

// ---------------------------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------------------------

/// What a loop of kind `Broadcast`, `Closure`, `GraphStep`, `Recurse` or `For` states beyond its
/// kind, sink and order. A loop of any other kind, and a broadcast that does not flatten, state
/// none.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum StepAttrs {
	/// A broadcast whose body answers arrays that are spliced into the output one level.
	Flatten,
	/// A closure builtin run over a collection. It takes no operand or region beyond those of the
	/// loop's kind.
	Closure(ClosureLoopAttrs),
	/// A graph or reference lookup.
	Lookup(LookupAttrs),
	/// A recursion.
	Recurse(RecurseAttrs),
	/// The parameter a `FOR` binds its element to, by which code the body runs reads the element
	/// through the frame (a host pipeline, a call). It takes no operand or region of its own, and
	/// the loop keeps its `first` region.
	Param(Strand),
}

impl StepAttrs {
	/// Whether the step takes the operands and the clause regions of the loop it is stated on,
	/// and so leaves the loop no `first` region: every step but a `FOR`'s parameter.
	pub(crate) fn takes_clauses(&self) -> bool {
		!matches!(self, Self::Param(_))
	}
}

/// The builtin a loop of kind `Closure` stands for, which a lowering that does not run the loop
/// inline calls.
///
/// The loop's sink names what the builtin does with the closure's answers, and its operands are
/// the collection and, for a `fold`, the initial accumulator. The name is what the call or the
/// method was written with, because a denied capability and a refused argument name it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ClosureLoopAttrs {
	/// The builtin as written, module included for a function (`array::some`), or the method
	/// name alone (`some`).
	pub(crate) function: Strand,
	/// Whether the loop was written as a method of its collection.
	pub(crate) method: bool,
}

/// One subject of a lookup: the table it scans and, for a reference lookup, the field of that
/// table that holds the reference.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LookupSubjectShape {
	pub(crate) table: TableName,
	pub(crate) referencing_field: Option<String>,
	/// The key range the scan is limited to. The operands of its bounds follow the step's source
	/// operand, subject by subject, start before end.
	pub(crate) range: Option<KeyRange>,
}

/// What a lookup reads and how it shapes what it reaches.
///
/// The loop reads the records its subjects reach from the source operand, one iteration per
/// reached record; the element of its body is the record, and the body yields it unchanged. The
/// clauses of the lookup are regions of the loop after the body, in the order of the fields below,
/// each evaluated against a reached record, which `$this`, `$self` and `$value` name.
///
/// Operands: the source, then the operands of the range subjects' bounds, then the `LIMIT` value
/// and the `START` value when present, at the slots named below. Both are evaluated once before
/// the first record is read.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LookupAttrs {
	pub(crate) kind: LookupKind,
	/// `ONLY`: the step answers the one record it reaches, or `NONE`.
	pub(crate) only: bool,
	/// `ONLY` on a lookup of the path below the run of hops this step ends: what the run answers
	/// over all its origins together is one value, `NONE` for none, and more than one is an error.
	/// The path below is read without its `ONLY`. Only the last hop of a run states it.
	pub(crate) only_path: bool,
	pub(crate) subjects: Box<[LookupSubjectShape]>,
	/// The projection the lookup writes as a `SELECT` list, one `Clause(Projection)` region per
	/// column; `None` is the records themselves.
	pub(crate) fields: Option<FieldsShape>,
	/// `ORDER BY`, one `Clause(OrderKey)` region per key.
	pub(crate) order: Option<SortShape>,
	/// `AS name`: where the step's value lands in the output of the path that holds it.
	pub(crate) alias: Option<OutputPath>,
	/// A `Clause(Where)` region holds the `WHERE` predicate.
	pub(crate) cond: bool,
	/// `SPLIT ON`: the number of `Clause(Split)` regions.
	pub(crate) split: Option<u16>,
	/// `GROUP BY`: the number of `ErrorScope(ShapeToNone)` regions, one per key. `Some(0)` is
	/// `GROUP ALL`, whose columns are aggregates all the same.
	pub(crate) group: Option<u16>,
	pub(crate) limit: Option<OperandSlot>,
	pub(crate) start: Option<OperandSlot>,
	/// What a query trace does with the lookup, as the statement was marked
	/// ([`crate::expr::logical::trace`]).
	pub(crate) trace: crate::expr::logical::LookupTrace,
}

/// How the path of a recursion was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RecursePathForm {
	/// `v.{n}`: each depth is the value itself, and the body yields its element.
	Absent,
	/// `v.{n}(body)`.
	Nested,
	/// `v.{n}.body`, up to and including the recursion marker.
	Trailing,
}

/// What a recursion collects, by its `+instruction`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RecurseInstructionShape {
	/// The paths from the start to each end.
	Path {
		inclusive: bool,
	},
	/// Every value reached, in the order reached.
	Collect {
		inclusive: bool,
	},
	/// The shortest path to the value the operand after the source names.
	Shortest {
		inclusive: bool,
	},
}

/// A recursion over the value of its source operand.
///
/// The body region receives the value of a depth as its element and the depth as its position, and
/// yields the value the next depth starts from. The recursion marker (`expr.repeat_recurse`) of a
/// trailing path stands in the body. Operands: the source, then the value a shortest-path
/// instruction looks for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RecurseAttrs {
	pub(crate) depth: Recurse,
	pub(crate) form: RecursePathForm,
	pub(crate) instruction: Option<RecurseInstructionShape>,
}

// ---------------------------------------------------------------------------------------------
// Write stages
// ---------------------------------------------------------------------------------------------

/// One stage of the per-record write orders, in the order the stages are declared.
///
/// A stage that varies by an action or an image carries it as its payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WriteStageKind {
	CheckRecordUserAccess,
	CheckRecordExists,
	CheckTableNotView,
	CheckTableNotLightweight(LightweightReason),
	CheckTableType(TableTypeFor),
	CheckPermissionsQuickCreate,
	CheckUpdatePermissions,
	CheckDeletePermissions,
	CheckCreatePermissions,
	RecheckUpdatePermissions,
	CheckWhereCondition,
	ComputeInputData,
	CheckDataFields,
	ProcessRecordData,
	ProcessMergeData,
	DefaultRecordData,
	GenerateRecordId,
	ProcessTableFields,
	CleanupTableFields,
	CleanupTableReferences,
	ClearRecordData,
	StoreRecordData,
	StoreEdgesData,
	StoreInlineAdjacencyData,
	StoreIndexData,
	ReclaimDocId,
	PurgeRecordData,
	MaterialiseObservedFields,
	ProcessTableReferences,
	ProcessTableViews(Action),
	ProcessTableEvents(Action),
	ProcessTableLives(Action),
	ProcessChangefeeds,
	CheckSelectPermissions(RecordImage),
	OutputWrite,
}

/// Which half of a write stage's work an op is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WriteMark {
	/// Row-local work, which may run in any order and at once.
	Compute,
	/// Work on shared state, which runs in iteration order.
	Apply,
}

const _: () = {
	assert!(::core::mem::size_of::<LogOp>() <= 32);
};

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use super::LogTag;

	#[test]
	fn mnemonics_are_unique_and_namespaced() {
		let mut seen = HashSet::new();
		for tag in LogTag::ALL {
			let name = tag.mnemonic();
			assert!(name.starts_with("logical."), "{name} is outside the dialect");
			assert!(seen.insert(name), "{name} names two tags");
		}
	}

	#[test]
	fn tags_are_listed_in_order() {
		assert!(LogTag::ALL.windows(2).all(|pair| pair[0] < pair[1]));
	}
}
