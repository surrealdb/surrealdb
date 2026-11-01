//! The operations of the physical dialect.
//!
//! One [`PhysOp`] variant per access path, relational operator, control construct and statement
//! the physical plan holds, plus the ops that exist only at this level: pipelines, guarded
//! choices, batched reads, materialisation and unit loads. A variant's payload is the op's
//! attribute set: plain data that is part of the op's identity. Evaluated clauses are operands or
//! regions, never payload, and no payload holds an expression, a plan or a statement. Where a
//! planner type embeds an expression, the attribute is its expression-free shape and the
//! expression is an operand or a region, as each variant's doc lists.
//!
//! The legality and decision of a pipeline are never attributes: they live in the container's
//! placement table, keyed by the pipeline op, and are not part of identity.
//!
//! A payload larger than 24 bytes is boxed, so the op storage of a container stays dense.
//!
//! [`PhysTag`] is the payload-free twin of [`PhysOp`], and [`PhysOp::tag`] is total.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;
use surrealdb_iam::AuthLimit;
use surrealdb_strand::{Strand, TableName};

use crate::catalog::{Distance, IndexId, KeyStructure};
use crate::exec::field_path::FieldPath;
use crate::exec::index::key_range::DeliveredKeyOrder;
use crate::exec::ir::expr::ops::DestructureEntry;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::statements::{
	BindStepShape, ContentMode, DdlGuard, InfoSubject, MatchBindings, MatchOutputShape,
	MatchPattern, OperandSlot, Place,
};
use crate::exec::ir::iface::{ChooseSpec, IndexRef, LoadUnitSpec, StatementCommit};
use crate::exec::ir::logical::ops::{
	AccessAttrs, AlterAttrs, AssignAttrs, DefineAttrs, EventsAttrs, FieldsShape, LetAttrs,
	LiveAttrs, LoopKind, MatchInsertAttrs, MatchUpdateAttrs, RebuildAttrs, RemoveAttrs, ShowAttrs,
	SourceRefShape, StepAttrs, SubscriberAttrs, TxnControl, TxnRegionMode, UseTarget, WriteAttrs,
	WriteMark, WriteStageKind,
};
pub(crate) use crate::exec::ir::logical::ops::{ComputedReadsShape, DerivedReads, WindowShape};
use crate::exec::operators::SplitTarget;
use crate::exec::operators::scan::common::DecodeFields;
use crate::exec::operators::scan::{CountOutput, MergeMode};
use crate::exec::planner::CatalogWithin;
use crate::exec::pre_decode_filter::PreDecodeShape;
use crate::exec::vm::payload::{CountSelect, JoinBuild, JoinMatch, Spill, TopKKey, ValuesRead};
use crate::exec::vm::plain::ValueData;
use crate::expr::logical::{GuardKind, OutputPath};
use crate::expr::operator::{MatchRef, MatchesOperator};
use crate::expr::{Dir, ExplainFormat, With};
use crate::kvs::Direction;
use crate::val::{Number, RecordId, Value};

define_op_tags! {
	/// The payload-free identity of a [`PhysOp`].
	pub(crate) enum PhysTag {
		TableScan => "phys.table_scan",
		IndexScan => "phys.index_scan",
		IndexUnion => "phys.index_union",
		Bitmap => "phys.bitmap",
		IndexLookup => "phys.index_lookup",
		FullTextScan => "phys.fulltext_scan",
		KeyRangeScan => "phys.key_range_scan",
		KnnScan => "phys.knn_scan",
		KnnTopK => "phys.knn_topk",
		Count => "phys.count",
		RecordScan => "phys.record_scan",
		Vacant => "phys.vacant",
		Enforced => "phys.enforced",
		Values => "phys.values",
		Subquery => "phys.subquery",
		Probe => "phys.probe",
		Union => "phys.union",
		Join => "phys.join",
		Distinct => "phys.distinct",
		PermissionFilter => "phys.permission_filter",
		Layer => "phys.layer",
		Filter => "phys.filter",
		Fetch => "phys.fetch",
		Split => "phys.split",
		Sort => "phys.sort",
		Window => "phys.window",
		Aggregate => "phys.aggregate",
		Limit => "phys.limit",
		Project => "phys.project",
		Timeout => "phys.timeout",
		VersionScope => "phys.version_scope",
		Only => "phys.only",
		Explain => "phys.explain",
		Eval => "phys.eval",
		Request => "phys.request",
		Txn => "phys.txn",
		Stmt => "phys.stmt",
		Statement => "phys.statement",
		Match => "phys.match",
		MatchStep => "phys.match_step",
		DdlGuard => "phys.ddl_guard",
		Define => "phys.define",
		Remove => "phys.remove",
		Alter => "phys.alter",
		Rebuild => "phys.rebuild",
		Info => "phys.info",
		Use => "phys.use",
		Option => "phys.option",
		Access => "phys.access",
		Kill => "phys.kill",
		Live => "phys.live",
		Subscriber => "phys.subscriber",
		Show => "phys.show",
		Write => "phys.write",
		WriteStage => "phys.write_stage",
		Events => "phys.events",
		Assign => "phys.assign",
		Unset => "phys.unset",
		Content => "phys.content",
		Block => "phys.block",
		Let => "phys.let",
		Sleep => "phys.sleep",
		If => "phys.if",
		Loop => "phys.loop",
		Pipeline => "phys.pipeline",
		Choose => "phys.choose",
		Prefetch => "phys.prefetch",
		Materialize => "phys.materialize",
		LoadUnit => "phys.load_unit",
		Yield => "phys.yield",
		Return => "phys.return",
		Break => "phys.break",
		Next => "phys.next",
	}
}

/// One operation of the physical dialect.
///
/// A `phys` op's regions keep the kind of the logical region the op replaces. The regions new at
/// this level, a pipeline's and a physical choice arm's, are `Pipeline` and `ChooseArm`.
///
/// Carries no `Eq` or `Hash`: some payloads embed types of the planner that implement neither, so
/// identity is the structural digest of the attributes.
#[derive(Clone, Debug)]
pub(crate) enum PhysOp {
	/// Every record of a table in key order, or those within the bounds of its record ids.
	/// Operands: the values of the [`KeyBoundShape::Operand`] bounds of `bounds`, in slot order.
	TableScan(Box<TableScanAttrs>),
	/// The records a B-tree index lists under an access, in the index's key order. Operands: the
	/// values the access is built from, none when `access` is fully literal.
	IndexScan(Box<IndexScanAttrs>),
	/// The records of the B-tree scans of its branches, each record once. Operands: none.
	IndexUnion(Box<IndexUnionAttrs>),
	/// The records whose doc-ids survive a fused bitmap plan, in doc-id order. One `Pipeline`
	/// region holds the index union the bitmap plan stands in for, when `fallback` says one was
	/// kept.
	Bitmap(Box<BitmapAttrs>),
	/// The records of a table whose field may equal a key read when the scan opens. Operand: the
	/// key.
	IndexLookup(Box<IndexLookupAttrs>),
	/// The records a full-text index lists for a query.
	FullTextScan(Box<FullTextScanAttrs>),
	/// The records of a table whose record id key lies in the ranges an access names over the
	/// components of the key, in key order along the access's direction. Operands: one value per
	/// [`KeyBoundShape::Operand`] of the access, in slot order.
	KeyRangeScan(Box<KeyRangeScanAttrs>),
	/// The records a KNN index lists for the nearest neighbours of a vector. One region per
	/// permission of the filter, then the residual, the `MATCHES` condition and the prefilter's
	/// residual, each when `filter` says it is present.
	KnnScan(Box<KnnScanAttrs>),
	/// The `k` rows of the input nearest a vector by a distance. Operands: the input, then the
	/// target when it is deferred.
	KnnTopK(Box<KnnTopKAttrs>),
	/// A count answered from an index or a source's cardinality. One `Clause(Where)` region holds
	/// the condition of an indexed count; one region holds the plan that answers when the
	/// shortcut does not apply, when there is one.
	Count(Box<CountAttrs>),
	/// One record read by id. Operand: the record id.
	RecordScan(Box<RecordScanAttrs>),
	/// No record of a table the statement above requires defined. The read is empty, and the
	/// statement still owes the error of a table that is not defined.
	Vacant {
		table: TableName,
	},
	/// Table enforcement before the chain it guards. Operand: the input.
	Enforced {
		table: TableName,
	},
	/// Rows produced from a value. Operand: the value.
	Values {
		read: ValuesRead,
	},
	/// A statement in relational position. One `Subquery` region.
	Subquery,
	/// The rows of the build input, kept keyed by `field`, read once for every evaluation of the
	/// outer statement. Operands: the build input, then the outer expression's value.
	Probe {
		field: Strand,
	},
	/// The concatenation of its inputs, in order; with no input, the empty scan.
	Union,
	/// Each left row paired with the right rows whose keys match. Operands: the left and the
	/// right input. Regions: one `Clause(Where)` pair per key, left then right, then the
	/// residual when `residual` is set, then the right input of the lookup when `lookup` is set.
	Join(Box<JoinAttrs>),
	/// One row per distinct value. Operand: the input.
	Distinct {
		spill: Spill,
	},
	/// The rows a permission admits, judged in the op's frame. Operand: the input. One
	/// `PermissionFrame` region for the predicate, then one for the `once` predicate when it is
	/// present.
	PermissionFilter(Box<PermissionFilterAttrs>),
	/// A resolver layer over the input's rows. Operand: the input. One `Clause(Projection)`
	/// region per field and a `FieldFrame` region per computed column.
	Layer(Box<LayerAttrs>),
	/// The rows a predicate admits. Operand: the input. One `Clause(Where)` region, then one for
	/// the `once` predicate when it is present.
	Filter,
	/// `FETCH`. Operand: the input. One `Clause(Projection)` region per path.
	Fetch,
	/// `SPLIT ON`. Operand: the input. One `Clause(Split)` region per target.
	Split {
		/// As [`LogOp::Split`](crate::exec::ir::logical::ops::LogOp::Split) states them.
		targets: Box<[SplitTarget]>,
	},
	/// `ORDER BY`. Operand: the input. One `Clause(OrderKey)` region per computed key.
	Sort(Box<SortAttrs>),
	/// Window functions over the input, which reaches it in partition-then-order sequence. A
	/// breaker: it may hold rows back until a partition or a peer group ends. Operand and regions
	/// as [`LogOp::Window`](crate::exec::ir::logical::ops::LogOp::Window) states them.
	Window(Box<WindowShape>),
	/// `GROUP BY`. Operand: the input. One `ErrorScope(ShapeToNone)` region per group key, then
	/// one `Clause(Projection)` region per field.
	Aggregate(Box<AggregateAttrs>),
	/// `LIMIT` and `START`, evaluated once, `LIMIT` first. Operand: the input, then the slots
	/// named.
	Limit {
		limit: Option<OperandSlot>,
		start: Option<OperandSlot>,
	},
	/// The projection. Operand: the input. One `Clause(Projection)` region per expression the
	/// shape holds, in the order the shape lists them, the omissions' sources after the fields.
	Project(Box<ProjectionShape>),
	/// `TIMEOUT`. Operand: the duration. One `Deadline` region holds the timed body.
	Timeout,
	/// `VERSION`. Operands: the input, then the version. Sets the snapshot of every op beneath it.
	VersionScope,
	/// `ONLY`. Operand: the input.
	Only,
	/// `EXPLAIN`. Operand: the input.
	Explain {
		analyze: bool,
		format: ExplainFormat,
	},
	/// A bare expression as a statement. Operand: the value. One `Clause(Projection)` region per
	/// `FETCH` path.
	Eval,
	/// The top region of a fragment, as the logical request.
	Request {
		fragment: Option<u32>,
		nested: bool,
	},
	/// An explicit or session transaction; one `Transaction` region.
	Txn {
		mode: TxnRegionMode,
	},
	/// One statement; one `Statement` region whose block argument is the statement timestamp.
	/// `control` names the transaction statement a slot of the request stands for, which the
	/// executor drives and no plan holds.
	Stmt {
		commit: StatementCommit,
		control: Option<TxnControl>,
	},
	/// A statement of a block, planned with the block and lowered as a group of its own. One
	/// `Statement` region holds its physical plan. `changes_catalog` marks a schema statement,
	/// `served` a statement that runs after a catalog change the block was not planned knowing,
	/// planned when the block reaches it from the statement as written at that index of the
	/// container's late-bound statements (its region yields `NONE` in its place), and
	/// `raises_when_reached` one whose plan is a refusal raised when the block reaches it.
	Statement {
		changes_catalog: bool,
		served: Option<u32>,
		within: CatalogWithin,
		raises_when_reached: bool,
	},
	/// A `MATCH` query, as the logical op: one `Subquery` region per stage, `stages` of them, the
	/// output stage last.
	Match {
		bindings: MatchBindings,
		stages: u16,
	},
	/// One stage of a `MATCH` query. Operand: the stage's input rows. Regions as the logical
	/// stage op of the same shape lists them.
	MatchStep(Box<MatchStepAttrs>),
	/// The authority check that precedes a schema statement's operands or definition.
	DdlGuard {
		guard: DdlGuard,
	},
	/// A `DEFINE`, as the logical op.
	Define(Box<DefineAttrs>),
	/// A `REMOVE`, as the logical op.
	Remove(Box<RemoveAttrs>),
	/// An `ALTER`, as the logical op.
	Alter(Box<AlterAttrs>),
	/// A `REBUILD INDEX`, as the logical op.
	Rebuild(Box<RebuildAttrs>),
	/// `INFO`, as the logical op.
	Info {
		subject: InfoSubject,
		structure: bool,
	},
	/// `USE`, as the logical op.
	Use {
		target: UseTarget,
	},
	/// `OPTION`, as the logical op.
	Option {
		name: Strand,
		value: bool,
	},
	/// `ACCESS`, as the logical op.
	Access(Box<AccessAttrs>),
	/// `KILL`. Operand: the live query id.
	Kill,
	/// `LIVE`, as the logical op.
	Live(Box<LiveAttrs>),
	/// A live subscription's work on one record image of a table, as the logical op.
	Subscriber(Box<SubscriberAttrs>),
	/// `SHOW CHANGES`, as the logical op.
	Show(Box<ShowAttrs>),
	/// A mutation. Operands as `subjects` lists them. One `Clause(ReturnClause)` region holds the
	/// projection when the output is `Fields`, and the op holds no other region. The
	/// [`PhysOp::WriteStage`] ops, in the order the stages run, are the body of the `Write` loop
	/// that delivers the subjects, and each stage holds the regions of the clauses it evaluates.
	Write(Box<PhysWriteAttrs>),
	/// One stage of a write's per-record order, with the half of its work the op is.
	WriteStage {
		stage: WriteStageKind,
		mark: WriteMark,
	},
	/// A table's events for one action, as the logical op.
	Events(Box<EventsAttrs>),
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
	/// Each statement in turn. One `Block` region.
	Block,
	/// `LET`. Operand: the value.
	Let(Box<LetAttrs>),
	/// `SLEEP`. Operand: the duration.
	Sleep,
	/// `IF`, as the nested one-condition form of the logical op. Operand: the condition. Two
	/// regions, the arm taken and the arm that follows: `StatementIf` regions in statement
	/// position and `ValueIf` regions when `value` is set.
	If {
		value: bool,
	},
	/// A loop over a source. Operand: the source. One body region, as the logical loop; when
	/// `first` is set, a second region holds the body the first element runs, planned against the
	/// catalog the first iteration leaves. `batch` bounds both a fan-out batch and the positions
	/// one execution of the loop's batched read covers; `unroll` is the number of iterations a
	/// batch form takes at once, from one to eight. The body of a `FOR` is a `StatementFor`
	/// region, or a `ValueFor` region when `value` is set; `value` is false for every other kind. A
	/// loop that states a [`StepAttrs`] that [takes clauses](StepAttrs::takes_clauses) takes the
	/// operands and the clause regions of the logical loop it stands for, and holds no `first`
	/// region; a `FOR` states its parameter as [`StepAttrs::Param`].
	Loop {
		kind: LoopKind,
		first: bool,
		batch: Batch,
		unroll: u8,
		value: bool,
		step: Option<Box<StepAttrs>>,
	},
	/// A pipeline: the unit a placement decision is made for. One `Pipeline` region. Its
	/// legality and decision are in the placement table, keyed by this op.
	Pipeline(Box<PipelineAttrs>),
	/// A guarded choice between prepared alternatives. One `ChooseArm` region per arm.
	Choose(Box<ChooseSpec>),
	/// The batched read of a loop's or a request's keys. Operands: the key values `keys` names.
	/// Raises nothing and yields nothing.
	Prefetch(Box<PrefetchKeys>),
	/// The input's rows, held bounded and spilling, so a write beneath cannot be seen by the
	/// read above. Operand: the input.
	Materialize {
		image: MaterializeImage,
	},
	/// A unit load: the only compile that happens while a program runs. Operands: the run-time
	/// parts of the unit's key, the statement timestamp among them.
	LoadUnit(Box<LoadUnitSpec>),
	/// The terminator of a region that delivers its operands as results.
	Yield,
	/// `RETURN`: the terminator that ends the region its target names with its operand.
	Return,
	/// The terminator that leaves the loop (`BREAK`).
	Break,
	/// The terminator that ends the iteration and continues the loop (`CONTINUE`).
	Next,
}

impl PhysOp {
	/// The payload-free identity of the op. Total: no variant is left unnamed.
	pub(crate) fn tag(&self) -> PhysTag {
		match self {
			PhysOp::TableScan(_) => PhysTag::TableScan,
			PhysOp::IndexScan(_) => PhysTag::IndexScan,
			PhysOp::IndexUnion(_) => PhysTag::IndexUnion,
			PhysOp::Bitmap(_) => PhysTag::Bitmap,
			PhysOp::IndexLookup(_) => PhysTag::IndexLookup,
			PhysOp::FullTextScan(_) => PhysTag::FullTextScan,
			PhysOp::KeyRangeScan(_) => PhysTag::KeyRangeScan,
			PhysOp::KnnScan(_) => PhysTag::KnnScan,
			PhysOp::KnnTopK(_) => PhysTag::KnnTopK,
			PhysOp::Count(_) => PhysTag::Count,
			PhysOp::RecordScan(..) => PhysTag::RecordScan,
			PhysOp::Vacant {
				..
			} => PhysTag::Vacant,
			PhysOp::Enforced {
				..
			} => PhysTag::Enforced,
			PhysOp::Values {
				..
			} => PhysTag::Values,
			PhysOp::Subquery => PhysTag::Subquery,
			PhysOp::Probe {
				..
			} => PhysTag::Probe,
			PhysOp::Union => PhysTag::Union,
			PhysOp::Join(_) => PhysTag::Join,
			PhysOp::Distinct {
				..
			} => PhysTag::Distinct,
			PhysOp::PermissionFilter(_) => PhysTag::PermissionFilter,
			PhysOp::Layer(_) => PhysTag::Layer,
			PhysOp::Filter => PhysTag::Filter,
			PhysOp::Fetch => PhysTag::Fetch,
			PhysOp::Split {
				..
			} => PhysTag::Split,
			PhysOp::Sort(_) => PhysTag::Sort,
			PhysOp::Window(_) => PhysTag::Window,
			PhysOp::Aggregate(_) => PhysTag::Aggregate,
			PhysOp::Limit {
				..
			} => PhysTag::Limit,
			PhysOp::Project(_) => PhysTag::Project,
			PhysOp::Timeout => PhysTag::Timeout,
			PhysOp::VersionScope => PhysTag::VersionScope,
			PhysOp::Only => PhysTag::Only,
			PhysOp::Explain {
				..
			} => PhysTag::Explain,
			PhysOp::Eval => PhysTag::Eval,
			PhysOp::Request {
				..
			} => PhysTag::Request,
			PhysOp::Txn {
				..
			} => PhysTag::Txn,
			PhysOp::Stmt {
				..
			} => PhysTag::Stmt,
			PhysOp::Statement {
				..
			} => PhysTag::Statement,
			PhysOp::Match {
				..
			} => PhysTag::Match,
			PhysOp::MatchStep(_) => PhysTag::MatchStep,
			PhysOp::DdlGuard {
				..
			} => PhysTag::DdlGuard,
			PhysOp::Define(_) => PhysTag::Define,
			PhysOp::Remove(_) => PhysTag::Remove,
			PhysOp::Alter(_) => PhysTag::Alter,
			PhysOp::Rebuild(_) => PhysTag::Rebuild,
			PhysOp::Info {
				..
			} => PhysTag::Info,
			PhysOp::Use {
				..
			} => PhysTag::Use,
			PhysOp::Option {
				..
			} => PhysTag::Option,
			PhysOp::Access(_) => PhysTag::Access,
			PhysOp::Kill => PhysTag::Kill,
			PhysOp::Live(_) => PhysTag::Live,
			PhysOp::Subscriber(_) => PhysTag::Subscriber,
			PhysOp::Show(_) => PhysTag::Show,
			PhysOp::Write(_) => PhysTag::Write,
			PhysOp::WriteStage {
				..
			} => PhysTag::WriteStage,
			PhysOp::Events(_) => PhysTag::Events,
			PhysOp::Assign(_) => PhysTag::Assign,
			PhysOp::Unset {
				..
			} => PhysTag::Unset,
			PhysOp::Content {
				..
			} => PhysTag::Content,
			PhysOp::Block => PhysTag::Block,
			PhysOp::Let(_) => PhysTag::Let,
			PhysOp::Sleep => PhysTag::Sleep,
			PhysOp::If {
				..
			} => PhysTag::If,
			PhysOp::Loop {
				..
			} => PhysTag::Loop,
			PhysOp::Pipeline(_) => PhysTag::Pipeline,
			PhysOp::Choose(_) => PhysTag::Choose,
			PhysOp::Prefetch(_) => PhysTag::Prefetch,
			PhysOp::Materialize {
				..
			} => PhysTag::Materialize,
			PhysOp::LoadUnit(_) => PhysTag::LoadUnit,
			PhysOp::Yield => PhysTag::Yield,
			PhysOp::Return => PhysTag::Return,
			PhysOp::Break => PhysTag::Break,
			PhysOp::Next => PhysTag::Next,
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Pipelines, batches and prefetch
// ---------------------------------------------------------------------------------------------

/// The identity of a pipeline within one container.
///
/// Ids are scoped to their container and never part of a cache key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PipelineId(pub(crate) u32);

/// How one pipeline depends on another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PipelineEdge {
	/// The other pipeline's rows are this one's input.
	Feeds,
	/// The other pipeline builds the structure this one holds, such as a join's hash side.
	Builds,
	/// The other pipeline probes the structure this one built.
	Probes,
}

/// The attributes of a pipeline op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PipelineAttrs {
	pub(crate) id: PipelineId,
	/// The pipelines this one has an edge to. Together, the pipelines of a container form a DAG.
	pub(crate) edges: SmallVec<[(PipelineEdge, PipelineId); 2]>,
	/// The logical pipeline this one stands for, when it was derived from one.
	pub(crate) logical: Option<OpId>,
}

/// The bounds of one batch of a loop.
///
/// Bounds both a fan-out batch and the positions one execution of the loop's batched read
/// covers. The VM picks the actual size within them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Batch {
	pub(crate) max_iterations: u32,
	pub(crate) max_bytes: u32,
}

/// The keys a batched read covers, as operand indices of the prefetch op.
///
/// A point names a single-key operand. A range names the operands that hold its begin and end
/// keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PrefetchKeys {
	pub(crate) points: SmallVec<[u16; 4]>,
	pub(crate) ranges: SmallVec<[(u16, u16); 2]>,
}

/// What a materialisation holds of each row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MaterializeImage {
	/// The record's key and its stored value.
	KeyAndValue,
}

// ---------------------------------------------------------------------------------------------
// Access paths
// ---------------------------------------------------------------------------------------------

/// What a read of rows keeps of the source it was chosen for, so the source can be stated again:
/// the schema the catalog declares for the rows and the `WITH` hint.
///
/// `FOR UPDATE` is not among them: a locked read of one record is [`PhysOp::RecordScan`]'s own
/// `for_update`, a locked read of a value is [`ValuesRead::Locked`], and a locked read of a whole
/// table is a refusal, so no other read holds one.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ReadClauses {
	/// What the catalog declares for the rows; `None` reads as an open schema.
	pub(crate) schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
	/// The `WITH INDEX` or `WITH NOINDEX` hint, which constrains the access path and not the rows.
	pub(crate) with: Option<With>,
}

/// The attributes of a table scan.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TableScanAttrs {
	pub(crate) table: TableName,
	pub(crate) read: ReadClauses,
	pub(crate) direction: Direction,
	/// The sort key a scan beneath a top-k sort reads from stored bytes to reject a row that
	/// cannot enter the top k.
	pub(crate) topk: Option<TopKKey>,
	/// Whether the table is a lightweight relation, which stores no record range to scan.
	pub(crate) lightweight: bool,
	/// Whether the scan may be taken to deliver record ids in id order along its direction. It is
	/// false when the key order of a table whose record id names coarsely ordered components, or
	/// of a versioned read, is not known to be id order, so that an `ORDER BY id` above the scan
	/// stays.
	pub(crate) id_order: bool,
	/// The shape of the predicate the stored bytes of a row judge before the row is decoded. It
	/// is the predicate of the filter the scan feeds, set only where that filter is the first op
	/// to read the rows; a record the shape refuses is never decoded, and one it cannot judge is
	/// judged by the predicate itself.
	pub(crate) prefilter: Option<PreDecodeShape>,
	/// The interval of record ids the scan reads, when the predicate above it bounds `id`. The
	/// bounds only narrow the keys read: every row inside them is a row the unbounded scan
	/// reaches, in the same order, and the predicate stays above the scan.
	pub(crate) bounds: Option<IdBoundsShape>,
	/// The index ranges whose entry counts bound the share of the scan's rows its predicate
	/// passes ([`crate::exec::index::probe`]), when a conjunct of the predicate is one an index
	/// answers. Read only to decide how the scan is placed as it opens; it never changes the rows.
	pub(crate) probes: Option<Box<ScanProbes>>,
}

/// The selectivity probes of a table scan, over the indexes the access-path decision reads.
pub(crate) type ScanProbes = crate::exec::index::probe::ProbeSet<IndexRef>;

/// The attributes of a read of one record by id.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RecordScanAttrs {
	/// The table of the record the operand names.
	pub(crate) table: TableName,
	/// Whether the record is read locked, registering its key for commit-time conflict detection
	/// whether or not it exists.
	pub(crate) for_update: bool,
	/// Whether the table is a lightweight relation and the read is not locked, so the record is
	/// read from the relation's edges.
	pub(crate) lightweight: bool,
	pub(crate) read: ReadClauses,
}

/// What a B-tree scan of an index lists.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexScanAttrs {
	pub(crate) table: TableName,
	pub(crate) read: ReadClauses,
	pub(crate) index: IndexRef,
	pub(crate) access: crate::exec::index::access_path::BTreeAccess,
	pub(crate) direction: Direction,
	/// The record fields the order satisfies, when it satisfies the statement's ordering.
	pub(crate) ordered_by: Option<Vec<FieldPath>>,
	/// The index columns the access pins, in definition order, as plain field paths, when the
	/// access lists exactly the records whose columns satisfy the conjuncts it was chosen from
	/// ([`Self::exact`]); a conjunct the access covers ([`Self::stated`]) is answered by the scan.
	pub(crate) pinned: Option<Vec<FieldPath>>,
	/// The columns of the index, in definition order, that the access states its comparisons
	/// over, whether or not the access is exact. `None` where a range is widened to the seconds
	/// of its bounds, which lists records its comparison rejects.
	pub(crate) stated: Option<Vec<crate::expr::Idiom>>,
	/// Whether the filter that is the first op to read the scan's rows judges only comparisons
	/// the access covers over [`Self::stated`], so that it rechecks each record the scan lists
	/// (`recheck::run`).
	pub(crate) rechecked: bool,
	/// The top-level fields read from each record; `None` reads whole records.
	pub(crate) decode: Option<DecodeFields>,
}

impl IndexScanAttrs {
	/// Whether the access lists exactly the records whose columns satisfy the conjuncts it was
	/// chosen from: it pins columns.
	pub(crate) fn exact(&self) -> bool {
		self.pinned.is_some()
	}
}

/// What a branch of an index union reads.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BranchReadShape {
	/// The entries of a B-tree index under an access.
	BTree(ValueData<crate::exec::index::access_path::BTreeAccess>),
	/// The records a full-text index lists for a query.
	FullText {
		query: String,
		operator: MatchesOperator,
		/// The first column of the index: the field the `MATCHES` the branch answers reads.
		column: Option<crate::expr::Idiom>,
		/// Whether the index keys its documents by the table's shared doc ids, so a hit it lists
		/// can be scored under the clause of another full-text index of the table.
		shared_doc_ids: bool,
	},
}

/// One scan of an index union.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexBranchShape {
	pub(crate) index: IndexRef,
	pub(crate) read: BranchReadShape,
	pub(crate) direction: Direction,
}

/// The attributes of an index union.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexUnionAttrs {
	pub(crate) table: TableName,
	pub(crate) read: ReadClauses,
	pub(crate) branches: Vec<IndexBranchShape>,
	/// The key the branches are merged by, when the union delivers the statement's ordering.
	pub(crate) merge: Option<MergeMode>,
	pub(crate) ordered_by: Option<Vec<FieldPath>>,
	pub(crate) decode: Option<DecodeFields>,
	/// Where a bounded sort above orders first by a sum of `search::score` calls over the
	/// clauses the union's full-text branches answer, the top-k key the branches reject hits by,
	/// with the sum. A branch scores each hit under every clause of the sum, adds the scores as
	/// the sort's key adds them, and passes over a hit whose sum the sort's threshold shows worse
	/// than every row the sort keeps, before its record is read. The key's path is empty.
	pub(crate) topk: Option<(TopKKey, ScoreSum)>,
}

/// A sum of `search::score` calls, shaped as the expression that adds them: a clause's score,
/// or the sum of two sums, left before right.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ScoreSum {
	/// The score of the hit under clause `N`, the value of `search::score(N)`.
	Clause(MatchRef),
	/// The first sum plus the second.
	Add(Box<ScoreSum>, Box<ScoreSum>),
}

impl ScoreSum {
	/// The clauses the sum scores, left to right, each once per appearance.
	pub(crate) fn clauses(&self, into: &mut Vec<MatchRef>) {
		match self {
			Self::Clause(clause) => into.push(*clause),
			Self::Add(left, right) => {
				left.clauses(into);
				right.clauses(into);
			}
		}
	}
}

/// A fused bitmap plan.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BitmapShape {
	BTree {
		index: IndexRef,
		access: crate::exec::index::access_path::BTreeAccess,
	},
	FullText {
		index: IndexRef,
		query: String,
		operator: MatchesOperator,
	},
	Graph {
		source: RecordId,
		direction: Dir,
		edge_tables: Vec<TableName>,
	},
	And(Vec<BitmapShape>),
	Or(Vec<BitmapShape>),
	AndNot {
		base: Box<BitmapShape>,
		subtract: Box<BitmapShape>,
	},
}

/// The attributes of a bitmap read.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BitmapAttrs {
	pub(crate) table: TableName,
	pub(crate) read: ReadClauses,
	pub(crate) root: BitmapShape,
	pub(crate) decode: Option<DecodeFields>,
	/// Whether a `Pipeline` region holds the read that answers instead when an unanchored root
	/// overflows: a pipeline whose block ends in a `phys.yield` of the rows of the index union the
	/// bitmap plan stands in for.
	pub(crate) fallback: bool,
}

/// Where an index lookup's key comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LookupKeyShape {
	/// The row of the statement the lookup is evaluated for.
	Parent,
	/// The row the probe keeps.
	Probe,
}

/// The attributes of an index lookup.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexLookupAttrs {
	pub(crate) table: TableName,
	pub(crate) read: ReadClauses,
	pub(crate) index: IndexRef,
	pub(crate) field: Strand,
	pub(crate) key: LookupKeyShape,
}

/// The attributes of a full-text scan.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FullTextScanAttrs {
	pub(crate) table: TableName,
	pub(crate) read: ReadClauses,
	pub(crate) index: IndexRef,
	pub(crate) query: String,
	pub(crate) operator: MatchesOperator,
	/// The first column of the index: the field the `MATCHES` the scan answers reads.
	pub(crate) column: Option<crate::expr::Idiom>,
	pub(crate) decode: Option<DecodeFields>,
	/// Where a bounded sort above orders first by the clause's `search::score`, the top-k key
	/// the scan rejects hits by: a hit whose score the sort's threshold shows worse than every
	/// row the sort keeps is passed over before its record is read. The key is the score the
	/// scan computes for the hit, so its path is empty.
	pub(crate) topk: Option<TopKKey>,
}

/// One bound of a key-range access.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyBoundShape {
	/// A value known when the plan is made, already one the component can hold.
	Literal(Value),
	/// A value the scan reads when it opens: the operand at this position of the op's operands.
	/// Slots are assigned in the order [`KeyRangeShape::bounds`] or [`IdBoundsShape::bounds`]
	/// lists the bounds, so the n-th operand bound holds slot n.
	Operand(OperandSlot),
}

/// The values one position of the equality run is pinned to.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyEqShape {
	/// One value.
	One(KeyBoundShape),
	/// Each of several values: one key range per value.
	In(Vec<KeyBoundShape>),
}

/// One side of the range on the position after the equality run.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRangeBoundShape {
	pub(crate) bound: KeyBoundShape,
	/// Whether a key equal to the bound is admitted.
	pub(crate) inclusive: bool,
}

/// The range on the position right after the equality run.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRangeOnShape {
	pub(crate) position: u32,
	pub(crate) low: Option<KeyRangeBoundShape>,
	pub(crate) high: Option<KeyRangeBoundShape>,
}

/// The expression-free shape of a key-range access: the structure of the table's keys it was
/// planned against, the equality run, the range after it, the direction and the order it delivers.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRangeShape {
	pub(crate) key: std::sync::Arc<KeyStructure>,
	pub(crate) eq: Vec<KeyEqShape>,
	pub(crate) range: Option<KeyRangeOnShape>,
	pub(crate) direction: Direction,
	pub(crate) delivers: DeliveredKeyOrder,
	/// The component position each `ORDER BY` key reads, in key order, when `delivers` is not
	/// [`DeliveredKeyOrder::None`]; empty otherwise.
	pub(crate) ordered_by: Vec<u32>,
}

impl KeyRangeShape {
	/// The bounds of the access in operand-slot order: the equality run by position, each `IN`
	/// element in written order, then the low and the high bound of the range.
	pub(crate) fn bounds(&self) -> impl Iterator<Item = &KeyBoundShape> {
		let eq = self.eq.iter().flat_map(|eq| match eq {
			KeyEqShape::One(bound) => std::slice::from_ref(bound).iter(),
			KeyEqShape::In(bounds) => bounds.iter(),
		});
		let range = self
			.range
			.iter()
			.flat_map(|range| [range.low.as_ref(), range.high.as_ref()])
			.flatten()
			.map(|side| &side.bound);
		eq.chain(range)
	}

	/// The number of bounds that are operands: the number of operands the op takes.
	pub(crate) fn holes(&self) -> usize {
		self.bounds().filter(|bound| matches!(bound, KeyBoundShape::Operand(_))).count()
	}
}

/// The interval of record ids a table scan reads: a side that is open reads to the end of the
/// table.
///
/// A bound is a record id of the scanned table whose key is a number, a string or a uuid, the
/// keys whose storage order is the order the record ids compare in.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IdBoundsShape {
	pub(crate) low: Option<KeyRangeBoundShape>,
	pub(crate) high: Option<KeyRangeBoundShape>,
}

impl IdBoundsShape {
	/// The bounds in operand-slot order: the low bound, then the high.
	pub(crate) fn bounds(&self) -> impl Iterator<Item = &KeyBoundShape> {
		[self.low.as_ref(), self.high.as_ref()].into_iter().flatten().map(|side| &side.bound)
	}

	/// The number of bounds that are operands: the number of operands the scan takes.
	pub(crate) fn holes(&self) -> usize {
		self.bounds().filter(|bound| matches!(bound, KeyBoundShape::Operand(_))).count()
	}
}

/// The attributes of a key-range scan.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRangeScanAttrs {
	pub(crate) table: TableName,
	pub(crate) read: ReadClauses,
	pub(crate) access: KeyRangeShape,
	/// The sort key a scan beneath a top-k sort reads from stored bytes to reject a row that
	/// cannot enter the top k; its coarse cutoff stops the scan past the second of the threshold.
	pub(crate) topk: Option<TopKKey>,
	/// The shape of the predicate the stored bytes of a row judge before the row is decoded, as
	/// [`TableScanAttrs::prefilter`] states it.
	pub(crate) prefilter: Option<PreDecodeShape>,
}

/// One permission a KNN search gates each candidate on. Its predicate is a `PermissionFrame`
/// region, followed by one `FieldFrame(Computed)` region per field `computed` derives.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KnnPermissionShape {
	pub(crate) auth: Option<AuthLimit>,
	pub(crate) guard: GuardKind,
	/// The computed fields the predicate reads, by table.
	pub(crate) computed: Option<ComputedReadsShape>,
}

impl KnnPermissionShape {
	/// The number of regions the permission takes: its predicate and the fields it derives.
	pub(crate) fn regions(&self) -> usize {
		1 + self.computed.as_ref().map_or(0, ComputedReadsShape::derived_regions)
	}
}

/// The prefilter of a KNN search.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KnnPrefilterShape {
	pub(crate) root: BitmapShape,
	/// Whether a residual predicate follows the bitmap, as the last region.
	pub(crate) residual: bool,
	pub(crate) uncovered_matches: bool,
}

/// What a KNN search gates each candidate on. The predicates are regions, in the order the op's
/// doc lists.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KnnFilterShape {
	pub(crate) permissions: Vec<KnnPermissionShape>,
	pub(crate) residual: bool,
	pub(crate) matches_condition: bool,
	pub(crate) prefilter: Option<KnnPrefilterShape>,
}

impl KnnFilterShape {
	/// The number of regions the permissions take, ahead of the conditions.
	pub(crate) fn permission_regions(&self) -> usize {
		self.permissions.iter().map(KnnPermissionShape::regions).sum()
	}
}

/// The attributes of a KNN scan.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KnnScanAttrs {
	pub(crate) table: TableName,
	pub(crate) read: ReadClauses,
	pub(crate) index: IndexRef,
	/// The query vector. Empty for a vector known only when the statement runs, which is the
	/// scan's one operand.
	pub(crate) vector: std::sync::Arc<[Number]>,
	/// The query vector as written, for a vector known only when the statement runs: the text a
	/// refusal of its value names.
	pub(crate) vector_text: Option<String>,
	pub(crate) k: u32,
	pub(crate) ef: u32,
	pub(crate) filter: Option<KnnFilterShape>,
	/// Whether nothing above the scan reads more of a row than its `id` and every field of the
	/// table reads as stored.
	pub(crate) ids_only: bool,
}

/// The target of a brute-force nearest-neighbour search.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KnnTargetShape {
	Literal(Vec<Number>),
	/// The vector is the value of the op's last operand, written as `text`: the text a refusal of
	/// its value names. With `before_rows`, the operand is a parameter the request binds or the
	/// catalog stores, which the statement evaluates before it reads a row: an unrankable one
	/// refuses the statement whatever it reads, as one known when the statement is planned does.
	Deferred(String, bool),
}

/// The attributes of a brute-force nearest-neighbour search.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KnnTopKAttrs {
	/// The path of each row the vector a row is ranked by is read from.
	pub(crate) field: FieldPath,
	pub(crate) vector: KnnTargetShape,
	pub(crate) k: u32,
	pub(crate) distance: Distance,
}

/// What a count answers from.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CountShape {
	/// Every record a source names. The source's value is the op's operand when it is not a
	/// table.
	All {
		source: SourceRefShape,
		output: CountOutput,
	},
	/// The records of a table a condition admits, counted from a `COUNT` index when the table has
	/// one, else by walking the keys of the accesses of `btree`, else as the exact cardinality of
	/// `bitmap`. The accesses of `btree` are record-disjoint, so the count is the sum of their key
	/// counts: one access for a condition one walk covers, one per value of an `IN` list or per
	/// branch of an `OR` whose branches no record can satisfy together. Both payloads are boxed,
	/// so a count of a source stays as small as its source shape.
	Indexed {
		table: TableName,
		output: CountOutput,
		btree: Option<Box<[(IndexRef, crate::exec::index::access_path::BTreeAccess)]>>,
		bitmap: Option<Box<BitmapShape>>,
	},
}

/// The attributes of a count.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CountAttrs {
	pub(crate) shape: CountShape,
	pub(crate) select: CountSelect,
	/// The `COUNT` index whose deltas the count reads, when it reads them.
	pub(crate) deltas: Option<IndexId>,
	/// The paging of a subquery whose rows the count stands for, applied to what it counts.
	pub(crate) cap: Option<crate::exec::vm::payload::CountCap>,
}

// ---------------------------------------------------------------------------------------------
// Relational operators
// ---------------------------------------------------------------------------------------------

/// The attributes of a join.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct JoinAttrs {
	pub(crate) matching: JoinMatch,
	pub(crate) build: JoinBuild,
	pub(crate) spill: Spill,
	/// The name the right rows land under.
	pub(crate) alias: String,
	/// The number of key pairs.
	pub(crate) keys: u16,
	/// Whether a residual predicate follows the key pairs.
	pub(crate) residual: bool,
	/// The key pair whose index the right side is read through for a left row's key, when it is.
	/// The join's last region is then the lookup arm
	/// ([`RegionKind::JoinLookupArm`](crate::exec::ir::iface::region_kind::RegionKind::JoinLookupArm)),
	/// which yields the right input's chain over a `phys.index_lookup` keyed by the arm's
	/// argument. The build side is `Right`, and is read for the left rows the lookup does not
	/// serve.
	pub(crate) lookup: Option<u16>,
}

/// The attributes of a permission filter.
///
/// The `AUTH LIMIT` the predicate runs under is the op's authority frame.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PermissionFilterAttrs {
	pub(crate) guard: GuardKind,
	/// Whether the filter hides the access paths beneath it from the planner.
	pub(crate) hides_access_paths: bool,
	pub(crate) computed: Option<ComputedReadsShape>,
}

/// How a sort key reads its value from a row.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SortKeyReadShape {
	/// A field path read from the row. Holds no region.
	Path(FieldPath),
	/// An expression: one `Clause(OrderKey)` region.
	Computed,
}

/// One key of a sort.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SortKeySpecShape {
	pub(crate) read: SortKeyReadShape,
	pub(crate) ascending: bool,
	pub(crate) collate: bool,
	pub(crate) numeric: bool,
}

/// The attributes of a sort.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SortAttrs {
	pub(crate) keys: Vec<SortKeySpecShape>,
	/// The number of rows the sort keeps, when it keeps a bounded top.
	pub(crate) keep: Option<usize>,
	/// Whether the scan beneath reads the first key from stored bytes.
	pub(crate) pushdown: bool,
	/// `ORDER BY RAND()`.
	pub(crate) random: bool,
	pub(crate) spill: Spill,
	/// `TEMPFILES`: the sort holds on disk what outgrows the memory it may use, unless it keeps a
	/// bounded top or orders at random.
	pub(crate) tempfiles: bool,
}

/// The attributes of a resolver layer.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LayerAttrs {
	pub(crate) fields: FieldsShape,
	/// The name each unaliased column lands under, one per `FieldShape::Single` with no alias, in
	/// the order the fields list them, as `ProjectAttrs::names` states them for a projection.
	pub(crate) names: Vec<OutputPath>,
	/// The fields the schema of the layer's input declares `COMPUTED`, as
	/// `ProjectAttrs::computed` states them.
	pub(crate) computed: Vec<Strand>,
	/// The `COMPUTED` fields of the layer's table its guards read, derived onto a copy of the row
	/// the guards are judged against, as `ProjectAttrs::judged` states them: one
	/// `FieldFrame(Computed)` region per field the shape derives follows the frame regions of the
	/// columns.
	pub(crate) judged: Option<ComputedReadsShape>,
}

/// The attributes of an aggregation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AggregateAttrs {
	/// The number of group keys: one `ErrorScope(ShapeToNone)` region each, before the field
	/// regions. With none, the grouping is one group.
	pub(crate) groups: u16,
	pub(crate) fields: FieldsShape,
	/// The name each field lands under in the group's object, as
	/// [`AggregateShape::names`](crate::exec::ir::logical::ops::AggregateShape::names) states
	/// them.
	pub(crate) names: Vec<OutputPath>,
	/// The fields that take the group's first value.
	pub(crate) first_value: Vec<usize>,
	pub(crate) spill: Spill,
}

/// One omitted path.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OmissionShape {
	Path(FieldPath),
	/// A walk from a source value. The source is the next region of the op, and each step or
	/// action that holds an expression takes the next region after it, a `Clause(Projection)`
	/// region that takes the document the walk applies it to: the element the walk is at for a
	/// `Matching` step or action, and `NONE` for an `IndexAt` step or `RemoveIndexAt` action. An
	/// aliased destructure entry takes a region too; a walk never evaluates it, because an
	/// aliased entry names an expression and no location.
	Walk {
		steps: Vec<WalkStepShape>,
		action: WalkActionShape,
	},
}

/// One step of an omission's walk.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum WalkStepShape {
	Field(String),
	Index(usize),
	IndexAt,
	Matching,
	Each,
	First,
	Last,
	Destructure(Box<[DestructureEntry]>),
	Nowhere,
}

/// What an omission's walk does where it ends.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum WalkActionShape {
	RemoveField(String),
	RemoveIndex(usize),
	RemoveIndexAt,
	RemoveMatching,
	RemoveFirst,
	RemoveLast,
	Clear,
	Destructure(Box<[DestructureEntry]>),
	Nothing,
}

/// One field of a projection.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectedFieldShape {
	pub(crate) output: FieldPath,
	pub(crate) aliased: bool,
	/// The position of the sort key this field is read from, when the sort beneath computed it.
	pub(crate) sort_key: Option<u16>,
}

/// How a projection lands in the output. The expressions are `Clause(Projection)` regions.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProjectionShape {
	/// The whole row. Holds no region.
	All,
	/// `SELECT VALUE e`: one region.
	Value,
	/// `SELECT VALUE e OMIT …`: one region, then the omissions' regions.
	OmittedValue {
		omit: Vec<OmissionShape>,
	},
	/// The value of the sort key at this position. Holds no region.
	SortedValue(u16),
	/// An object per row with one entry per field.
	Fields {
		fields: Vec<ProjectedFieldShape>,
		omit: Vec<OmissionShape>,
	},
	/// `*` followed by fields.
	Star {
		fields: Vec<ProjectedFieldShape>,
		omit: Vec<OmissionShape>,
	},
}

impl ProjectionShape {
	/// The omitted paths the projection removes, after its fields.
	pub(crate) fn omissions(&self) -> &[OmissionShape] {
		match self {
			ProjectionShape::All | ProjectionShape::Value | ProjectionShape::SortedValue(..) => &[],
			ProjectionShape::OmittedValue {
				omit,
			}
			| ProjectionShape::Fields {
				omit,
				..
			}
			| ProjectionShape::Star {
				omit,
				..
			} => omit,
		}
	}
}

/// One stage of a `MATCH` query.
#[derive(Clone, Debug)]
pub(crate) enum MatchStepAttrs {
	/// A read stage: one `Clause(Where)` region per predicate, `predicates` of them.
	Read {
		optional_group: Option<u32>,
		patterns: Box<[MatchPattern]>,
		predicates: u16,
	},
	Update(MatchUpdateAttrs),
	Delete {
		target: crate::expr::match_plan::BindingId,
		detach: crate::expr::match_plan::DetachMode,
	},
	Insert(MatchInsertAttrs),
	Output(MatchOutputShape),
	/// A step of the folded binding table: operands and clause regions as the shape states.
	Bind(BindStepShape),
}

// ---------------------------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------------------------

/// Where a write's subjects come from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WriteSubjectsShape {
	/// One operand per target.
	Targets,
	/// The relational input of the write.
	Rows,
	/// The relational input, then one operand per target the rows are guaranteed to be among.
	Guaranteed,
	/// The `INSERT` shape: the subject loop over the rows, then the `INTO` value when the
	/// statement names one. The rows are one value, or an `expr.array` of `expr.tuple_row`s, one
	/// per `VALUES` tuple.
	Inserts,
	/// `RELATE`: the operands `from`, `through` and `to`, in that order.
	Edges,
}

/// The attributes of a physical write.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PhysWriteAttrs {
	pub(crate) write: WriteAttrs,
	pub(crate) subjects: WriteSubjectsShape,
}

// The op storage of a container is dense: no payload widens the op past this bound.
const _: () = {
	assert!(::core::mem::size_of::<PhysOp>() <= 32);
};

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use super::PhysTag;

	#[test]
	fn mnemonics_are_unique_and_namespaced() {
		let mut seen = HashSet::new();
		for tag in PhysTag::ALL {
			let name = tag.mnemonic();
			assert!(name.starts_with("phys."), "{name} is outside the dialect");
			assert!(seen.insert(name), "{name} names two tags");
		}
	}

	#[test]
	fn tags_are_listed_in_order() {
		assert!(PhysTag::ALL.windows(2).all(|pair| pair[0] < pair[1]));
	}
}

/// Test helpers for the shape of a key-range access.
#[cfg(test)]
pub(crate) mod key_range_fixtures {
	use std::sync::Arc;

	use surrealdb_strand::Strand;

	use super::KeyStructure;
	use crate::expr::{KeyNames, Kind, KindLiteral};

	/// The structure of a table whose id is `TYPE [name: kind, ...]`.
	pub(crate) fn key(components: &[(&str, Kind)]) -> Arc<KeyStructure> {
		let names = KeyNames::new(
			components.iter().map(|(name, _)| Strand::new(*name)).collect(),
			components.len(),
		)
		.expect("valid names");
		let kind = Kind::Literal(KindLiteral::Array(
			components.iter().map(|(_, kind)| kind.clone()).collect(),
		));
		Arc::new(KeyStructure::from_id_field(&kind, &names).expect("a structure"))
	}

	/// `[city: string, time: datetime]`.
	pub(crate) fn reading() -> Arc<KeyStructure> {
		key(&[("city", Kind::String), ("time", Kind::Datetime)])
	}

	/// `[a: int, b: int]`.
	pub(crate) fn pair() -> Arc<KeyStructure> {
		key(&[("a", Kind::Int), ("b", Kind::Int)])
	}
}
