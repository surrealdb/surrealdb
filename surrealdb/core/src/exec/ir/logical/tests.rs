//! Unit tests of the logical dialect's own rules.
//!
//! Each test states an answer the dialect owes the interface, through the entry point a consumer
//! calls. The fixtures are one simple op per tag; a table that ranges over a tag or a variant is
//! an exhaustive `match`, so a new variant fails to compile until the table names it.

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;
use std::sync::Arc;

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::{Strand, TableName};

use super::ops::{
	AccessAttrs, AggregateShape, AlterAttrs, AssignAttrs, ClosureLoopAttrs, DefineAttrs, EventDefs,
	EventsAttrs, FieldShape, FieldsShape, GateClause, ImageView, LetAttrs, LetBinding, LiveAttrs,
	LogOp, LogTag, LookupAttrs, LookupSubjectShape, LoopKind, LoopSink, MatchInsertAttrs,
	MatchUpdateAttrs, ProjectAttrs, RebuildAttrs, RecurseAttrs, RecurseInstructionShape,
	RecursePathForm, RemoveAttrs, ShowAttrs, SortAttrs, SortKeyShape, SortShape, SourceAttrs,
	SourceOrder, SourceRefShape, StepAttrs, Stored, SubscriberAdmission, SubscriberAttrs,
	SubscriberProjection, TxnRegionEnd, TxnRegionMode, TxnStart, UseTarget, WindowCallShape,
	WindowShape, WriteAttrs, WriteGates, WriteImage, WriteMark, WriteSchema, WriteStageKind,
	WriteStages, WriteStored,
};
use super::print::LogicalSyntax;
use super::scope::{LoopExit, loop_exit, return_target};
use super::stages::{
	declares_id_default, gates_fault, gates_of, image_of, orders, stage_mutates_image, stage_run,
	statement_of_text, statement_text, static_mark, stored_of, view_of,
};
use super::txn::{StatementBody, StatementSlot, TopRegion, regions_of};
use super::verify::{loop_body_args, timestamp_arg, verify_op};
use crate::doc::Action;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope};
use crate::exec::ir::core::location::{LocOrigin, Location, Span};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::print::AttrSyntax;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Provenance, Rewriter};
use crate::exec::ir::core::verify::VerifyRule;
use crate::exec::ir::expr::ops::{ExprOp, KeyBound, KeyRange};
use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx, SemanticCatalog};
use crate::exec::ir::iface::choose::{
	ArmBody, ArmEquivalence, ArmLabel, ArmSpec, ChooseGranularity, ChooseOrigin, ChooseSpec,
	ExhaustiveFact, GenericArm, Guard,
};
use crate::exec::ir::iface::estimate::SizeBucket;
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::frame::FrameTable;
use crate::exec::ir::iface::identity::AttrDigest;
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::{
	ClauseRole, ErrorScope, FieldRole, RegionKind, RegionProperties, StatementCommit, StoredRole,
	TxnKind,
};
use crate::exec::ir::iface::statements::{
	AccessOp, AlterObject, BindStepShape, ContentMode, DdlChecks, DdlGuard, DefineObject,
	DefineTable, DefinitionTemplate, InfoSubject, LiveShape, MatchBindings, MatchData,
	MatchOutputShape, OutputShape, PermissionShape, PermissionsShape, Place, PlacePart,
	RemoveObject, ShowSince, TemplateHole, UnaddressedPart, WriteFlags, WriteForm,
};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::iface::unit::UnitKind;
use crate::exec::ir::op_kind::{OpKind, OpTag, OperandRule, Terminator};
use crate::exec::operators::write::plan::{WriteFacts, WriteForm as PlanForm};
use crate::exec::operators::write::stage::{LightweightReason, RecordImage, StageId, TableTypeFor};
use crate::expr::logical::{
	EffectSet, Emission, GuardKind, JoinKind, MisplacedControl, OutputPath, RelationAlias,
	RequestStatementKind, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction,
	lower_request,
};
use crate::expr::lookup::LookupKind;
use crate::expr::match_plan::DetachMode;
use crate::expr::part::Recurse;
use crate::expr::statements::define::DefineKind;
use crate::expr::table_type::{Relation, TableType};
use crate::expr::{AssignOperator, ChangeFeed, Dir, Explain, ExplainFormat, Kind, With};
use crate::kvs::cache::ds::TableSnapshot;
use crate::val::{Uuid, Value};

// ---------------------------------------------------------------------------------------------
// Support
// ---------------------------------------------------------------------------------------------

/// A semantic catalog that holds no definition.
struct NoCatalog;

impl SemanticCatalog for NoCatalog {}

/// What an effect context borrows: the builtin registry and a frame table.
struct Env {
	registry: FunctionRegistry,
	frames: FrameTable,
}

impl Env {
	fn new() -> Self {
		Self {
			registry: FunctionRegistry::with_builtins(),
			frames: FrameTable::default(),
		}
	}

	fn cx(&self) -> EffectCx<'_> {
		EffectCx {
			registry: &self.registry,
			catalog: &NoCatalog,
			frames: &self.frames,
		}
	}
}

fn strand(text: &str) -> Strand {
	Strand::new(text)
}

fn table(text: &str) -> TableName {
	TableName::new(strand(text))
}

fn template(ty: &'static str) -> DefinitionTemplate {
	DefinitionTemplate {
		ty,
		value: Value::None,
		holes: SmallVec::new(),
	}
}

fn write_attrs() -> WriteAttrs {
	WriteAttrs {
		form: WriteForm::Create,
		only: false,
		output: OutputShape::After,
		with: None,
		explain: None,
		flags: WriteFlags::default(),
		body: crate::exec::ir::logical::ops::WriteBody::default(),
		gates: crate::exec::ir::logical::ops::WriteGates::default(),
		image: crate::exec::ir::logical::ops::WriteImage::default(),
		stored: Default::default(),
		derived: Default::default(),
		into_text: None,
		statement: None,
		linked: None,
	}
}

fn place() -> Place {
	Place {
		parts: Box::new([PlacePart::Field(strand("a"))]),
	}
}

fn value_fields() -> FieldsShape {
	FieldsShape::Value {
		alias: None,
	}
}

fn choose_spec() -> ChooseSpec {
	ChooseSpec {
		granularity: ChooseGranularity::Statement,
		arms: smallvec![ArmSpec {
			guard: Guard::Otherwise,
			body: ArmBody::PreLowered,
			label: ArmLabel("generic"),
		}],
		generic: GenericArm::Present,
		equivalence: ArmEquivalence::SameRowsSameOrder,
		origin: ChooseOrigin::PhysicalGuarded,
	}
}

/// The simplest op of each logical tag.
fn fixture(tag: LogTag) -> LogOp {
	match tag {
		LogTag::Request => LogOp::Request {
			fragment: None,
			nested: false,
		},
		LogTag::Txn => LogOp::Txn {
			mode: TxnRegionMode::Explicit {
				start: TxnStart::Begin,
				end: TxnRegionEnd::Commit,
				emission: Emission::Stream,
			},
		},
		LogTag::Stmt => LogOp::Stmt {
			commit: StatementCommit::Auto,
			control: None,
		},
		LogTag::Statement => LogOp::Statement {
			changes_catalog: false,
			served: None,
		},
		LogTag::Source => LogOp::Source(Box::new(SourceAttrs {
			source_ref: SourceRefShape::Table(table("t")),
			with: None,
			for_update: false,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		})),
		LogTag::Values => LogOp::Values {
			for_update: false,
			written: false,
		},
		LogTag::Match => LogOp::Match {
			bindings: MatchBindings(Box::new([])),
		},
		LogTag::MatchRead => LogOp::MatchRead {
			optional_group: None,
			patterns: Box::new([]),
		},
		LogTag::MatchUpdate => LogOp::MatchUpdate(Box::new(MatchUpdateAttrs {
			target: 0,
			data: MatchData::Content,
		})),
		LogTag::MatchDelete => LogOp::MatchDelete {
			target: 0,
			detach: DetachMode::Detach,
		},
		LogTag::MatchInsert => LogOp::MatchInsert(Box::new(MatchInsertAttrs {
			nodes: Box::new([]),
			edges: Box::new([]),
		})),
		LogTag::MatchOutput => LogOp::MatchOutput(Box::new(MatchOutputShape {
			columns: Box::new([]),
			distinct: false,
			order: Box::new([]),
		})),
		LogTag::BindStep => LogOp::BindStep(Box::new(BindStepShape::Distinct)),
		LogTag::Subquery => LogOp::Subquery,
		LogTag::Write => LogOp::Write(Box::new(write_attrs())),
		LogTag::Assign => LogOp::Assign(Box::new(AssignAttrs {
			place: place(),
			op: AssignOperator::Assign,
		})),
		LogTag::Unset => LogOp::Unset {
			place: place(),
		},
		LogTag::Content => LogOp::Content {
			mode: ContentMode::Content,
		},
		LogTag::DdlGuard => LogOp::DdlGuard {
			guard: DdlGuard::RefuseNoWrite,
		},
		LogTag::Define => LogOp::Define(Box::new(DefineAttrs {
			object: DefineObject::Param {
				name: strand("p"),
				value: 0,
				comment: 1,
				permissions: PermissionShape::Full,
			},
			mode: DefineKind::Default,
			template: template("StoredParamDefinition"),
			checks: DdlChecks::None,
		})),
		LogTag::Remove => LogOp::Remove(Box::new(RemoveAttrs {
			object: RemoveObject::Param {
				name: strand("p"),
			},
			if_exists: false,
			checks: DdlChecks::None,
			expunge: false,
		})),
		LogTag::Alter => LogOp::Alter(Box::new(AlterAttrs {
			object: AlterObject::Namespace {
				compact: false,
			},
			if_exists: false,
			checks: DdlChecks::None,
			template: None,
		})),
		LogTag::Rebuild => LogOp::Rebuild(Box::new(RebuildAttrs {
			index: strand("ix"),
			table: table("t"),
			if_exists: false,
			concurrently: false,
		})),
		LogTag::UnionAll => LogOp::UnionAll,
		LogTag::Join => LogOp::Join {
			kind: JoinKind::Inner,
			alias: RelationAlias::new(0),
		},
		LogTag::Filter => LogOp::Filter,
		LogTag::PermissionFilter => LogOp::PermissionFilter {
			guard: GuardKind::Read,
			computed: Some(Box::new(super::ops::ComputedReadsShape {
				table: table("t"),
				fields: Some(vec!["c".to_owned()]),
				derived: super::ops::DerivedReads::LateBound,
			})),
		},
		LogTag::Split => LogOp::Split {
			targets: Box::new([]),
		},
		LogTag::Aggregate => LogOp::Aggregate(Box::new(AggregateShape {
			groups: 1,
			fields: value_fields(),
			names: Vec::new(),
			first_value: Vec::new(),
		})),
		LogTag::Distinct => LogOp::Distinct,
		LogTag::Sort => LogOp::Sort(Box::new(SortAttrs {
			ordering: SortShape::Random,
			tempfiles: false,
		})),
		LogTag::Window => LogOp::Window(Box::new(WindowShape {
			partitions: 0,
			order: Box::new([]),
			calls: Box::new([WindowCallShape {
				output: strand("w"),
				function: WindowFunction::RowNumber,
				args: 0,
				frame: WindowFrame {
					units: WindowFrameUnits::Rows,
					start: WindowFrameBound::UnboundedPreceding,
					end: WindowFrameBound::CurrentRow,
				},
			}]),
		})),
		LogTag::Limit => LogOp::Limit {
			limit: Some(0),
			start: None,
		},
		LogTag::Project => LogOp::Project(Box::new(ProjectAttrs {
			fields: value_fields(),
			layer: false,
			omit: 0,
			names: Vec::new(),
			computed: Vec::new(),
			sort_keys: Vec::new(),
			judged: None,
		})),
		LogTag::Fetch => LogOp::Fetch,
		LogTag::Timeout => LogOp::Timeout,
		LogTag::VersionScope => LogOp::VersionScope,
		LogTag::Only => LogOp::Only,
		LogTag::Explain => LogOp::Explain {
			analyze: false,
			format: ExplainFormat::Text,
		},
		LogTag::Eval => LogOp::Eval,
		LogTag::Block => LogOp::Block,
		LogTag::Let => LogOp::Let(Box::new(untyped_let("x"))),
		LogTag::Return => LogOp::Return,
		LogTag::Loop => LogOp::Loop {
			kind: LoopKind::For,
			sink: LoopSink::Discard,
			order: SourceOrder::Positional,
			step: None,
		},
		LogTag::If => LogOp::If,
		LogTag::Info => LogOp::Info {
			subject: InfoSubject::Root,
			structure: false,
		},
		LogTag::Sleep => LogOp::Sleep,
		LogTag::Use => LogOp::Use {
			target: UseTarget::Ns,
		},
		LogTag::Option => LogOp::Option {
			name: strand("IMPORT"),
			value: true,
		},
		LogTag::Access => LogOp::Access(Box::new(AccessAttrs {
			ac: strand("a"),
			base: None,
			op: AccessOp::Show {
				gr: None,
			},
		})),
		LogTag::Kill => LogOp::Kill,
		LogTag::Live => LogOp::Live(Box::new(LiveAttrs {
			id: Uuid::nil(),
			node: Uuid::nil(),
			fields: LiveShape::Select,
			template: template("StoredLiveQuery"),
		})),
		LogTag::Subscriber => LogOp::Subscriber(Box::new(SubscriberAttrs {
			checked: true,
			admission: SubscriberAdmission::All,
			view: ImageView::Plain,
			layers: Vec::new(),
			projection: SubscriberProjection::Subject,
			cond: false,
			fetch: 0,
		})),
		LogTag::Show => LogOp::Show(Box::new(ShowAttrs {
			table: None,
			since: ShowSince::Versionstamp(0),
			limit: None,
		})),
		LogTag::Choose => LogOp::Choose(Box::new(choose_spec())),
		LogTag::WriteStage => LogOp::WriteStage {
			stage: WriteStageKind::OutputWrite,
			mark: WriteMark::Compute,
		},
		LogTag::Events => LogOp::Events(Box::new(EventsAttrs {
			action: Action::Update,
			defs: EventDefs::default(),
			events: Box::new([]),
			unlowerable: false,
		})),
		LogTag::Yield => LogOp::Yield,
		LogTag::Next => LogOp::Next,
		LogTag::Break => LogOp::Break,
	}
}

fn untyped_let(name: &str) -> LetAttrs {
	LetAttrs {
		name: strand(name),
		binding: LetBinding::Local,
		ty: None,
	}
}

/// The failure class the interface declares for each logical tag. Every row is exact.
fn declared_class(tag: LogTag) -> Fallibility {
	let none = Fallibility::INFALLIBLE;
	let value = Fallibility::VALUE;
	let data = Fallibility::DATA;
	let context = Fallibility::CONTEXT;
	match tag {
		LogTag::Request
		| LogTag::Stmt
		| LogTag::Statement
		| LogTag::Block
		| LogTag::If
		| LogTag::Subquery
		| LogTag::UnionAll
		| LogTag::Distinct
		| LogTag::Project
		| LogTag::Filter
		| LogTag::Split
		| LogTag::Join
		| LogTag::Explain
		| LogTag::Choose
		| LogTag::Loop
		| LogTag::Yield
		| LogTag::Next
		| LogTag::Break
		| LogTag::Return
		| LogTag::Values
		| LogTag::Eval
		| LogTag::Timeout
		| LogTag::Match
		| LogTag::MatchRead
		| LogTag::BindStep
		| LogTag::MatchOutput => none,
		LogTag::Txn | LogTag::Source | LogTag::Fetch => data,
		LogTag::Limit | LogTag::Aggregate | LogTag::Sort | LogTag::Window => value,
		LogTag::Only => value | data,
		LogTag::VersionScope | LogTag::Sleep => value | context,
		LogTag::Let => none,
		LogTag::PermissionFilter | LogTag::Use | LogTag::Show => context | data,
		LogTag::Option | LogTag::DdlGuard => context,
		LogTag::Write
		| LogTag::WriteStage
		| LogTag::Events
		| LogTag::Define
		| LogTag::Remove
		| LogTag::Alter
		| LogTag::Rebuild
		| LogTag::Info
		| LogTag::Access
		| LogTag::Kill
		| LogTag::Live
		| LogTag::Subscriber
		| LogTag::MatchUpdate
		| LogTag::MatchDelete
		| LogTag::MatchInsert
		| LogTag::Assign
		| LogTag::Unset
		| LogTag::Content => Fallibility::TOP,
	}
}

/// Whether the digest of an op of `tag` is opaque.
fn is_opaque(tag: LogTag) -> bool {
	match tag {
		LogTag::Stmt
		| LogTag::Statement
		| LogTag::Txn
		| LogTag::Request
		| LogTag::Write
		| LogTag::WriteStage
		| LogTag::Events
		| LogTag::Define
		| LogTag::Remove
		| LogTag::Alter
		| LogTag::Rebuild
		| LogTag::Live
		| LogTag::Access
		| LogTag::Kill
		| LogTag::Return
		| LogTag::Break
		| LogTag::Next
		| LogTag::Yield
		| LogTag::Choose
		| LogTag::Loop => true,
		LogTag::Source
		| LogTag::Values
		| LogTag::Match
		| LogTag::MatchRead
		| LogTag::MatchUpdate
		| LogTag::MatchDelete
		| LogTag::MatchInsert
		| LogTag::MatchOutput
		| LogTag::BindStep
		| LogTag::Subquery
		| LogTag::Assign
		| LogTag::Unset
		| LogTag::Content
		| LogTag::DdlGuard
		| LogTag::UnionAll
		| LogTag::Join
		| LogTag::Filter
		| LogTag::PermissionFilter
		| LogTag::Split
		| LogTag::Aggregate
		| LogTag::Distinct
		| LogTag::Sort
		| LogTag::Window
		| LogTag::Limit
		| LogTag::Project
		| LogTag::Fetch
		| LogTag::Timeout
		| LogTag::VersionScope
		| LogTag::Only
		| LogTag::Explain
		| LogTag::Eval
		| LogTag::Block
		| LogTag::Let
		| LogTag::If
		| LogTag::Info
		| LogTag::Sleep
		| LogTag::Use
		| LogTag::Option
		| LogTag::Subscriber
		| LogTag::Show => false,
	}
}

// ---------------------------------------------------------------------------------------------
// Write stages
// ---------------------------------------------------------------------------------------------

/// Every executor stage with every payload it can carry.
fn every_stage() -> Vec<StageId> {
	let mut stages = vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckRecordExists,
		StageId::CheckTableNotView,
		StageId::CheckPermissionsQuickCreate,
		StageId::CheckUpdatePermissions,
		StageId::CheckDeletePermissions,
		StageId::CheckCreatePermissions,
		StageId::RecheckUpdatePermissions,
		StageId::CheckWhereCondition,
		StageId::ComputeInputData,
		StageId::CheckDataFields,
		StageId::ProcessRecordData,
		StageId::ProcessMergeData,
		StageId::DefaultRecordData,
		StageId::GenerateRecordId,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::CleanupTableReferences,
		StageId::ClearRecordData,
		StageId::StoreRecordData,
		StageId::StoreEdgesData,
		StageId::StoreInlineAdjacencyData,
		StageId::StoreIndexData,
		StageId::ReclaimDocId,
		StageId::PurgeRecordData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessChangefeeds,
		StageId::OutputWrite,
	];
	for reason in [LightweightReason::Updated, LightweightReason::GivenADataClause] {
		stages.push(StageId::CheckTableNotLightweight(reason));
	}
	for form in
		[TableTypeFor::Create, TableTypeFor::Upsert, TableTypeFor::Relate, TableTypeFor::Insert]
	{
		stages.push(StageId::CheckTableType(form));
	}
	for action in [Action::Create, Action::Update, Action::Delete] {
		stages.push(StageId::ProcessTableViews(action));
		stages.push(StageId::ProcessTableEvents(action));
		stages.push(StageId::ProcessTableLives(action));
	}
	for image in [RecordImage::Initial, RecordImage::Current] {
		stages.push(StageId::CheckSelectPermissions(image));
	}
	stages
}

const FORMS: [PlanForm; 7] = [
	PlanForm::Create,
	PlanForm::Update,
	PlanForm::Upsert,
	PlanForm::Delete,
	PlanForm::Relate,
	PlanForm::Insert,
	PlanForm::View,
];

/// Every combination of the statement facts that shape an order.
fn every_facts(form: PlanForm) -> Vec<WriteFacts> {
	let mut out = Vec::new();
	for bits in 0u8..16 {
		out.push(WriteFacts {
			form,
			has_data: bits & 1 != 0,
			on_duplicate_key_update: bits & 2 != 0,
			ignore: bits & 4 != 0,
			repeatable: bits & 8 != 0,
		});
	}
	out
}

/// The number of leading `Compute` stages of an order, after checking that the marks split the
/// order at most once: every stage from the first `Apply` on is `Apply`.
fn split_of(order: &[WriteStageKind]) -> usize {
	let marks: Vec<WriteMark> = order.iter().copied().map(static_mark).collect();
	let split = marks.iter().position(|mark| *mark == WriteMark::Apply).unwrap_or(marks.len());
	assert!(
		marks[split..].iter().all(|mark| *mark == WriteMark::Apply),
		"a Compute stage follows an Apply stage in {order:?}"
	);
	split
}

/// A statement binds each stage of its form once: the run holds every stage of every order of the
/// form exactly once, its first order's stages in that order, and its marks split it at most
/// once.
#[test]
fn stage_run_holds_every_stage_once() {
	for form in FORMS {
		for facts in every_facts(form) {
			let built = orders(form, facts);
			let run = stage_run(form, facts);
			for (index, stage) in run.iter().enumerate() {
				assert!(!run[..index].contains(stage), "{form:?} repeats {stage:?}");
			}
			for (_, order) in &built {
				for stage in order.iter() {
					assert!(run.contains(stage), "{form:?} drops {stage:?}");
				}
			}
			for stage in &run {
				assert!(
					built.iter().any(|(_, order)| order.contains(stage)),
					"{form:?} invents {stage:?}"
				);
			}
			if let Some((_, first)) = built.first() {
				let mut at = 0;
				for stage in first.iter() {
					at += run[at..]
						.iter()
						.position(|held| held == stage)
						.unwrap_or_else(|| panic!("{form:?} reorders {stage:?}"));
				}
			}
			split_of(&run);
		}
	}
}

/// The stages that build, derive or clear the image are the ones [`stage_mutates_image`] names,
/// and no stage that runs between the first apply stage and the stage that stores the record of an
/// order changes it: the physical plan encodes the image ahead of its apply wherever that holds.
#[test]
fn only_compute_stages_and_the_clearing_of_a_record_change_its_image() {
	assert!(stage_mutates_image(WriteStageKind::ComputeInputData));
	assert!(stage_mutates_image(WriteStageKind::ProcessTableFields));
	assert!(stage_mutates_image(WriteStageKind::ClearRecordData));
	assert!(stage_mutates_image(WriteStageKind::MaterialiseObservedFields));
	assert!(!stage_mutates_image(WriteStageKind::StoreRecordData));
	assert!(!stage_mutates_image(WriteStageKind::StoreIndexData));
	assert!(!stage_mutates_image(WriteStageKind::ProcessChangefeeds));
	assert!(!stage_mutates_image(WriteStageKind::OutputWrite));
	for form in FORMS {
		for facts in every_facts(form) {
			for (_, order) in orders(form, facts) {
				let Some(store) = order.iter().position(|s| *s == WriteStageKind::StoreRecordData)
				else {
					continue;
				};
				let first_apply = order
					.iter()
					.position(|stage| static_mark(*stage) == WriteMark::Apply)
					.expect("an order that stores applies");
				for stage in &order[first_apply..store] {
					assert!(
						!stage_mutates_image(*stage),
						"{form:?}: {stage:?} runs before the store"
					);
				}
			}
		}
	}
}

/// A catalog that answers every table with one fixed refusal to describe it.
struct TableAnswer(fn() -> CatalogAnswer<Arc<TableSnapshot>>);

impl SemanticCatalog for TableAnswer {
	fn table(&self, _name: &TableName) -> CatalogAnswer<Arc<TableSnapshot>> {
		(self.0)()
	}
}

/// The statement a write that names no one table carries reads back from the text it prints as.
#[test]
fn the_statement_of_a_write_reads_back_from_its_text() {
	for sql in [
		"DELETE $vertex<->?",
		"UPDATE type::table($table) SET a = 1 WHERE b > 2 RETURN NONE",
		"INSERT INTO $table { a: 1 }",
		"UPSERT $target CONTENT { a: 1 }",
	] {
		let parsed = crate::syn::parse(sql).expect("the statement parses");
		let Some(surrealdb_expr::parser::TopLevelExpr::Expr(expr)) =
			parsed.expressions.into_iter().next()
		else {
			panic!("`{sql}` is one statement");
		};
		let Ok(crate::expr::logical::LogicalOp::Write(write)) = crate::expr::logical::lower(expr)
		else {
			panic!("`{sql}` lowers to a write");
		};
		assert_eq!(statement_of_text(&statement_text(&write)), Ok(write), "{sql}");
	}
	assert!(statement_of_text("SELECT * FROM t").is_err());
	assert!(statement_of_text("DELETE a; DELETE b").is_err());
}

fn every(clause: GateClause) -> WriteGates {
	WriteGates {
		create: clause,
		update: clause,
		delete: clause,
		select: clause,
	}
}

/// The permission clauses of a write are never read as absent by default: a write that names no
/// one table states them dynamic, a table the catalog does not define states them absent, and a
/// catalog that cannot answer states them late-bound.
#[test]
fn a_gate_clause_is_never_read_from_what_the_catalog_does_not_answer() {
	let named = table("t");

	let held = gates_of(&NoCatalog, Some(&named));
	assert_eq!(held.gates, every(GateClause::LateBound));
	assert!(!held.has_predicate());

	let dynamic = gates_of(&NoCatalog, None);
	assert_eq!(dynamic.gates, every(GateClause::Dynamic));

	let absent = gates_of(&TableAnswer(|| CatalogAnswer::Absent), Some(&named));
	assert_eq!(absent.gates, every(GateClause::Absent));

	let pending = gates_of(&TableAnswer(|| CatalogAnswer::NotLoaded), Some(&named));
	assert_eq!(pending.gates, every(GateClause::LateBound));

	assert_eq!(WriteGates::default(), every(GateClause::Dynamic));
}

/// The facts of a table's images are never read as plain by default: a write that names no one
/// table states them dynamic, a table the catalog does not define has no field and so a plain
/// view, and a catalog that cannot answer states them late-bound.
#[test]
fn the_images_of_a_table_are_never_read_from_what_the_catalog_does_not_answer() {
	let named = table("t");
	assert_eq!(image_of(&NoCatalog, Some(&named)).view, ImageView::LateBound);
	assert_eq!(image_of(&NoCatalog, None).view, ImageView::Dynamic);
	assert_eq!(
		image_of(&TableAnswer(|| CatalogAnswer::NotLoaded), Some(&named)).view,
		ImageView::LateBound
	);
	let absent = image_of(&TableAnswer(|| CatalogAnswer::Absent), Some(&named));
	assert_eq!(
		absent,
		WriteImage {
			view: ImageView::Plain,
			keyed: false,
			id_default: false
		}
	);
	assert_eq!(WriteImage::default().view, ImageView::Dynamic);
}

/// What a table stores is never read as nothing by default: a write that names no one table states
/// it dynamic, a table the catalog does not define stores nothing and is not schemafull, and a
/// catalog that cannot answer states it late-bound. The statement's own fact about the catalog is
/// carried whichever way the table is stated.
#[test]
fn what_a_table_stores_is_never_read_from_what_the_catalog_does_not_answer() {
	let named = table("t");
	let all = |kind| WriteStored {
		table: Some(named.clone()),
		schemafull: false,
		catalog_fixed: true,
		fields: kind,
		references: kind,
		indexes: kind,
		events: kind,
		views: kind,
		referenced: kind,
		edges: kind,
		schema: WriteSchema::default(),
		reference_fields: Box::default(),
		materialized: Box::default(),
		index_set: Default::default(),
		view_set: Default::default(),
		repair_set: Default::default(),
		image_layers: Default::default(),
	};
	assert_eq!(stored_of(&NoCatalog, Some(&named), true), all(Stored::LateBound));
	assert_eq!(
		stored_of(&TableAnswer(|| CatalogAnswer::NotLoaded), Some(&named), true),
		all(Stored::LateBound)
	);
	assert_eq!(
		stored_of(&TableAnswer(|| CatalogAnswer::Absent), Some(&named), true),
		all(Stored::Nothing)
	);
	let dynamic = stored_of(&NoCatalog, None, false);
	assert_eq!(dynamic, WriteStored::default());
	assert_eq!(dynamic.table, None);
	assert_eq!(dynamic.fields, Stored::Dynamic);
	assert!(stored_of(&NoCatalog, None, true).catalog_fixed);
	assert_eq!(WriteStored::default().views, Stored::Dynamic);
}

/// The repairs of a delete are stated for a write that purges a record and for no other, and never
/// read from what the catalog does not answer: a write that names no one table states them
/// dynamic, a table the catalog does not define is referenced by no field, and a catalog that
/// cannot answer states them late-bound. A write that does not purge asks the catalog nothing,
/// because the references of a table are a database-wide read.
#[test]
fn the_repairs_of_a_delete_are_never_read_from_what_the_catalog_does_not_answer() {
	use std::sync::atomic::{AtomicBool, Ordering};

	use super::ops::WriteRepairs;
	use super::stages::repairs_of;

	struct Asked {
		table: fn() -> CatalogAnswer<Arc<TableSnapshot>>,
		asked: AtomicBool,
	}

	impl SemanticCatalog for Asked {
		fn table(&self, _name: &TableName) -> CatalogAnswer<Arc<TableSnapshot>> {
			(self.table)()
		}

		fn referencing(
			&self,
			_table: &TableName,
		) -> CatalogAnswer<crate::exec::catalog_deps::Referencing> {
			self.asked.store(true, Ordering::Relaxed);
			CatalogAnswer::NotLoaded
		}
	}

	let named = table("t");
	let dynamic = repairs_of(&NoCatalog, None, true);
	assert_eq!(dynamic.referenced, Stored::Dynamic);
	assert_eq!(dynamic.facts, WriteRepairs::default());
	assert!(dynamic.regions.is_empty());

	for (answer, expected) in [
		((|| CatalogAnswer::Absent) as fn() -> CatalogAnswer<Arc<TableSnapshot>>, Stored::Nothing),
		(|| CatalogAnswer::NotLoaded, Stored::LateBound),
		(|| CatalogAnswer::Unknown, Stored::LateBound),
	] {
		let catalog = Asked {
			table: answer,
			asked: AtomicBool::new(false),
		};
		let purging = repairs_of(&catalog, Some(&named), true);
		assert_eq!(purging.referenced, expected);
		assert_eq!(purging.facts, WriteRepairs::default());
		assert!(
			!catalog.asked.load(Ordering::Relaxed),
			"the references of a table the catalog cannot describe are not asked for"
		);
		let other = repairs_of(&catalog, Some(&named), false);
		assert_eq!(other.referenced, Stored::Nothing);
		assert!(!catalog.asked.load(Ordering::Relaxed), "a write that does not purge asks nothing");
	}
}

/// The repairs of a table are the `REFERENCE` fields that can name its records, by the table that
/// defines them and in the order it holds them, each with the strategy its `ON DELETE` clause
/// directs. A `THEN` expression is the one clause region its field hosts, and a field with no
/// reference states nothing.
#[test]
fn the_repairs_of_a_table_follow_the_clause_of_each_reference_field() {
	use super::ops::{RepairField, RepairKind, RepairTable};
	use super::stages::repairs_over;
	use crate::catalog::FieldDefinition;
	use crate::exec::catalog_deps::Referencing;
	use crate::expr::Idiom;
	use crate::expr::reference::{Reference, ReferenceDeleteStrategy as Strategy};

	let field = |name: &str, on_delete: Option<Strategy>| FieldDefinition {
		name: Idiom::field(Strand::new(name)),
		table: table("author"),
		reference: on_delete.map(|on_delete| Reference {
			on_delete,
			materialize: None,
		}),
		..FieldDefinition::default()
	};
	let then = Strategy::Custom(crate::syn::expr("$reference.name").expect("an expression"));
	let referencing: Referencing = Arc::from(vec![
		(
			table("author"),
			Arc::from(vec![
				field("a", Some(Strategy::Ignore)),
				field("plain", None),
				field("b", Some(then)),
				field("c", Some(Strategy::Unset)),
				field("d", Some(Strategy::Cascade)),
				field("e", Some(Strategy::Reject)),
			]),
		),
		(table("bare"), Arc::from(vec![field("plain", None)])),
	]);
	let repairs = repairs_over(&referencing);
	assert_eq!(repairs.referenced, Stored::Declared);
	assert!(!repairs.facts.unlowerable);
	assert_eq!(
		repairs.facts.tables.as_ref(),
		[RepairTable {
			table: table("author"),
			fields: vec![
				RepairField {
					name: Idiom::field(Strand::new("a")),
					strategy: RepairKind::Ignore,
				},
				RepairField {
					name: Idiom::field(Strand::new("b")),
					strategy: RepairKind::Custom,
				},
				RepairField {
					name: Idiom::field(Strand::new("c")),
					strategy: RepairKind::Unset,
				},
				RepairField {
					name: Idiom::field(Strand::new("d")),
					strategy: RepairKind::Cascade,
				},
				RepairField {
					name: Idiom::field(Strand::new("e")),
					strategy: RepairKind::Reject,
				},
			]
			.into_boxed_slice(),
		}],
		"a table with no reference field is no repair"
	);
	assert_eq!(repairs.facts.region_count(), 1);
	assert_eq!(repairs.regions.len(), 1, "one region for the one THEN");

	let none: Referencing = Arc::from(Vec::new());
	let empty = repairs_over(&none);
	assert_eq!(empty.referenced, Stored::Nothing);
	assert_eq!(empty.facts.region_count(), 0);
}

/// A field view derives when any field is `COMPUTED` and cuts when any field's select permission
/// is not `FULL`; only a table with neither has a plain view, and a table with no field has one.
#[test]
fn a_view_that_derives_or_cuts_is_never_read_as_plain() {
	use crate::catalog::{DefineDefault, FieldDefinition, Permission};
	use crate::expr::{Expr, Literal};

	let field = |computed: bool, select: Permission| FieldDefinition {
		computed: computed.then(|| Expr::Literal(Literal::Bool(true))),
		select_permission: select,
		..FieldDefinition::default()
	};
	assert_eq!(view_of(&[]), ImageView::Plain);
	assert_eq!(view_of(&[field(false, Permission::Full)]), ImageView::Plain);
	assert_eq!(
		view_of(&[field(false, Permission::Full), field(false, Permission::None)]),
		ImageView::Cut
	);
	assert_eq!(view_of(&[field(true, Permission::Full)]), ImageView::Derived);
	assert_eq!(
		view_of(&[field(true, Permission::None), field(false, Permission::None)]),
		ImageView::Derived
	);

	let id = |default: DefineDefault| FieldDefinition {
		name: crate::expr::Idiom::field(surrealdb_strand::Strand::new("id")),
		default,
		..FieldDefinition::default()
	};
	let literal = || Expr::Literal(Literal::Bool(true));
	assert!(!declares_id_default(&[]));
	assert!(!declares_id_default(&[id(DefineDefault::None)]));
	assert!(!declares_id_default(&[id(DefineDefault::Always(literal()))]));
	assert!(declares_id_default(&[id(DefineDefault::Set(literal()))]));
}

/// A gate stage hosts a predicate exactly when its clause is one, and no other stage but
/// `ProcessTableFields` does.
#[test]
fn a_gate_stage_hosts_a_predicate_exactly_when_its_clause_is_one() {
	let predicate = GateClause::Predicate {
		derives: false,
	};
	let gates = WriteGates {
		create: GateClause::Full,
		update: predicate,
		delete: GateClause::None,
		select: GateClause::Absent,
	};
	let stages = |update: bool, create: bool, other: bool| {
		vec![
			(WriteStageKind::CheckCreatePermissions, create),
			(WriteStageKind::CheckUpdatePermissions, update),
			(WriteStageKind::RecheckUpdatePermissions, update),
			(WriteStageKind::CheckDeletePermissions, false),
			(WriteStageKind::OutputWrite, other),
		]
	};
	assert_eq!(gates_fault(gates, &stages(true, false, false)), None);
	assert!(gates_fault(gates, &stages(false, false, false)).is_some());
	assert!(gates_fault(gates, &stages(true, true, false)).is_some());
	assert!(gates_fault(gates, &stages(true, false, true)).is_some());
	assert_eq!(gates_fault(WriteGates::default(), &stages(false, false, false)), None);
	// The field permission predicates `ProcessTableFields` hosts are its schema's, not a gate's.
	let mut fields = stages(true, false, false);
	fields.push((WriteStageKind::ProcessTableFields, true));
	assert_eq!(gates_fault(gates, &fields), None);
}

/// Field definitions for the schema stage, of the table `t`.
mod schema_fixture {
	use surrealdb_strand::{Strand, TableName};

	use crate::catalog::{DefineDefault, FieldDefinition, LogicalBody, Permission};
	use crate::expr::{Expr, Idiom, Literal};

	/// A field `name` of `t` that states no clause and narrows no authority.
	pub(super) fn field(name: &str) -> FieldDefinition {
		FieldDefinition {
			name: Idiom::field(Strand::new(name)),
			table: TableName::new(Strand::new("t")),
			auth_limit: crate::catalog::auth::AuthLimit::new_no_limit(),
			..FieldDefinition::default()
		}
	}

	pub(super) fn literal() -> Expr {
		Expr::Literal(Literal::Bool(true))
	}

	/// An expression that reads the field `name` of its record.
	pub(super) fn reading(name: &str) -> Expr {
		Expr::Idiom(Idiom::field(Strand::new(name)))
	}

	pub(super) fn with_value(mut field: FieldDefinition, value: Expr) -> FieldDefinition {
		field.value_body = Some(LogicalBody::of(&value));
		field.value = Some(value);
		field
	}

	pub(super) fn with_default(mut field: FieldDefinition, default: Expr) -> FieldDefinition {
		field.default_body = Some(LogicalBody::of(&default));
		field.default = DefineDefault::Set(default);
		field
	}

	pub(super) fn with_assert(mut field: FieldDefinition, assert: Expr) -> FieldDefinition {
		field.assert_body = Some(LogicalBody::of(&assert));
		field.assert = Some(assert);
		field
	}

	pub(super) fn computed(mut field: FieldDefinition, body: Expr) -> FieldDefinition {
		field.computed_body = Some(LogicalBody::of(&body));
		field.computed = Some(body);
		field
	}

	pub(super) fn guarded(mut field: FieldDefinition, create: Permission) -> FieldDefinition {
		field.create_permission = create;
		field
	}
}

/// The steps of a table's field schema are its stored fields in the order they are produced, each
/// stating the clauses it has, and a field with no clause states a step with no region.
#[test]
fn a_stored_field_states_the_clauses_it_has_and_a_field_with_none_states_none() {
	use schema_fixture::*;

	use super::ops::{FieldClause, FieldClauses, FieldGate, FieldStep};
	use super::stages::schema_over;
	use crate::catalog::Permission;
	use crate::expr::logical::EvalFrame;

	let registry = FunctionRegistry::with_builtins();
	let plain = schema_over(&[field("a")], &registry);
	assert!(!plain.facts.unlowerable);
	assert_eq!(plain.facts.fields.as_ref(), [Strand::new("a")]);
	assert_eq!(plain.facts.id_field, None);
	assert_eq!(
		plain.facts.steps.as_ref(),
		[FieldStep::Field(FieldClauses {
			index: 0,
			default: None,
			value: None,
			assert: None,
			create: FieldGate::Allow,
			update: FieldGate::Allow,
		})]
	);
	assert!(plain.regions.is_empty());
	assert!(plain.facts.region_counts().kinds().is_empty());

	// A schema is stated for a table that declares a field, and for none else.
	assert_eq!(schema_over(&[], &registry).facts, WriteSchema::default());

	let clauses =
		with_assert(with_value(with_default(field("b"), literal()), literal()), literal());
	let clauses = guarded(clauses, Permission::None);
	let schema = schema_over(&[clauses], &registry);
	let [FieldStep::Field(stated)] = schema.facts.steps.as_ref() else {
		panic!("a stored field is one step");
	};
	let clause = Some(FieldClause {
		caller_bound: false,
	});
	assert_eq!((stated.default, stated.value, stated.assert), (clause, clause, clause));
	assert_eq!(stated.create, FieldGate::Deny);
	assert_eq!(stated.update, FieldGate::Allow);
	// One region per clause the steps state, of its kind, in the order the stage hosts them.
	let kinds: Vec<_> = schema.regions.iter().map(|region| region.kind).collect();
	assert_eq!(
		kinds,
		[
			RegionKind::FieldFrame(FieldRole::Default),
			RegionKind::FieldFrame(FieldRole::Value),
			RegionKind::FieldFrame(FieldRole::Assert),
		]
	);
	assert_eq!(schema.facts.region_counts().kinds(), kinds);
	// The clause of a field runs in the caller's mode, under the field's `AUTH LIMIT`, over the
	// names of the field's place.
	for region in &schema.regions {
		assert!(region.place);
		assert_eq!(region.frame, EvalFrame::caller());
	}
}

/// A field declaring no `DEFAULT` takes a static `VALUE` as one; a `VALUE` that reads another field
/// is no default.
#[test]
fn a_static_value_stands_as_the_default_of_a_field_that_declares_none() {
	use schema_fixture::*;

	use super::ops::FieldStep;
	use super::stages::schema_over;

	let registry = FunctionRegistry::with_builtins();
	let fixed = schema_over(&[with_value(field("a"), literal())], &registry);
	let [FieldStep::Field(stated)] = fixed.facts.steps.as_ref() else {
		panic!("a stored field is one step");
	};
	assert!(stated.default.is_some() && stated.value.is_some());
	let kinds: Vec<_> = fixed.regions.iter().map(|region| region.kind).collect();
	assert_eq!(
		kinds,
		[RegionKind::FieldFrame(FieldRole::Default), RegionKind::FieldFrame(FieldRole::Value)]
	);
	assert_eq!(fixed.regions[0].body, fixed.regions[1].body);

	let read = schema_over(&[with_value(field("a"), reading("b")), field("b")], &registry);
	let steps: Vec<_> = read
		.facts
		.steps
		.iter()
		.map(|step| match step {
			FieldStep::Field(stated) => (stated.index, stated.default.is_some()),
			FieldStep::Derive(_) => panic!("no field is computed"),
		})
		.collect();
	// `b` is produced before `a`, which reads it, and only `a` has a clause.
	assert_eq!(steps, [(1, false), (0, false)]);
	assert_eq!(read.facts.region_counts().value, 1);
	assert_eq!(read.facts.region_counts().default, 0);
}

/// The columns and the guards of the indexes that are not marked for removal are the regions of
/// the stage that maintains them, one per column then one per guard, index by index in the
/// catalog's order, each under the statement's own frame; the definitions are stated whole.
#[test]
fn the_columns_and_guards_of_the_maintained_indexes_are_regions_of_the_stage() {
	use super::ops::IndexDefs;
	use super::stages::indexes_over;
	use crate::catalog::{Index, IndexDefinition, IndexId};
	use crate::expr::logical::EvalFrame;

	let index =
		|id: u32, cols: &[&str], kind: Index, guard: Option<&str>, removed: bool| IndexDefinition {
			index_id: IndexId(id),
			name: Strand::new(format!("ix{id}")),
			table_name: table("t"),
			cols: cols.iter().map(|col| crate::syn::idiom(col).expect("an idiom")).collect(),
			index: kind,
			count_cond: guard
				.map(|text| crate::expr::Cond(crate::syn::expr(text).expect("an expression"))),
			prepare_remove: removed,
			format_version: 3,
			comment: None,
			nulls_not_distinct: false,
		};
	let defs: std::sync::Arc<[IndexDefinition]> = std::sync::Arc::from(vec![
		index(1, &["a", "b.c"], Index::Idx, None, false),
		index(2, &["z"], Index::Uniq, None, true),
		index(3, &[], Index::Count(None), Some("active = true"), false),
	]);
	let indexes = indexes_over(&defs, None);
	assert!(!indexes.facts.unlowerable);
	assert_eq!(indexes.facts.defs, IndexDefs::new(defs));
	assert_eq!(indexes.facts.region_count(), 3);
	assert_eq!(indexes.regions.len(), 3);
	for region in &indexes.regions {
		assert_eq!(
			region.kind,
			RegionKind::Clause(crate::exec::ir::iface::region_kind::ClauseRole::IndexColumn)
		);
		assert_eq!(region.frame, EvalFrame::caller());
		assert!(!region.place);
	}
	// A table that declares no index states nothing.
	let none = indexes_over(&std::sync::Arc::from(Vec::new()), None);
	assert_eq!(none.facts, super::ops::WriteIndexes::default());
	assert!(none.regions.is_empty());
}

/// An index definition is hashed, printed and read back by value, so the printed text of a write
/// that names it identifies the definitions the program maintains from.
#[test]
fn the_definitions_of_an_index_set_read_back_from_their_text() {
	use super::ops::IndexDefs;
	use crate::catalog::{Index, IndexDefinition, IndexId};

	let def = |id: u32, name: &str, kind: Index, removed: bool| IndexDefinition {
		index_id: IndexId(id),
		name: Strand::new(name),
		table_name: table("t"),
		cols: vec![crate::syn::idiom("a.b").expect("an idiom")],
		index: kind,
		count_cond: None,
		prepare_remove: removed,
		format_version: 3,
		comment: Some("kept".to_owned()),
		nulls_not_distinct: false,
	};
	let set = IndexDefs::new(std::sync::Arc::from(vec![
		def(1, "first", Index::Idx, false),
		def(2, "second", Index::Uniq, true),
	]));
	let text = set.to_text();
	assert_eq!(IndexDefs::from_text(&text).expect("the text reads"), set);
	assert_eq!(IndexDefs::from_text("").expect("an empty set"), IndexDefs::default());
	assert!(IndexDefs::from_text("zz").is_err());
	assert!(IndexDefs::from_text("abc").is_err());
	let other = IndexDefs::new(std::sync::Arc::from(vec![def(1, "first", Index::Uniq, false)]));
	assert_ne!(set, other);
	let hash = |set: &IndexDefs| {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::collections::hash_map::DefaultHasher::new();
		set.hash(&mut hasher);
		hasher.finish()
	};
	assert_eq!(hash(&set), hash(&IndexDefs::from_text(&text).expect("the text reads")));
	assert_ne!(hash(&set), hash(&other));
}

/// The clauses of the views a table feeds are the regions of the stage that maintains them, view
/// by view in the catalog's order, each under the frame of a view; the definitions are stated
/// whole. A materialized view hosts its condition and its projection, an aggregated one its
/// condition, its group expressions, its aggregate arguments and, for each field, the expression
/// and then the subscripts and filters its name embeds.
#[test]
fn the_clauses_of_the_maintained_views_are_regions_of_the_stage() {
	use super::ops::TableDefs;
	use super::stages::{ViewBody, view_table_for_test, views_over};

	let materialized = view_table_for_test("m", "a, b AS c", Some("a > 1"), None);
	let plain = view_table_for_test("p", "*", None, None);
	let aggregated = view_table_for_test(
		"g",
		"a, count() AS n, math::max(x) AS top",
		Some("x != NONE"),
		Some(&["a"]),
	);
	let defs: std::sync::Arc<[crate::catalog::TableDefinition]> =
		std::sync::Arc::from(vec![materialized, plain, aggregated]);
	let views = views_over(&defs);
	assert!(!views.facts.unlowerable);
	assert_eq!(views.facts.defs, TableDefs::new(std::sync::Arc::clone(&defs)));
	assert_eq!(views.facts.region_count(), views.regions.len());
	// `m`: condition and projection. `p`: projection. `g`: condition, one group expression, the
	// aggregate arguments (`x` for the maximum), then the three fields' expressions.
	let shape: Vec<bool> =
		views.regions.iter().map(|body| matches!(body, ViewBody::Fields(_))).collect();
	assert_eq!(&shape[..3], &[false, true, true]);
	assert!(shape[3..].iter().all(|fields| !fields));
	let (groups, arguments) = match &defs[2].view {
		Some(crate::catalog::ViewDefinition::Aggregated {
			analysis,
			..
		}) => (analysis.group_expressions.len(), analysis.aggregate_arguments.len()),
		_ => panic!("the third view is aggregated"),
	};
	assert_eq!(groups, 1);
	assert_eq!(views.regions.len(), 3 + 1 + groups + arguments + 3);

	// A table that feeds no view states nothing.
	let none = views_over(&std::sync::Arc::from(Vec::new()));
	assert_eq!(none.facts, super::ops::WriteViews::default());
	assert!(none.regions.is_empty());
}

/// A subscript or a filter a field's name embeds is a region of its own, after the field's
/// expression, in the order the name's parts hold them.
#[test]
fn a_subscript_a_group_name_embeds_is_a_region_after_its_field() {
	use super::stages::{view_table_for_test, views_over};

	// A field's name is its alias, or its expression simplified, which keeps no subscript: the
	// subscript the name embeds is written in the alias.
	let embedded =
		view_table_for_test("g", "tags[0] AS first[0], count() AS n", None, Some(&["tags[0]"]));
	let defs: std::sync::Arc<[crate::catalog::TableDefinition]> =
		std::sync::Arc::from(vec![embedded]);
	let views = views_over(&defs);
	assert!(!views.facts.unlowerable);
	assert_eq!(views.facts.region_count(), views.regions.len());
	let Some(crate::catalog::ViewDefinition::Aggregated {
		analysis,
		..
	}) = &defs[0].view
	else {
		panic!("the view is aggregated");
	};
	let crate::catalog::aggregation::AggregateFields::Fields(items) = &analysis.fields else {
		panic!("the view selects fields");
	};
	let embeds: usize = items.iter().map(|(name, _)| super::ops::embedded_parts(name)).sum();
	assert_eq!(embeds, 1, "only the first field's name holds a subscript");
	// One group expression, no aggregate argument for `count()`, then each field with its
	// embedded part.
	assert_eq!(
		views.regions.len(),
		analysis.group_expressions.len()
			+ analysis.aggregate_arguments.len()
			+ items.len()
			+ embeds
	);
}

/// A view that selects a bare `VALUE` has no region the lowering could run.
#[test]
fn a_view_with_no_hostable_clause_states_no_region() {
	use super::stages::{view_table_for_test, views_over};

	let mut broken = view_table_for_test("m", "a", None, None);
	broken.view = None;
	let views = views_over(&std::sync::Arc::from(vec![broken]));
	assert!(views.facts.unlowerable);
	assert_eq!(views.facts.region_count(), 0);
	assert!(views.regions.is_empty());
}

/// A table definition is hashed, printed and read back by value, so the printed text of a write
/// that feeds a view identifies the definitions the program maintains it from.
#[test]
fn the_definitions_of_a_view_set_read_back_from_their_text() {
	use super::ops::TableDefs;
	use super::stages::view_table_for_test;

	let set = TableDefs::new(std::sync::Arc::from(vec![
		view_table_for_test("m", "a, b AS c", Some("a > 1"), None),
		view_table_for_test("g", "a, count() AS n", None, Some(&["a"])),
	]));
	let text = set.to_text();
	assert_eq!(TableDefs::from_text(&text).expect("the text reads"), set);
	assert_eq!(TableDefs::from_text("").expect("an empty set"), TableDefs::default());
	assert!(TableDefs::from_text("zz").is_err());
	assert!(TableDefs::from_text("abc").is_err());
	let other =
		TableDefs::new(std::sync::Arc::from(vec![view_table_for_test("m", "a", None, None)]));
	assert_ne!(set, other);
	let hash = |set: &TableDefs| {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::collections::hash_map::DefaultHasher::new();
		set.hash(&mut hasher);
		hasher.finish()
	};
	assert_eq!(hash(&set), hash(&TableDefs::from_text(&text).expect("the text reads")));
	assert_ne!(hash(&set), hash(&other));
}

/// A `COMPUTED` field another field's clause reads is derived at its place in the order, in the
/// computed frame, and the one nothing reads is left to the read side. Every computed field is
/// named for the instruction that strips them.
#[test]
fn a_computed_field_another_clause_reads_is_derived_at_its_place() {
	use schema_fixture::*;

	use super::ops::FieldStep;
	use super::stages::schema_over;
	use crate::expr::logical::EvalFrame;

	let registry = FunctionRegistry::with_builtins();
	let fields = [
		with_value(field("a"), reading("c")),
		computed(field("c"), literal()),
		computed(field("z"), literal()),
	];
	let schema = schema_over(&fields, &registry);
	assert!(!schema.facts.unlowerable);
	let derived: Vec<_> = schema
		.facts
		.steps
		.iter()
		.map(|step| match step {
			FieldStep::Derive(field) => format!("derive {}", field.name.as_str()),
			FieldStep::Field(stated) => format!("field {}", stated.index),
		})
		.collect();
	assert_eq!(derived, ["derive c", "field 0"]);
	assert_eq!(
		schema.facts.computed.as_ref(),
		[Strand::new("c"), Strand::new("z")],
		"every computed field is stripped, the ones no step derives among them"
	);
	let counts = schema.facts.region_counts();
	assert_eq!((counts.value, counts.derive, counts.predicate_derive), (1, 1, 0));
	let kinds: Vec<_> = schema.regions.iter().map(|region| region.kind).collect();
	assert_eq!(
		kinds,
		[RegionKind::FieldFrame(FieldRole::Value), RegionKind::FieldFrame(FieldRole::Computed)]
	);
	assert_eq!(schema.regions[1].frame, EvalFrame::computed(None));
	assert!(!schema.regions[1].place);
}

/// A field's `PERMISSIONS FOR create` predicate is a `PermissionFrame` region over the names of the
/// field's place, in the frame of a permission a write triggers, and a field no clause of which can
/// be judged states no step.
#[test]
fn a_field_permission_predicate_is_a_region_in_the_frame_of_a_permission_a_write_triggers() {
	use schema_fixture::*;

	use super::ops::{FieldGate, FieldStep};
	use super::stages::schema_over;
	use crate::catalog::{FieldDefinition, Permission};
	use crate::expr::logical::EvalFrame;

	let registry = FunctionRegistry::with_builtins();
	let schema = schema_over(&[guarded(field("a"), Permission::Specific(literal()))], &registry);
	let [FieldStep::Field(stated)] = schema.facts.steps.as_ref() else {
		panic!("a stored field is one step");
	};
	assert_eq!(
		stated.create,
		FieldGate::Predicate {
			caller_bound: false,
			derives: Box::new([]),
		}
	);
	assert_eq!(stated.update, FieldGate::Allow);
	let [region] = schema.regions.as_slice() else {
		panic!("a predicate is one region");
	};
	assert_eq!(region.kind, RegionKind::PermissionFrame);
	assert_eq!(region.frame, EvalFrame::mutable_permission(None));
	assert!(region.place);

	// A computed field that is not named by one field cannot be keyed by a derivation.
	let nested = computed(
		FieldDefinition {
			name: crate::expr::Idiom::field(Strand::new("a"))
				.push(crate::expr::part::Part::Field(Strand::new("b"))),
			..field("a")
		},
		literal(),
	);
	assert!(schema_over(&[nested], &registry).facts.unlowerable);
}

/// Producing clauses that read each other in a cycle state the cycle and no step.
#[test]
fn clauses_that_read_each_other_in_a_cycle_state_the_cycle_and_no_step() {
	use schema_fixture::*;

	use super::stages::schema_over;

	let registry = FunctionRegistry::with_builtins();
	let schema = schema_over(
		&[with_value(field("a"), reading("b")), with_value(field("b"), reading("a"))],
		&registry,
	);
	assert_eq!(schema.facts.cycle.as_deref(), Some("a, b"));
	assert!(schema.facts.steps.is_empty() && schema.regions.is_empty());
	assert_eq!(schema.facts.fields.len(), 2);
}

/// The stage that applies the field schema may host the kinds its regions have, and the stage that
/// generates an id the `DEFAULT` of the `id` field, in an order a verifier of kinds in sequence
/// accepts.
#[test]
fn the_regions_of_the_schema_stages_follow_the_order_of_their_tails() {
	use super::ops::SchemaRegions;
	use super::verify::stage_tail;

	let kinds = SchemaRegions {
		default: 2,
		value: 1,
		assert: 3,
		permission: 2,
		derive: 1,
		predicate_derive: 4,
	}
	.kinds();
	assert_eq!(kinds.len(), 13);
	let tail = stage_tail(WriteStageKind::ProcessTableFields);
	let mut next = 0;
	for kind in &kinds {
		next += tail[next..]
			.iter()
			.position(|held| held == kind)
			.unwrap_or_else(|| panic!("{kind:?} is out of the tail's order"));
	}
	assert_eq!(
		stage_tail(WriteStageKind::GenerateRecordId),
		[RegionKind::FieldFrame(FieldRole::Default)]
	);
}

// ---------------------------------------------------------------------------------------------
// Fallibility, effects and structure
// ---------------------------------------------------------------------------------------------

/// Each tag answers the class the interface declares for it, a typed `LET` answers `VALUE`, and
/// the effects answer carries the same class.
#[test]
fn fallibility_per_tag() {
	let env = Env::new();
	let cx = env.cx();
	for tag in LogTag::ALL {
		let op = fixture(*tag);
		assert_eq!(op.fallibility(&cx), declared_class(*tag), "{}", tag.mnemonic());
		assert_eq!(
			op.local_effects(&cx).fallibility,
			op.fallibility(&cx),
			"{}: effects and fallibility disagree",
			tag.mnemonic()
		);
	}
	let typed = LogOp::Let(Box::new(LetAttrs {
		ty: Some(Kind::Int),
		..untyped_let("x")
	}));
	assert_eq!(typed.fallibility(&cx), Fallibility::VALUE);
	let session = LogOp::Let(Box::new(LetAttrs {
		binding: LetBinding::Session,
		..untyped_let("x")
	}));
	assert_eq!(session.fallibility(&cx), Fallibility::INFALLIBLE);
}

/// Exactly the four control-transfer ops are terminators, and each names its own kind with a
/// target that is derived from nesting.
#[test]
fn terminators_are_the_control_transfers() {
	for tag in LogTag::ALL {
		let op = fixture(*tag);
		let expected = match tag {
			LogTag::Yield | LogTag::Next | LogTag::Break | LogTag::Return => true,
			LogTag::Request
			| LogTag::Txn
			| LogTag::Stmt
			| LogTag::Statement
			| LogTag::Source
			| LogTag::Values
			| LogTag::Match
			| LogTag::MatchRead
			| LogTag::MatchUpdate
			| LogTag::MatchDelete
			| LogTag::MatchInsert
			| LogTag::MatchOutput
			| LogTag::BindStep
			| LogTag::Subquery
			| LogTag::Write
			| LogTag::Assign
			| LogTag::Unset
			| LogTag::Content
			| LogTag::DdlGuard
			| LogTag::Define
			| LogTag::Remove
			| LogTag::Alter
			| LogTag::Rebuild
			| LogTag::UnionAll
			| LogTag::Join
			| LogTag::Filter
			| LogTag::PermissionFilter
			| LogTag::Split
			| LogTag::Aggregate
			| LogTag::Distinct
			| LogTag::Sort
			| LogTag::Window
			| LogTag::Limit
			| LogTag::Project
			| LogTag::Fetch
			| LogTag::Timeout
			| LogTag::VersionScope
			| LogTag::Only
			| LogTag::Explain
			| LogTag::Eval
			| LogTag::Block
			| LogTag::Let
			| LogTag::Loop
			| LogTag::If
			| LogTag::Info
			| LogTag::Sleep
			| LogTag::Use
			| LogTag::Option
			| LogTag::Access
			| LogTag::Kill
			| LogTag::Live
			| LogTag::Subscriber
			| LogTag::Show
			| LogTag::Choose
			| LogTag::WriteStage
			| LogTag::Events => false,
		};
		assert_eq!(op.is_terminator(), expected, "{}", tag.mnemonic());
		assert_eq!(op.terminator().is_some(), expected, "{}", tag.mnemonic());
	}
	assert!(matches!(LogOp::Yield.terminator(), Some(Terminator::Yield)));
	assert!(matches!(LogOp::Return.terminator(), Some(Terminator::Return { .. })));
	assert!(matches!(LogOp::Break.terminator(), Some(Terminator::Break { .. })));
	assert!(matches!(LogOp::Next.terminator(), Some(Terminator::Next { .. })));
}

// ---------------------------------------------------------------------------------------------
// Digest
// ---------------------------------------------------------------------------------------------

/// An op as a container holds it, at a statement position and with a provenance.
fn op_at(kind: LogOp, stmt: u32, origin: LocOrigin) -> OpData {
	OpData {
		kind: OpKind::Logical(kind),
		operands: SmallVec::new(),
		results: SmallVec::new(),
		regions: SmallVec::new(),
		frame: FrameId::new(0),
		snapshot: SnapshotId::new(0),
		loc: Location {
			stmt,
			span: Some(Span {
				start: stmt,
				end: stmt + 9,
			}),
			origin,
		},
		provenance: Provenance::Bound,
		parent: BlockId::default(),
	}
}

/// Where an op stands in the request text is not identity: two ops that differ only in their
/// location digest alike. An attribute is identity: ops that differ in one digest apart. An opaque
/// op digests to the opaque value, and no other op does.
#[test]
fn attr_digest_ignores_location() {
	for tag in LogTag::ALL {
		let first = op_at(fixture(*tag), 0, LocOrigin::Query);
		let second = op_at(fixture(*tag), 7, LocOrigin::Synthesized);
		assert_ne!(first.loc, second.loc);
		let (a, b) = (first.kind.attr_digest(), second.kind.attr_digest());
		assert_eq!(a, b, "{}: the digest reads the location", tag.mnemonic());
		if is_opaque(*tag) {
			assert_eq!(a, AttrDigest::OPAQUE, "{}", tag.mnemonic());
		} else {
			assert_ne!(a, AttrDigest::OPAQUE, "{}", tag.mnemonic());
		}
	}

	// Every tag with no attribute is told apart by its mnemonic.
	let bare = [LogOp::Filter, LogOp::Distinct, LogOp::Fetch, LogOp::Eval, LogOp::Block];
	for (at, left) in bare.iter().enumerate() {
		for right in &bare[at + 1..] {
			assert_ne!(left.attr_digest(), right.attr_digest(), "{:?} and {right:?}", left);
		}
	}

	// One attribute changed.
	let pairs = [
		(
			LogOp::Option {
				name: strand("IMPORT"),
				value: true,
			},
			LogOp::Option {
				name: strand("IMPORT"),
				value: false,
			},
		),
		(
			LogOp::Option {
				name: strand("IMPORT"),
				value: true,
			},
			LogOp::Option {
				name: strand("FORCE"),
				value: true,
			},
		),
		(
			LogOp::Limit {
				limit: Some(0),
				start: None,
			},
			LogOp::Limit {
				limit: Some(0),
				start: Some(1),
			},
		),
		(
			LogOp::Content {
				mode: ContentMode::Content,
			},
			LogOp::Content {
				mode: ContentMode::Merge,
			},
		),
		(LogOp::Let(Box::new(untyped_let("x"))), LogOp::Let(Box::new(untyped_let("y")))),
		(
			LogOp::Let(Box::new(untyped_let("x"))),
			LogOp::Let(Box::new(LetAttrs {
				ty: Some(Kind::Int),
				..untyped_let("x")
			})),
		),
		(
			LogOp::Let(Box::new(LetAttrs {
				ty: Some(Kind::Int),
				..untyped_let("x")
			})),
			LogOp::Let(Box::new(LetAttrs {
				ty: Some(Kind::String),
				..untyped_let("x")
			})),
		),
		(
			LogOp::DdlGuard {
				guard: DdlGuard::RefuseNoWrite,
			},
			LogOp::DdlGuard {
				guard: DdlGuard::Allow {
					action: crate::iam::Action::Edit,
					resource: crate::iam::ResourceKind::Any,
					base: crate::expr::Base::Root,
				},
			},
		),
		(
			LogOp::Use {
				target: UseTarget::Ns,
			},
			LogOp::Use {
				target: UseTarget::Db,
			},
		),
	];
	for (left, right) in &pairs {
		assert_ne!(left.attr_digest(), right.attr_digest(), "{left:?} and {right:?}");
		assert!(!left.attrs_eq(right), "{left:?} has the attributes of {right:?}");
		assert!(left.attrs_eq(left), "{left:?} differs from itself");
	}
}

/// Attributes are equal exactly when every digested attribute is: the literal kinds `1`, `1f`
/// and `1dec` are three sets of attributes, and an opaque op equals no op.
#[test]
fn attrs_eq_tells_number_kinds_apart() {
	use rust_decimal::Decimal;

	use crate::expr::KindLiteral;
	let typed = |literal: KindLiteral| {
		LogOp::Let(Box::new(LetAttrs {
			ty: Some(Kind::Literal(literal)),
			..untyped_let("x")
		}))
	};
	let kinds = [
		typed(KindLiteral::Integer(1)),
		typed(KindLiteral::Float(1.0)),
		typed(KindLiteral::Decimal(Decimal::ONE)),
	];
	for (at, left) in kinds.iter().enumerate() {
		for right in &kinds[at + 1..] {
			assert!(!left.attrs_eq(right), "{left:?} has the attributes of {right:?}");
		}
	}
	for tag in LogTag::ALL {
		let equal = fixture(*tag).attrs_eq(&fixture(*tag));
		assert_eq!(equal, !is_opaque(*tag), "{}", tag.mnemonic());
	}
}

// ---------------------------------------------------------------------------------------------
// Text form
// ---------------------------------------------------------------------------------------------

/// The attribute text of `kind`.
fn printed(kind: &OpKind) -> String {
	let mut out = String::new();
	LogicalSyntax.print_attrs(kind, &mut out);
	out
}

/// The text of the attributes a loop states beyond its kind, sink and order.
#[test]
fn the_step_of_a_loop_prints_as_documented() {
	let loop_op = |kind, order, step| {
		printed(&OpKind::Logical(LogOp::Loop {
			kind,
			sink: LoopSink::Output,
			order,
			step,
		}))
	};
	assert_eq!(
		loop_op(LoopKind::Select, SourceOrder::Delivery, None),
		"kind = Select, sink = Output, order = Delivery, step = none"
	);
	assert_eq!(
		loop_op(LoopKind::Broadcast, SourceOrder::Positional, Some(Box::new(StepAttrs::Flatten))),
		"kind = Broadcast, sink = Output, order = Positional, step = some(Flatten)"
	);
	let lookup = LookupAttrs {
		kind: LookupKind::Graph(Dir::Out),
		only: false,
		only_path: false,
		subjects: Box::new([LookupSubjectShape {
			table: table("knows"),
			referencing_field: None,
			range: None,
		}]),
		fields: None,
		order: None,
		alias: None,
		cond: false,
		split: None,
		group: None,
		limit: None,
		start: None,
		trace: crate::expr::logical::LookupTrace::None,
	};
	assert_eq!(
		loop_op(
			LoopKind::GraphStep,
			SourceOrder::Delivery,
			Some(Box::new(StepAttrs::Lookup(lookup)))
		),
		"kind = GraphStep, sink = Output, order = Delivery, step = some(Lookup({ kind = Graph(Out), only = false, only_path = false, subjects = [{ table = \"knows\", referencing_field = none, range = none }], fields = none, order = none, alias = none, cond = false, split = none, group = none, limit = none, start = none, trace = None }))"
	);
	let recurse = RecurseAttrs {
		depth: Recurse::Range(Some(1), None),
		form: RecursePathForm::Nested,
		instruction: Some(RecurseInstructionShape::Collect {
			inclusive: false,
		}),
	};
	assert_eq!(
		loop_op(
			LoopKind::Recurse,
			SourceOrder::Positional,
			Some(Box::new(StepAttrs::Recurse(recurse)))
		),
		"kind = Recurse, sink = Output, order = Positional, step = some(Recurse({ depth = Range(some(1), none), form = Nested, instruction = some(Collect { inclusive = false }) }))"
	);
}

/// A lookup that states every attribute: two subjects, one of them over a key range, a projection,
/// an order, an alias, a predicate, splits, group keys, `LIMIT` and `START`.
fn rich_lookup() -> LookupAttrs {
	LookupAttrs {
		kind: LookupKind::Graph(Dir::Both),
		only: true,
		only_path: false,
		subjects: Box::new([
			LookupSubjectShape {
				table: table("we\"ird"),
				referencing_field: None,
				range: None,
			},
			LookupSubjectShape {
				table: table("knows"),
				referencing_field: Some("a\nb".to_owned()),
				range: Some(KeyRange {
					start: Bound::Included(KeyBound::Number(1)),
					end: Bound::Excluded(KeyBound::Array {
						len: 2,
					}),
				}),
			},
		]),
		fields: Some(FieldsShape::Select(Box::new([
			FieldShape::All,
			FieldShape::Single {
				alias: None,
				kind: Some(Kind::Int),
			},
		]))),
		order: Some(SortShape::Keys(Box::new([SortKeyShape {
			collate: true,
			numeric: false,
			ascending: false,
			path: false,
		}]))),
		alias: Some(OutputPath::field("friends")),
		cond: true,
		split: Some(1),
		group: Some(0),
		limit: Some(3),
		start: Some(4),
		trace: crate::expr::logical::LookupTrace::None,
	}
}

/// Ops that carry the attribute types the one-per-tag fixtures leave out: options, sequences,
/// nested payloads, escapes, templates and guarded arms.
fn rich_fixtures() -> Vec<LogOp> {
	let no_permissions = PermissionsShape {
		select: PermissionShape::None,
		create: PermissionShape::Full,
		update: PermissionShape::Specific(2),
		delete: PermissionShape::Full,
	};
	vec![
		LogOp::Source(Box::new(SourceAttrs {
			source_ref: SourceRefShape::Table(table("we\"ird\nname")),
			with: Some(With::Index(vec!["a".to_owned(), "b\\c".to_owned()])),
			for_update: true,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		})),
		LogOp::Write(Box::new(WriteAttrs {
			form: WriteForm::Upsert,
			only: true,
			output: OutputShape::Fields,
			with: Some(With::NoIndex),
			explain: Some(Explain(true)),
			flags: WriteFlags {
				ignore: true,
				relation: true,
				or_update: true,
				where_never: true,
			},
			body: crate::exec::ir::logical::ops::WriteBody::default(),
			gates: crate::exec::ir::logical::ops::WriteGates::default(),
			image: crate::exec::ir::logical::ops::WriteImage::default(),
			stored: Default::default(),
			derived: Default::default(),
			into_text: None,
			statement: None,
			linked: None,
		})),
		LogOp::Let(Box::new(LetAttrs {
			binding: LetBinding::Session,
			ty: Some(Kind::Int),
			..untyped_let("x")
		})),
		LogOp::Loop {
			kind: LoopKind::GraphStep,
			sink: LoopSink::Output,
			order: SourceOrder::Delivery,
			step: Some(Box::new(StepAttrs::Lookup(rich_lookup()))),
		},
		LogOp::Loop {
			kind: LoopKind::GraphStep,
			sink: LoopSink::Output,
			order: SourceOrder::Delivery,
			step: Some(Box::new(StepAttrs::Lookup(LookupAttrs {
				kind: LookupKind::Reference,
				only: false,
				only_path: false,
				subjects: Box::new([LookupSubjectShape {
					table: table("comment"),
					referencing_field: Some("author".to_owned()),
					range: None,
				}]),
				fields: None,
				order: None,
				alias: None,
				cond: false,
				split: None,
				group: None,
				limit: None,
				start: None,
				trace: crate::expr::logical::LookupTrace::None,
			}))),
		},
		LogOp::Loop {
			kind: LoopKind::Recurse,
			sink: LoopSink::Output,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Recurse(RecurseAttrs {
				depth: Recurse::Range(Some(1), None),
				form: RecursePathForm::Trailing,
				instruction: Some(RecurseInstructionShape::Shortest {
					inclusive: true,
				}),
			}))),
		},
		LogOp::Loop {
			kind: LoopKind::Recurse,
			sink: LoopSink::Output,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Recurse(RecurseAttrs {
				depth: Recurse::Fixed(2),
				form: RecursePathForm::Absent,
				instruction: None,
			}))),
		},
		LogOp::Loop {
			kind: LoopKind::Broadcast,
			sink: LoopSink::Output,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Flatten)),
		},
		LogOp::Limit {
			limit: Some(0),
			start: Some(1),
		},
		LogOp::Txn {
			mode: TxnRegionMode::Session {
				id: 3,
			},
		},
		LogOp::Info {
			subject: InfoSubject::User {
				base: Some(crate::expr::Base::Db),
			},
			structure: true,
		},
		LogOp::Option {
			name: strand("a name with spaces and a \u{1} control"),
			value: false,
		},
		LogOp::Assign(Box::new(AssignAttrs {
			place: Place {
				parts: Box::new([
					PlacePart::Field(strand("a")),
					PlacePart::Dynamic(1),
					PlacePart::Index(2),
					PlacePart::Unaddressed(UnaddressedPart::Method, "push(1)".into()),
					PlacePart::Last,
				]),
			},
			op: AssignOperator::Extend,
		})),
		LogOp::Access(Box::new(AccessAttrs {
			ac: strand("a"),
			base: Some(crate::expr::Base::Ns),
			op: AccessOp::Purge {
				kind: crate::expr::statements::access::PurgeKind::Both,
				grace: std::time::Duration::new(5, 7),
			},
		})),
		LogOp::Show(Box::new(ShowAttrs {
			table: Some(table("t")),
			since: ShowSince::Versionstamp(9),
			limit: Some(5),
		})),
		LogOp::Sort(Box::new(SortAttrs {
			ordering: SortShape::Keys(Box::new([
				SortKeyShape {
					collate: true,
					numeric: false,
					ascending: true,
					path: false,
				},
				SortKeyShape {
					collate: false,
					numeric: true,
					ascending: false,
					path: false,
				},
			])),
			tempfiles: true,
		})),
		LogOp::Loop {
			kind: LoopKind::Write,
			sink: LoopSink::Write(Box::new(WriteStages(Box::new([
				WriteStageKind::CheckRecordExists,
				WriteStageKind::ProcessTableViews(Action::Update),
			])))),
			order: SourceOrder::Delivery,
			step: None,
		},
		LogOp::Project(Box::new(ProjectAttrs {
			fields: FieldsShape::Select(Box::new([
				FieldShape::All,
				FieldShape::Single {
					alias: Some(OutputPath::field("n")),
					kind: Some(Kind::String),
				},
			])),
			layer: true,
			omit: 2,
			names: Vec::new(),
			computed: vec![strand("c")],
			sort_keys: Vec::new(),
			judged: None,
		})),
		LogOp::Choose(Box::new(ChooseSpec {
			granularity: ChooseGranularity::Morsel,
			arms: smallvec![
				ArmSpec {
					guard: Guard::TableIs(table("t")),
					body: ArmBody::UnitLoad(UnitKind::TableRead),
					label: ArmLabel("specialised"),
				},
				ArmSpec {
					guard: Guard::SizeAtMost {
						bucket: SizeBucket::new(3),
					},
					body: ArmBody::PreLowered,
					label: ArmLabel("small"),
				},
				ArmSpec {
					guard: Guard::TypeHolds(Kind::Int),
					body: ArmBody::PreLowered,
					label: ArmLabel("int"),
				},
			],
			generic: GenericArm::ExhaustiveBy(ExhaustiveFact::TypeFact(Kind::Int)),
			equivalence: ArmEquivalence::OrderedByTotalKey,
			origin: ChooseOrigin::LogicalKindDispatch,
		})),
		LogOp::WriteStage {
			stage: WriteStageKind::CheckTableType(TableTypeFor::Relate),
			mark: WriteMark::Apply,
		},
		LogOp::Define(Box::new(DefineAttrs {
			object: DefineObject::Table(Box::new(DefineTable {
				id: Some(7),
				name: 0,
				drop: false,
				full: true,
				view: None,
				permissions: no_permissions,
				changefeed: Some(ChangeFeed {
					expiry: crate::val::BoundedDuration(std::time::Duration::from_secs(60)),
					store_diff: true,
				}),
				comment: 1,
				table_type: TableType::Relation(Relation {
					from: vec![table("a"), table("b")],
					to: Vec::new(),
					enforced: true,
					lightweight: false,
				}),
				graphql_alias: Some("al\"ias".to_owned()),
				graphql_deprecated: None,
				inline_edges_cap: Some(4),
				inline_refs_cap: None,
			})),
			mode: DefineKind::Overwrite,
			template: DefinitionTemplate {
				ty: "StoredTableDefinition",
				value: Value::Bool(true),
				holes: smallvec![TemplateHole::Name, TemplateHole::Other("view_query")],
			},
			checks: DdlChecks::None,
		})),
	]
}

/// Printing, parsing and printing again yields the text printed first, for the fixture of every
/// tag and for ops that carry the attribute types the fixtures leave out. The parse yields an op
/// of the tag that was printed.
#[test]
fn logical_round_trip_per_tag() {
	let mut ops: Vec<LogOp> = LogTag::ALL.iter().map(|tag| fixture(*tag)).collect();
	ops.extend(rich_fixtures());
	for op in ops {
		let kind = OpKind::Logical(op);
		let tag = kind.tag();
		let text = printed(&kind);
		let parsed = LogicalSyntax
			.parse_attrs(tag, &text)
			.unwrap_or_else(|e| panic!("{}: `{text}` does not parse: {e}", tag.mnemonic()));
		assert_eq!(parsed.tag(), tag, "{}", tag.mnemonic());
		assert_eq!(printed(&parsed), text, "{}: the second print differs", tag.mnemonic());
	}
}

/// An op without attributes prints nothing, and every other tag's fixture prints something.
#[test]
fn attribute_free_ops_print_nothing() {
	for tag in LogTag::ALL {
		let text = printed(&OpKind::Logical(fixture(*tag)));
		let bare =
			matches!(
				tag,
				LogTag::Subquery
					| LogTag::UnionAll
					| LogTag::Filter
					| LogTag::Distinct
					| LogTag::Fetch | LogTag::Timeout
					| LogTag::VersionScope
					| LogTag::Only | LogTag::Eval
					| LogTag::Block | LogTag::Return
					| LogTag::If | LogTag::Sleep
					| LogTag::Kill | LogTag::Yield
					| LogTag::Next | LogTag::Break
			);
		assert_eq!(text.is_empty(), bare, "{}: `{text}`", tag.mnemonic());
	}
}

/// Text that does not match a tag's attribute list exactly answers an error and never an op with
/// defaults: text for an op without attributes, a missing, reordered, extra or unknown key, a
/// malformed value, a trailing remainder, a tag of another dialect, and attributes the text does
/// not carry (a user's credentials, a `DdlChecks` other than `none`).
#[test]
fn logical_attrs_reject_unreadable_text() {
	let parse = |tag: LogTag, text: &str| LogicalSyntax.parse_attrs(OpTag::Logical(tag), text);
	assert!(parse(LogTag::Filter, "").is_ok());
	assert!(parse(LogTag::Filter, "x = 1").is_err());

	let values = "for_update = false, written = true";
	assert!(parse(LogTag::Values, values).is_ok());
	assert!(parse(LogTag::Values, "").is_err());
	assert!(parse(LogTag::Values, "for_update = false").is_err());
	assert!(parse(LogTag::Values, "written = true, for_update = false").is_err());
	assert!(parse(LogTag::Values, "for_update = false, written = true, more = 1").is_err());
	assert!(parse(LogTag::Values, "for_update = false, written = maybe").is_err());
	assert!(parse(LogTag::Values, "for_update = false, written = true,").is_err());
	assert!(parse(LogTag::Values, &format!("{values} trailing")).is_err());

	assert!(parse(LogTag::Use, "target = Ns").is_ok());
	assert!(parse(LogTag::Use, "target = Elsewhere").is_err());
	assert!(parse(LogTag::Option, "name = \"unterminated, value = true").is_err());
	assert!(parse(LogTag::Limit, "limit = some(-1), start = none").is_err());
	assert!(parse(LogTag::Limit, "limit = some(7), start = none").is_ok());

	let other = OpTag::ALL.iter().find(|tag| !matches!(tag, OpTag::Logical(_)));
	let other = other.expect("a tag of another dialect exists");
	assert!(LogicalSyntax.parse_attrs(*other, "").is_err());

	// Credentials are never in the text, so a statement that holds them is not readable.
	let user = LogOp::Alter(Box::new(AlterAttrs {
		object: AlterObject::User(Box::new(crate::exec::ir::iface::statements::AlterUser {
			name: 0,
			base: crate::expr::Base::Root,
			password: None,
			hash: Some("secret-hash".to_owned()),
			scram: None,
			roles: crate::expr::statements::alter::AlterKind::None,
			token_duration: crate::expr::statements::alter::AlterKind::None,
			session_duration: crate::expr::statements::alter::AlterKind::None,
			comment: crate::expr::statements::alter::AlterKind::None,
		})),
		if_exists: false,
		checks: DdlChecks::None,
		template: None,
	}));
	let text = printed(&OpKind::Logical(user));
	assert!(!text.contains("secret-hash"), "a credential is printed: {text}");
	assert!(parse(LogTag::Alter, &text).is_err());

	// A `DdlChecks` other than `none` is not readable either.
	let define = LogOp::Define(Box::new(DefineAttrs {
		object: DefineObject::Param {
			name: strand("p"),
			value: 0,
			comment: 1,
			permissions: PermissionShape::Full,
		},
		mode: DefineKind::Default,
		template: template("StoredParamDefinition"),
		checks: DdlChecks::None,
	}));
	let text = printed(&OpKind::Logical(define));
	assert!(parse(LogTag::Define, &text).is_ok());
	assert!(parse(LogTag::Define, &text.replace("checks = none", "checks = unsupported")).is_err());

	// An alteration's template is readable: the fields it replaces print with it.
	let alter = LogOp::Alter(Box::new(AlterAttrs {
		object: AlterObject::Param {
			name: strand("p"),
			value: None,
			comment: crate::expr::statements::alter::AlterKind::Set("c".to_owned()),
			permissions: None,
		},
		if_exists: false,
		checks: DdlChecks::None,
		template: Some(crate::exec::ir::iface::statements::AlterTemplate {
			ty: "StoredParamDefinition",
			value: Value::None,
			fields: Box::new([strand("comment")]),
		}),
	}));
	let text = printed(&OpKind::Logical(alter));
	assert!(text.contains("fields = [\"comment\"]"), "{text}");
	assert!(parse(LogTag::Alter, &text).is_ok());
	assert!(parse(LogTag::Alter, &text.replace("fields = [", "fields = (")).is_err());
}

/// The built-in syntax table holds the logical syntax in the slot of the logical dialect.
#[test]
fn attr_syntaxes_hold_the_logical_syntax() {
	let syntax = crate::exec::ir::registry::attr_syntaxes().0[1]
		.expect("the table holds the logical attribute syntax");
	let kind = OpKind::Logical(fixture(LogTag::Option));
	let mut text = String::new();
	syntax.print_attrs(&kind, &mut text);
	assert_eq!(text, printed(&kind));
}

// ---------------------------------------------------------------------------------------------
// Region-stack walks
// ---------------------------------------------------------------------------------------------

const AUTO: RegionKind = RegionKind::Statement(StatementCommit::Auto);
const IN_TXN: RegionKind = RegionKind::Statement(StatementCommit::InTxn);
const EXPLICIT: RegionKind = RegionKind::Transaction(TxnKind::Explicit);
const SHAPE_TO_NONE: RegionKind = RegionKind::ErrorScope(ErrorScope::ShapeToNone);
const WHERE: RegionKind = RegionKind::Clause(ClauseRole::Where);

/// A `RETURN` ends the innermost context around it: a statement of the request or of a
/// transaction, a body, stored code, or a clause a statement evaluates per row or per value. Every
/// block, arm, `FOR` body, parenthesised statement, operand and statement of a list between passes
/// it; a stack that reaches the request or a transaction first, or holds nothing that catches, has
/// no target.
#[test]
fn return_target_walk() {
	use RegionKind::{
		Block, CatalogUnit, ClosureBody, Deadline, FunctionBody, LazyOperand, Loop,
		PermissionFrame, Request, StatementFor, StatementIf, Subquery, ValueBlock, ValueFor,
		ValueIf, WriteStage,
	};

	// A block, an arm, a FOR body and a parenthesised statement pass a RETURN to the statement.
	assert_eq!(return_target(&[Request, AUTO, ValueIf]), Some(1), "LET $d = IF c {{ RETURN 1 }}");
	assert_eq!(
		return_target(&[Request, AUTO, ValueBlock, StatementIf]),
		Some(1),
		"LET $w = {{ IF c {{ RETURN 5 }}; 6 }}"
	);
	assert_eq!(return_target(&[Request, AUTO, StatementIf]), Some(1), "IF c {{ RETURN 5 }}");
	assert_eq!(return_target(&[Request, AUTO]), Some(1), "RETURN 6");
	assert_eq!(return_target(&[Request, AUTO, Subquery]), Some(1), "RETURN 1 + (RETURN 2)");
	assert_eq!(return_target(&[Request, AUTO, Block, ValueBlock]), Some(1));
	assert_eq!(return_target(&[Request, EXPLICIT, IN_TXN]), Some(2), "BEGIN; RETURN 6");
	assert_eq!(
		return_target(&[Request, EXPLICIT, IN_TXN, StatementIf]),
		Some(2),
		"BEGIN; IF c {{ RETURN 5 }}"
	);
	assert_eq!(return_target(&[Request, AUTO, StatementFor]), Some(1));
	assert_eq!(return_target(&[Request, AUTO, ValueFor]), Some(1));
	assert_eq!(return_target(&[Request, AUTO, ValueFor, StatementIf]), Some(1));

	// A statement of a statement list is no context: the body around it is.
	assert_eq!(return_target(&[Request, AUTO, FunctionBody, IN_TXN, Subquery]), Some(2));
	assert_eq!(return_target(&[Request, AUTO, ValueBlock, IN_TXN, Subquery]), Some(1));

	// A clause evaluated per row or per value is a context; one evaluated once is not.
	assert_eq!(return_target(&[Request, AUTO, Loop, WHERE]), Some(3), "WHERE (RETURN true)");
	assert_eq!(return_target(&[Request, AUTO, Loop, WHERE, LazyOperand, Subquery]), Some(3));
	assert_eq!(return_target(&[Request, AUTO, Loop, WHERE, ValueBlock, ValueIf]), Some(3));
	for role in [
		ClauseRole::WriteCond,
		ClauseRole::Projection,
		ClauseRole::SetValue,
		ClauseRole::Content,
		ClauseRole::DuplicateUpdate,
		ClauseRole::ReturnClause,
		ClauseRole::OrderKey,
		ClauseRole::GroupKey,
		ClauseRole::Split,
	] {
		assert_eq!(
			return_target(&[Request, AUTO, Loop, RegionKind::Clause(role), Subquery]),
			Some(3),
			"{role:?}"
		);
	}
	for role in [
		ClauseRole::From,
		ClauseRole::Limit,
		ClauseRole::Start,
		ClauseRole::Timeout,
		ClauseRole::InsertValues,
	] {
		assert_eq!(
			return_target(&[Request, AUTO, RegionKind::Clause(role), Subquery]),
			Some(1),
			"{role:?}"
		);
	}

	// A lazy operand, an error scope and a deadline pass a RETURN.
	assert_eq!(return_target(&[Request, AUTO, ValueBlock, LazyOperand]), Some(1));
	assert_eq!(return_target(&[Request, AUTO, SHAPE_TO_NONE]), Some(1));
	assert_eq!(
		return_target(&[Request, AUTO, RegionKind::ErrorScope(ErrorScope::MethodBody)]),
		Some(1)
	);
	assert_eq!(
		return_target(&[Request, AUTO, RegionKind::ErrorScope(ErrorScope::PermissionCheck)]),
		Some(1)
	);
	assert_eq!(return_target(&[Request, AUTO, Deadline, LazyOperand]), Some(1));

	// Every body and every frame of stored code catches.
	for scope in [
		FunctionBody,
		ClosureBody,
		PermissionFrame,
		RegionKind::FieldFrame(FieldRole::Computed),
		RegionKind::EventFrame,
		RegionKind::SubscriberFrame,
		WriteStage,
		RegionKind::StoredClause(StoredRole::Body),
	] {
		assert_eq!(
			return_target(&[Request, AUTO, scope, StatementIf, ValueBlock]),
			Some(2),
			"{scope:?}"
		);
	}
	assert_eq!(return_target(&[Request, AUTO, FunctionBody, StatementFor, Loop]), Some(2));

	// The request, a transaction and a catalog unit are never reached, and an empty stack has no
	// target.
	assert_eq!(return_target(&[]), None);
	assert_eq!(return_target(&[Request]), None);
	assert_eq!(return_target(&[Request, EXPLICIT]), None);
	assert_eq!(return_target(&[Request, EXPLICIT, Block]), None);
	assert_eq!(
		return_target(&[Request, RegionKind::Transaction(TxnKind::Session), StatementIf]),
		None
	);
	assert_eq!(return_target(&[CatalogUnit]), None);
	assert_eq!(return_target(&[CatalogUnit, Subquery]), None);
	// The top of a unit that passes a RETURN is where it lands: the unit's run takes it.
	assert_eq!(return_target(&[Loop, ValueBlock]), Some(0));
	assert_eq!(return_target(&[Subquery, ValueIf, LazyOperand]), Some(0));
	assert_eq!(return_target(&[Request, EXPLICIT, StatementIf, CatalogUnit, StatementIf]), None);
}

/// A `BREAK` or `CONTINUE` goes to the `FOR` written around it, through every region that passes
/// it; stored code, a function or closure body, and the statement are barriers.
#[test]
fn loop_target_walk() {
	use RegionKind::{
		Block, CatalogUnit, ClosureBody, EventFrame, FunctionBody, LazyOperand, Loop,
		PermissionFrame, Request, StatementFor, StatementIf, SubscriberFrame, ValueBlock, ValueFor,
		ValueIf, WriteStage,
	};

	// `FOR $i IN [1, 2] { BREAK }`.
	assert_eq!(loop_exit(&[Request, AUTO, StatementFor]), LoopExit::Target(2));
	// `FOR $i IN [1, 2] { IF c { BREAK } }`: a statement branch passes.
	assert_eq!(loop_exit(&[Request, AUTO, StatementFor, StatementIf]), LoopExit::Target(2));
	// `FOR … { SELECT * FROM t WHERE (BREAK) }`: a clause and the select loop pass.
	assert_eq!(loop_exit(&[Request, AUTO, StatementFor, Loop, WHERE]), LoopExit::Target(2));
	// `FOR $i IN [1, 2] { UPDATE t SET a = (BREAK) }`.
	assert_eq!(
		loop_exit(&[Request, AUTO, StatementFor, Loop, RegionKind::Clause(ClauseRole::SetValue)]),
		LoopExit::Target(2)
	);
	// A BREAK in a lazy operand or a value branch of the loop body.
	assert_eq!(loop_exit(&[Request, AUTO, StatementFor, LazyOperand]), LoopExit::Target(2));
	assert_eq!(
		loop_exit(&[Request, AUTO, StatementFor, ValueIf, LazyOperand, SHAPE_TO_NONE]),
		LoopExit::Target(2)
	);
	assert_eq!(loop_exit(&[Request, AUTO, StatementFor, Block, ValueBlock]), LoopExit::Target(2));
	// A FOR standing as a value is a target too, and the innermost FOR wins.
	assert_eq!(loop_exit(&[Request, AUTO, ValueFor]), LoopExit::Target(2));
	assert_eq!(
		loop_exit(&[Request, AUTO, StatementFor, StatementIf, ValueFor]),
		LoopExit::Target(4)
	);
	// The loop inside a function body is the target of a BREAK written in it.
	assert_eq!(loop_exit(&[Request, AUTO, FunctionBody, StatementFor]), LoopExit::Target(3));
	// Inside a transaction.
	assert_eq!(loop_exit(&[Request, EXPLICIT, IN_TXN, StatementFor]), LoopExit::Target(3));

	// Each barrier ends the walk, whatever FOR stands outside it.
	for barrier in [
		FunctionBody,
		ClosureBody,
		PermissionFrame,
		RegionKind::FieldFrame(FieldRole::Value),
		RegionKind::FieldFrame(FieldRole::Default),
		RegionKind::FieldFrame(FieldRole::Assert),
		RegionKind::FieldFrame(FieldRole::Computed),
		EventFrame,
		SubscriberFrame,
		WriteStage,
		CatalogUnit,
		RegionKind::StoredClause(StoredRole::IndexColumn),
		RegionKind::StoredClause(StoredRole::Body),
	] {
		assert_eq!(
			loop_exit(&[Request, AUTO, StatementFor, barrier]),
			LoopExit::Barrier,
			"{barrier:?}"
		);
		assert_eq!(
			loop_exit(&[Request, AUTO, StatementFor, barrier, StatementIf, WHERE]),
			LoopExit::Barrier,
			"{barrier:?}"
		);
	}
	// `FOR $i IN [1] { [1].map(|$x| { BREAK }) }`.
	assert_eq!(
		loop_exit(&[Request, AUTO, StatementFor, Loop, ClosureBody, Block]),
		LoopExit::Barrier
	);

	// No FOR around the BREAK: the statement ends the walk.
	assert_eq!(loop_exit(&[Request, AUTO]), LoopExit::Barrier);
	assert_eq!(loop_exit(&[Request, AUTO, StatementIf]), LoopExit::Barrier);
	assert_eq!(loop_exit(&[Request, AUTO, Loop, WHERE]), LoopExit::Barrier);
	assert_eq!(loop_exit(&[Request, AUTO, ValueBlock, LazyOperand]), LoopExit::Barrier);
	assert_eq!(loop_exit(&[Request, EXPLICIT, IN_TXN]), LoopExit::Barrier);
	// The statement of the request ends the walk. A statement of a block stands in a statement
	// list, so a `BREAK` inside it reaches the `FOR` that holds the list.
	assert_eq!(loop_exit(&[Request, AUTO, IN_TXN]), LoopExit::Barrier);
	assert_eq!(loop_exit(&[Request, AUTO, StatementFor, IN_TXN]), LoopExit::Target(2));
	assert_eq!(loop_exit(&[Request, AUTO, StatementFor, IN_TXN, Loop, WHERE]), LoopExit::Target(2));
	assert_eq!(loop_exit(&[Request, AUTO, Block, IN_TXN]), LoopExit::Barrier);
	// The request and a transaction are barriers; so is an empty stack.
	assert_eq!(loop_exit(&[Request]), LoopExit::Barrier);
	assert_eq!(loop_exit(&[Request, EXPLICIT]), LoopExit::Barrier);
	assert_eq!(loop_exit(&[]), LoopExit::Barrier);
}

// ---------------------------------------------------------------------------------------------
// The logical verifier
// ---------------------------------------------------------------------------------------------

fn synthesized() -> Location {
	Location {
		stmt: 0,
		span: None,
		origin: LocOrigin::Synthesized,
	}
}

/// Creates `kind` at `at` with `results` untyped results.
fn new_op(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	kind: OpKind,
	operands: &[ValueId],
	results: usize,
) -> OpId {
	let spec = OpSpec {
		kind,
		operands: operands.iter().copied().collect(),
		results: (0..results).map(|_| TypeKnowledge::Unknown).collect(),
		frame: FrameId::new(0),
		snapshot: SnapshotId::new(0),
		loc: synthesized(),
	};
	rw.create(at, spec).expect("the op is created")
}

/// The results of `op`.
fn results_of(rw: &Rewriter<'_>, op: OpId) -> SmallVec<[ValueId; 2]> {
	rw.ir().op(op).expect("the op exists").results.clone()
}

/// The one value of a literal created at the end of `block`.
fn literal_in(rw: &mut Rewriter<'_>, block: BlockId) -> ValueId {
	let literal = OpKind::Expr(ExprOp::Literal(Box::new(Value::from(0_i64))));
	let op = new_op(rw, InsertPoint::End(block), literal, &[], 1);
	results_of(rw, op)[0]
}

/// A container with an empty request region, and that region's block.
fn new_request() -> (Ir, BlockId) {
	let ir = Ir::new(
		IrScope::Request {
			fragment: None,
		},
		RegionKind::Request,
	);
	let top = ir
		.region(ir.top())
		.and_then(|region| region.blocks.first().copied())
		.expect("the request region holds a block");
	(ir, top)
}

/// A statement in `top`, with its region holding the timestamp, and that region's block.
fn statement_block(rw: &mut Rewriter<'_>, top: BlockId) -> BlockId {
	let stmt = new_op(
		rw,
		InsertPoint::End(top),
		OpKind::Logical(LogOp::Stmt {
			commit: StatementCommit::Auto,
			control: None,
		}),
		&[],
		1,
	);
	let (_, block) =
		rw.create_region(stmt, AUTO, &[timestamp_arg()]).expect("the statement region is created");
	block
}

/// The types of the entry arguments the region of `kind` that `log` owns is built with: the
/// timestamp of a statement and of an isolated region, the element, the position and, for a fold,
/// the accumulator of a loop body.
fn entry_types(log: &LogOp, kind: RegionKind) -> Vec<TypeKnowledge> {
	if let RegionKind::Statement(_) = kind {
		vec![timestamp_arg()]
	} else if kind.isolated_from_above() {
		vec![timestamp_arg()]
	} else if let LogOp::Loop {
		sink,
		..
	} = log
	{
		vec![TypeKnowledge::Unknown; loop_body_args(sink)]
	} else {
		Vec::new()
	}
}

/// Creates `log` at the end of `block` with `operands`, and a region for each kind it declares.
fn op_with_regions(
	rw: &mut Rewriter<'_>,
	block: BlockId,
	log: LogOp,
	operands: &[ValueId],
) -> OpId {
	let results = usize::from(log.arity().results);
	let kinds = log.region_kinds();
	let entries: Vec<Vec<TypeKnowledge>> =
		kinds.iter().map(|kind| entry_types(&log, *kind)).collect();
	let op = new_op(rw, InsertPoint::End(block), OpKind::Logical(log), operands, results);
	for (kind, args) in kinds.iter().zip(&entries) {
		rw.create_region(op, *kind, args).expect("the region is created");
	}
	op
}

/// The rule `verify_op` rejects the op `build` creates under, or `None` when it accepts it.
/// `build` receives the rewriter and the block of the request region.
fn rejection_of(build: impl FnOnce(&mut Rewriter<'_>, BlockId) -> OpId) -> Option<VerifyRule> {
	let (mut ir, top) = new_request();
	let mut facts = FactCache::new();
	let op = build(&mut Rewriter::new(&mut ir, &mut facts), top);
	verify_op(&ir, op).err().map(|failure| failure.rule)
}

/// Only a layer names the computed fields of its input: a projection that is not a layer and names
/// one is rejected, and the same projection naming none is accepted.
#[test]
fn only_a_layer_names_computed_fields() {
	for (computed, rejected) in [(vec![strand("c")], Some(VerifyRule::PerOp)), (Vec::new(), None)] {
		let answer = rejection_of(|rw, top| {
			let block = statement_block(rw, top);
			let input = literal_in(rw, block);
			let project = LogOp::Project(Box::new(ProjectAttrs {
				fields: FieldsShape::Select(Box::new([FieldShape::All])),
				layer: false,
				omit: 0,
				names: Vec::new(),
				computed,
				sort_keys: Vec::new(),
				judged: None,
			}));
			op_with_regions(rw, block, project, &[input])
		});
		assert_eq!(answer, rejected);
	}
}

/// An events op states no event dormant: only the physical write-prune pass does, so a container
/// of the logical phase that holds a dormant event is rejected.
#[test]
fn an_event_is_never_dormant_in_the_logical_phase() {
	use crate::catalog::auth::AuthLimit;
	use crate::catalog::{EventDefinition, EventKind};

	for dormant in [false, true] {
		let answer = rejection_of(|rw, top| {
			let block = statement_block(rw, top);
			let definition = EventDefinition::new(
				"e".into(),
				"t".into(),
				crate::syn::expr("($event = 'DELETE')").expect("a clause"),
				vec![crate::syn::expr("1").expect("a clause")],
				EventKind::Sync,
				AuthLimit::new_no_limit(),
				None,
			);
			let mut attrs =
				super::stages::events_over(&std::sync::Arc::from(vec![definition]), Action::Update)
					.attrs;
			attrs.events[0].dormant = dormant;
			op_with_regions(rw, block, LogOp::Events(Box::new(attrs)), &[])
		});
		assert_eq!(answer, dormant.then_some(VerifyRule::PerOp), "dormant: {dormant}");
	}
}

/// A loop of kind `GraphStep` reads the records its lookup reaches, and no other loop reads data
/// of its own: the loops that run a body answer for the ops of their regions.
#[test]
fn only_a_graph_step_reads_data() {
	let env = Env::new();
	let cx = env.cx();
	for kind in [
		LoopKind::Select,
		LoopKind::Write,
		LoopKind::For,
		LoopKind::Closure,
		LoopKind::ElementFilter,
		LoopKind::GraphStep,
		LoopKind::Recurse,
		LoopKind::Broadcast,
	] {
		let log = LogOp::Loop {
			kind,
			sink: LoopSink::Output,
			order: SourceOrder::Positional,
			step: None,
		};
		let reads = log.local_effects(&cx).set.contains(EffectSet::READS_DATA);
		assert_eq!(reads, kind == LoopKind::GraphStep, "{kind:?}");
	}
}

/// A container built from each op's `arity()` and `region_kinds()` is accepted: the operands its
/// arity asks for, one region of each kind it declares, a statement region holding the timestamp
/// and every isolated region receiving it first.
#[test]
fn arity_and_regions_describe_a_valid_fixture() {
	let mut fixtures: Vec<LogOp> = LogTag::ALL.iter().map(|tag| fixture(*tag)).collect();
	fixtures.extend([
		LogOp::Loop {
			kind: LoopKind::Closure,
			sink: LoopSink::Fold,
			order: SourceOrder::Positional,
			step: None,
		},
		LogOp::Loop {
			kind: LoopKind::Select,
			sink: LoopSink::Output,
			order: SourceOrder::Delivery,
			step: None,
		},
		LogOp::Loop {
			kind: LoopKind::GraphStep,
			sink: LoopSink::Output,
			order: SourceOrder::Delivery,
			step: Some(Box::new(StepAttrs::Lookup(LookupAttrs {
				limit: None,
				start: None,
				subjects: Box::new([LookupSubjectShape {
					table: table("knows"),
					referencing_field: None,
					range: None,
				}]),
				..rich_lookup()
			}))),
		},
		LogOp::Loop {
			kind: LoopKind::Recurse,
			sink: LoopSink::Output,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Recurse(RecurseAttrs {
				depth: Recurse::Fixed(1),
				form: RecursePathForm::Nested,
				instruction: None,
			}))),
		},
		LogOp::Loop {
			kind: LoopKind::Broadcast,
			sink: LoopSink::Output,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Flatten)),
		},
		LogOp::Loop {
			kind: LoopKind::Closure,
			sink: LoopSink::Decisive(crate::exec::ir::logical::ops::DecisiveKind::Any),
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Closure(ClosureLoopAttrs {
				function: Strand::new("array::some"),
				method: false,
			}))),
		},
		LogOp::Txn {
			mode: TxnRegionMode::Explicit {
				start: TxnStart::Begin,
				end: TxnRegionEnd::Unterminated,
				emission: Emission::Hold,
			},
		},
		LogOp::Txn {
			mode: TxnRegionMode::Session {
				id: 0,
			},
		},
		LogOp::Stmt {
			commit: StatementCommit::InTxn,
			control: None,
		},
	]);
	for log in fixtures {
		let name = log.tag().mnemonic();
		let operands = match log.arity().operands {
			OperandRule::Exactly(count) | OperandRule::AtLeast(count) => count,
			OperandRule::Grouped(sizes) => sizes.first().copied().unwrap_or(0),
		};
		let stands_in_request = matches!(log, LogOp::Txn { .. } | LogOp::Stmt { .. });
		let rejected = rejection_of(|rw, top| {
			let block = if stands_in_request {
				top
			} else {
				statement_block(rw, top)
			};
			let values: Vec<ValueId> = (0..operands).map(|_| literal_in(rw, block)).collect();
			op_with_regions(rw, block, log, &values)
		});
		assert_eq!(rejected, None, "{name}");
	}
}

/// The verifier reads position, shape and structure: a block that feeds a `LET` is a value, a fold
/// body takes the accumulator, and transactions and statements stand only where the request puts
/// them.
#[test]
fn verifier_checks_position_shape_and_structure() {
	// A block that feeds a LET is a value block; a statement block is a `Block`.
	for (region, accepted) in [(RegionKind::ValueBlock, true), (RegionKind::Block, false)] {
		let rejected = rejection_of(|rw, top| {
			let block = statement_block(rw, top);
			let inner = new_op(rw, InsertPoint::End(block), OpKind::Logical(LogOp::Block), &[], 1);
			rw.create_region(inner, region, &[]).expect("the region is created");
			let value = results_of(rw, inner)[0];
			let binding = OpKind::Logical(LogOp::Let(Box::new(untyped_let("x"))));
			new_op(rw, InsertPoint::End(block), binding, &[value], 1);
			inner
		});
		assert_eq!(rejected.is_none(), accepted, "{region:?}");
		if !accepted {
			assert_eq!(rejected, Some(VerifyRule::RegionKinds));
		}
	}

	// A value block whose value nothing reads stands as a statement; so does a statement block.
	for region in [RegionKind::ValueBlock, RegionKind::Block] {
		let rejected = rejection_of(|rw, top| {
			let block = statement_block(rw, top);
			let inner = new_op(rw, InsertPoint::End(block), OpKind::Logical(LogOp::Block), &[], 1);
			rw.create_region(inner, region, &[]).expect("the region is created");
			inner
		});
		assert_eq!(rejected, None, "{region:?}");
	}

	// A fold body holds the element, the position and the accumulator; any other loop two.
	for (sink, args, accepted) in [
		(LoopSink::Fold, 2, false),
		(LoopSink::Fold, 3, true),
		(LoopSink::Output, 2, true),
		(LoopSink::Output, 3, false),
	] {
		let rejected = rejection_of(|rw, top| {
			let block = statement_block(rw, top);
			let source = literal_in(rw, block);
			let log = LogOp::Loop {
				kind: LoopKind::Closure,
				sink,
				order: SourceOrder::Positional,
				step: None,
			};
			let op = new_op(rw, InsertPoint::End(block), OpKind::Logical(log), &[source], 1);
			rw.create_region(op, RegionKind::Loop, &vec![TypeKnowledge::Unknown; args])
				.expect("the region is created");
			op
		});
		assert_eq!(rejected.is_none(), accepted, "{args} arguments");
	}

	// A transaction does not nest in a statement.
	let rejected = rejection_of(|rw, top| {
		let block = statement_block(rw, top);
		let txn = LogOp::Txn {
			mode: TxnRegionMode::Explicit {
				start: TxnStart::Begin,
				end: TxnRegionEnd::Commit,
				emission: Emission::Stream,
			},
		};
		op_with_regions(rw, block, txn, &[])
	});
	assert_eq!(rejected, Some(VerifyRule::PerOp));

	// The request region holds statements and transactions only.
	let rejected = rejection_of(|rw, top| op_with_regions(rw, top, LogOp::Block, &[]));
	assert_eq!(rejected, Some(VerifyRule::PerOp));

	// A statement region holds the statement timestamp.
	let rejected = rejection_of(|rw, top| {
		let stmt = LogOp::Stmt {
			commit: StatementCommit::Auto,
			control: None,
		};
		let op = new_op(rw, InsertPoint::End(top), OpKind::Logical(stmt), &[], 1);
		rw.create_region(op, AUTO, &[]).expect("the region is created");
		op
	});
	assert_eq!(rejected, Some(VerifyRule::PerOp));

	// A session transaction holds every statement of its fragment.
	let rejected = rejection_of(|rw, top| {
		let session = LogOp::Txn {
			mode: TxnRegionMode::Session {
				id: 0,
			},
		};
		let txn = op_with_regions(rw, top, session, &[]);
		op_with_regions(
			rw,
			top,
			LogOp::Stmt {
				commit: StatementCommit::Auto,
				control: None,
			},
			&[],
		);
		txn
	});
	assert_eq!(rejected, Some(VerifyRule::PerOp));

	// An unterminated transaction is the last transaction of the request.
	let rejected = rejection_of(|rw, top| {
		let unterminated = LogOp::Txn {
			mode: TxnRegionMode::Explicit {
				start: TxnStart::Begin,
				end: TxnRegionEnd::Unterminated,
				emission: Emission::Stream,
			},
		};
		let first = op_with_regions(rw, top, unterminated, &[]);
		let later = LogOp::Txn {
			mode: TxnRegionMode::Explicit {
				start: TxnStart::Begin,
				end: TxnRegionEnd::Commit,
				emission: Emission::Stream,
			},
		};
		op_with_regions(rw, top, later, &[]);
		first
	});
	assert_eq!(rejected, Some(VerifyRule::PerOp));

	// An operand count outside the arity is rejected.
	let rejected = rejection_of(|rw, top| {
		let block = statement_block(rw, top);
		let value = literal_in(rw, block);
		op_with_regions(rw, block, LogOp::Distinct, &[value, value])
	});
	assert_eq!(rejected, Some(VerifyRule::Arity));
}

/// A write stage hosts the clause regions of the statement it evaluates, after its own region and
/// in the order of the clauses; a stage that evaluates no clause hosts none. An assignment hosts
/// the filters of its place.
#[test]
fn write_stages_host_their_clauses() {
	let data = [
		RegionKind::Clause(ClauseRole::SetValue),
		RegionKind::Clause(ClauseRole::Content),
		RegionKind::Clause(ClauseRole::DuplicateUpdate),
	];
	let cond = [RegionKind::Clause(ClauseRole::WriteCond)];
	let cases: Vec<(WriteStageKind, Vec<RegionKind>, bool)> = vec![
		(WriteStageKind::ComputeInputData, vec![], true),
		(WriteStageKind::ComputeInputData, vec![data[0]], true),
		(WriteStageKind::ComputeInputData, vec![data[0], data[2]], true),
		(WriteStageKind::ComputeInputData, vec![data[1], data[1], data[2]], true),
		(WriteStageKind::ComputeInputData, vec![data[1], data[0]], false),
		(WriteStageKind::ComputeInputData, cond.to_vec(), false),
		(WriteStageKind::CheckWhereCondition, cond.to_vec(), true),
		(WriteStageKind::CheckWhereCondition, data[..1].to_vec(), false),
		(WriteStageKind::StoreRecordData, data[..1].to_vec(), false),
		(WriteStageKind::StoreRecordData, vec![], true),
	];
	for (stage, hosted, accepted) in cases {
		let rejected = rejection_of(|rw, top| {
			let block = statement_block(rw, top);
			let log = LogOp::WriteStage {
				stage,
				mark: static_mark(stage),
			};
			let op = new_op(rw, InsertPoint::End(block), OpKind::Logical(log), &[], 0);
			rw.create_region(op, RegionKind::WriteStage, &[timestamp_arg()])
				.expect("the region is created");
			for kind in &hosted {
				rw.create_region(op, *kind, &[TypeKnowledge::Unknown])
					.expect("the region is created");
			}
			op
		});
		assert_eq!(rejected.is_none(), accepted, "{stage:?} hosting {hosted:?}");
		if !accepted {
			assert_eq!(rejected, Some(VerifyRule::RegionKinds));
		}
	}

	for (filters, accepted) in [(0, true), (2, true)] {
		let rejected = rejection_of(|rw, top| {
			let block = statement_block(rw, top);
			let value = literal_in(rw, block);
			let log = LogOp::Assign(Box::new(AssignAttrs {
				place: place(),
				op: AssignOperator::Assign,
			}));
			let op = new_op(rw, InsertPoint::End(block), OpKind::Logical(log), &[value], 0);
			for _ in 0..filters {
				rw.create_region(op, WHERE, &[TypeKnowledge::Unknown])
					.expect("the region is created");
			}
			op
		});
		assert_eq!(rejected.is_none(), accepted, "{filters} filters");
	}
}

// ---------------------------------------------------------------------------------------------
// Transaction grouping
// ---------------------------------------------------------------------------------------------

/// Each group of the statement grouping maps to the top-level regions the binder builds:
/// auto-commit statement is a region of its own, a block is one transaction with a region per
/// statement including its controls, and a misplaced control keeps its mark.
#[test]
fn regions_of_groups_statements() {
	use RequestStatementKind::{Begin, Cancel, Commit, Run};

	let slot = |index: usize, commit: StatementCommit, body: StatementBody| StatementSlot {
		index,
		commit,
		body,
	};
	let explicit = |end: TxnRegionEnd| TxnRegionMode::Explicit {
		start: TxnStart::Begin,
		end,
		emission: Emission::Stream,
	};

	// Auto-commit statements are no transaction.
	assert_eq!(
		regions_of(&lower_request(&[Run, Run], false), None, None),
		vec![
			TopRegion::Statement(slot(0, StatementCommit::Auto, StatementBody::Run)),
			TopRegion::Statement(slot(1, StatementCommit::Auto, StatementBody::Run)),
		]
	);

	// `BEGIN; …; COMMIT` holds a region per statement, controls included; the next statement is
	// auto-commit again.
	assert_eq!(
		regions_of(&lower_request(&[Begin, Run, Commit, Run], false), None, None),
		vec![
			TopRegion::Transaction {
				mode: explicit(TxnRegionEnd::Commit),
				statements: vec![
					slot(0, StatementCommit::InTxn, StatementBody::Begin),
					slot(1, StatementCommit::InTxn, StatementBody::Run),
					slot(2, StatementCommit::InTxn, StatementBody::Commit),
				],
			},
			TopRegion::Statement(slot(3, StatementCommit::Auto, StatementBody::Run)),
		]
	);

	// `CANCEL` ends a block too.
	assert_eq!(
		regions_of(&lower_request(&[Begin, Cancel], false), None, None),
		vec![TopRegion::Transaction {
			mode: explicit(TxnRegionEnd::Cancel),
			statements: vec![
				slot(0, StatementCommit::InTxn, StatementBody::Begin),
				slot(1, StatementCommit::InTxn, StatementBody::Cancel),
			],
		}]
	);

	// A request that ends inside a block leaves it unterminated; in a fragment of a stream it
	// stays open.
	let unterminated = lower_request(&[Begin, Run], false);
	let held = vec![
		slot(0, StatementCommit::InTxn, StatementBody::Begin),
		slot(1, StatementCommit::InTxn, StatementBody::Run),
	];
	assert_eq!(
		regions_of(&unterminated, None, None),
		vec![TopRegion::Transaction {
			mode: explicit(TxnRegionEnd::Unterminated),
			statements: held.clone(),
		}]
	);
	assert_eq!(
		regions_of(&unterminated, Some(3), None),
		vec![TopRegion::Transaction {
			mode: explicit(TxnRegionEnd::Open),
			statements: held,
		}]
	);

	// A `COMMIT` with no block is a statement of its own that holds the error of its mark.
	assert_eq!(
		regions_of(&lower_request(&[Commit], false), None, None),
		vec![TopRegion::Statement(slot(
			0,
			StatementCommit::Auto,
			StatementBody::Misplaced(MisplacedControl::CommitOutsideBlock)
		))]
	);

	// A `BEGIN` inside a block is marked and the block still ends at its `COMMIT`.
	assert_eq!(
		regions_of(&lower_request(&[Begin, Begin, Run, Commit], false), None, None),
		vec![TopRegion::Transaction {
			mode: explicit(TxnRegionEnd::Commit),
			statements: vec![
				slot(0, StatementCommit::InTxn, StatementBody::Begin),
				slot(
					1,
					StatementCommit::InTxn,
					StatementBody::Misplaced(MisplacedControl::NestedBegin)
				),
				slot(2, StatementCommit::InTxn, StatementBody::Run),
				slot(3, StatementCommit::InTxn, StatementBody::Commit),
			],
		}]
	);

	// A request in a session transaction is that transaction; its controls are marked.
	assert_eq!(
		regions_of(&lower_request(&[Run, Begin], true), None, Some(7)),
		vec![TopRegion::Transaction {
			mode: TxnRegionMode::Session {
				id: 7
			},
			statements: vec![
				slot(0, StatementCommit::InTxn, StatementBody::Run),
				slot(
					1,
					StatementCommit::InTxn,
					StatementBody::Misplaced(MisplacedControl::BeginInSession)
				),
			],
		}]
	);

	assert!(regions_of(&[], None, None).is_empty());
}

// ---------------------------------------------------------------------------------------------
// Region constraints
// ---------------------------------------------------------------------------------------------

/// The constraint table and its walk, over containers built by hand so that each case names the
/// region and the op it holds.
mod region_constraints {
	use super::*;
	use crate::exec::ir::core::ir::{Walk, WalkOrder};
	use crate::exec::ir::expr::ops::{ClosureAttrs, NowWhich};
	use crate::exec::ir::iface::effects::OpEffects;
	use crate::exec::ir::iface::ids::RegionId;
	use crate::exec::ir::iface::region_kind::{Admission, Construct, Section4bRow};
	use crate::exec::ir::iface::statements::{DefaultShape, DefineField, DefineTable, IndexSpec};
	use crate::exec::ir::logical::constraints::{
		self, RegionRejection, admits, known_non_deterministic, known_write,
	};
	use crate::expr::logical::EffectSet;

	const ALL: [Construct; 11] = [
		Construct::Write,
		Construct::Ddl,
		Construct::Let,
		Construct::Sleep,
		Construct::ExternalCall,
		Construct::NonDeterministic,
		Construct::OtherRecordRead,
		Construct::UnenclosedBreak,
		Construct::EscapingReturn,
		Construct::TxnControl,
		Construct::Use,
	];
	const GROUP: RegionKind = RegionKind::Clause(ClauseRole::GroupKey);
	const ORDER: RegionKind = RegionKind::Clause(ClauseRole::OrderKey);
	const SPLIT: RegionKind = RegionKind::Clause(ClauseRole::Split);
	const PROJECTION: RegionKind = RegionKind::Clause(ClauseRole::Projection);
	const WRITE_COND: RegionKind = RegionKind::Clause(ClauseRole::WriteCond);

	fn log(op: LogOp) -> OpKind {
		OpKind::Logical(op)
	}

	fn put(rw: &mut Rewriter<'_>, block: BlockId, kind: OpKind) -> OpId {
		new_op(rw, InsertPoint::End(block), kind, &[], 0)
	}

	/// An op of `owner` at the end of `block` with one region of `kind`, and that region's block.
	fn region_of(
		rw: &mut Rewriter<'_>,
		block: BlockId,
		owner: OpKind,
		kind: RegionKind,
	) -> BlockId {
		let args = if kind.isolated_from_above() {
			vec![timestamp_arg()]
		} else {
			Vec::new()
		};
		let op = put(rw, block, owner);
		rw.create_region(op, kind, &args).expect("the region is created").1
	}

	/// A container whose one statement holds what `build` puts in the statement's block.
	fn container(build: impl FnOnce(&mut Rewriter<'_>, BlockId)) -> Ir {
		let (mut ir, top) = new_request();
		let mut facts = FactCache::new();
		{
			let mut rw = Rewriter::new(&mut ir, &mut facts);
			let block = statement_block(&mut rw, top);
			build(&mut rw, block);
		}
		ir
	}

	/// A container that holds `inner` directly in a region of `kind` owned by `owner`.
	fn holding(owner: OpKind, kind: RegionKind, inner: OpKind) -> Ir {
		container(|rw, stmt| {
			let block = region_of(rw, stmt, owner, kind);
			put(rw, block, inner);
		})
	}

	fn first(ir: &Ir) -> Option<RegionRejection> {
		let env = Env::new();
		constraints::rejection_of(ir, &env.cx()).expect("the container is walked")
	}

	fn region_with_kind(ir: &Ir, kind: RegionKind) -> RegionId {
		let mut found = None;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			for region in &ir.op(op).expect("the op exists").regions {
				if ir.region(*region).expect("the region exists").kind == kind {
					found.get_or_insert(*region);
				}
			}
			Walk::Continue
		});
		found.expect("the container holds a region of the kind")
	}

	/// Asserts that `ir` is rejected for `construct` by `row` in a region of `region`.
	fn assert_rejected(
		ir: &Ir,
		construct: Construct,
		row: Section4bRow,
		region: RegionKind,
		case: &str,
	) {
		let Some(found) = first(ir) else {
			panic!("{case}: nothing was rejected");
		};
		assert_eq!((found.construct, found.row, found.region), (construct, row, region), "{case}");
	}

	fn assert_admitted(ir: &Ir, case: &str) {
		assert_eq!(first(ir), None, "{case}");
	}

	fn full() -> PermissionsShape {
		PermissionsShape {
			select: PermissionShape::Full,
			create: PermissionShape::Full,
			update: PermissionShape::Full,
			delete: PermissionShape::Full,
		}
	}

	fn define(object: DefineObject) -> OpKind {
		log(LogOp::Define(Box::new(DefineAttrs {
			object,
			mode: DefineKind::Default,
			template: template("StoredDefinition"),
			checks: DdlChecks::None,
		})))
	}

	fn define_table() -> OpKind {
		define(DefineObject::Table(Box::new(DefineTable {
			id: None,
			name: 0,
			drop: false,
			full: false,
			view: None,
			permissions: full(),
			changefeed: None,
			comment: 1,
			table_type: TableType::Any,
			graphql_alias: None,
			graphql_deprecated: None,
			inline_edges_cap: None,
			inline_refs_cap: None,
		})))
	}

	fn define_field() -> OpKind {
		define(DefineObject::Field(Box::new(DefineField {
			name: 0,
			what: 1,
			field_kind: None,
			flexible: false,
			readonly: false,
			inline: false,
			value: None,
			assert: None,
			computed: None,
			default: DefaultShape::None,
			permissions: full(),
			comment: 2,
			reference: None,
			graphql_alias: None,
			graphql_deprecated: None,
			migrate: None,
			sql_type: None,
		})))
	}

	fn define_index() -> OpKind {
		define(DefineObject::Index {
			name: 0,
			name_text: "ix".into(),
			what: 1,
			columns: Box::new([]),
			index: IndexSpec::Idx,
			comment: 2,
			concurrently: false,
		})
	}

	fn define_param() -> OpKind {
		log(fixture(LogTag::Define))
	}

	/// The ops a condition clause rejects, with what each is.
	fn condition_inners() -> Vec<(OpKind, Construct)> {
		vec![
			(log(fixture(LogTag::Write)), Construct::Write),
			(log(fixture(LogTag::MatchDelete)), Construct::Write),
			(log(fixture(LogTag::Define)), Construct::Ddl),
			(log(fixture(LogTag::Remove)), Construct::Ddl),
			(log(fixture(LogTag::Alter)), Construct::Ddl),
			(log(fixture(LogTag::Rebuild)), Construct::Ddl),
			(log(fixture(LogTag::Sleep)), Construct::Sleep),
		]
	}

	/// The table: each row rejects the constructs it names in the regions it names, and every
	/// other construct, region and disabled row admits.
	#[test]
	fn region_constraints_table() {
		let rows: Vec<(Vec<RegionKind>, Section4bRow, Vec<Construct>)> = vec![
			(
				vec![WHERE, ORDER, GROUP, SPLIT],
				Section4bRow::ConditionClause,
				vec![Construct::Write, Construct::Ddl, Construct::Sleep],
			),
			(
				vec![
					RegionKind::PermissionFrame,
					RegionKind::FieldFrame(FieldRole::Assert),
					RegionKind::StoredClause(StoredRole::Permissions),
					RegionKind::StoredClause(StoredRole::Assert),
				],
				Section4bRow::PermissionOrAssertFrame,
				vec![Construct::Write, Construct::Ddl],
			),
			(
				vec![
					RegionKind::FieldFrame(FieldRole::Value),
					RegionKind::FieldFrame(FieldRole::Default),
					RegionKind::FieldFrame(FieldRole::Computed),
					RegionKind::StoredClause(StoredRole::Value),
					RegionKind::StoredClause(StoredRole::Default),
					RegionKind::StoredClause(StoredRole::Computed),
				],
				Section4bRow::FieldValueFrame,
				vec![Construct::Write, Construct::Ddl],
			),
			(
				vec![RegionKind::StoredClause(StoredRole::IndexColumn)],
				Section4bRow::IndexColumn,
				vec![Construct::NonDeterministic, Construct::OtherRecordRead],
			),
		];
		for (kinds, row, rejected) in &rows {
			for kind in kinds {
				for construct in ALL {
					let expected = if rejected.contains(&construct) {
						Admission::Reject {
							construct,
							row: *row,
						}
					} else {
						Admission::Admit
					};
					assert_eq!(admits(*kind, construct), expected, "{kind:?} {construct:?}");
				}
			}
		}

		// A region no enabled row names admits every construct, and so does an expression
		// region: its row holds as an invariant and never rejects.
		let admitting = [
			WRITE_COND,
			PROJECTION,
			RegionKind::Request,
			RegionKind::Statement(StatementCommit::Auto),
			RegionKind::Block,
			RegionKind::ValueBlock,
			RegionKind::StatementIf,
			RegionKind::ValueIf,
			RegionKind::StatementFor,
			RegionKind::ValueFor,
			RegionKind::Loop,
			RegionKind::Subquery,
			RegionKind::FunctionBody,
			RegionKind::ClosureBody,
			RegionKind::EventFrame,
			RegionKind::LazyOperand,
			SHAPE_TO_NONE,
			RegionKind::WriteStage,
			RegionKind::Deadline,
			RegionKind::StoredClause(StoredRole::Body),
			RegionKind::StoredClause(StoredRole::When),
			RegionKind::StoredClause(StoredRole::View),
		];
		for kind in admitting {
			for construct in ALL {
				assert_eq!(admits(kind, construct), Admission::Admit, "{kind:?} {construct:?}");
			}
		}
	}

	/// Each enabled row has a container it rejects and one it admits; the disabled rows and
	/// constructs admit.
	#[test]
	fn region_constraints_rows() {
		// ConditionClause: a write, a definition and a SLEEP in a condition, order, group
		// or split clause. The group key of an aggregate is the error scope it owns.
		let owners: Vec<(OpKind, RegionKind, RegionKind)> = vec![
			(log(fixture(LogTag::Filter)), WHERE, WHERE),
			(log(fixture(LogTag::Sort)), ORDER, ORDER),
			(log(fixture(LogTag::Split)), SPLIT, SPLIT),
			(log(fixture(LogTag::Aggregate)), SHAPE_TO_NONE, GROUP),
		];
		for (owner, kind, judged) in &owners {
			for (inner, construct) in condition_inners() {
				let ir = holding(owner.clone(), *kind, inner.clone());
				let case = format!("{kind:?} holding {construct:?}");
				assert_rejected(&ir, construct, Section4bRow::ConditionClause, *judged, &case);
			}
		}
		// The same ops in a region the row does not name are admitted: a projection, the
		// condition of a write, and the error scope of anything but an aggregate.
		for (owner, kind) in [
			(log(fixture(LogTag::Project)), PROJECTION),
			(log(fixture(LogTag::Filter)), WRITE_COND),
			(log(fixture(LogTag::Block)), SHAPE_TO_NONE),
		] {
			for (inner, construct) in condition_inners() {
				let ir = holding(owner.clone(), kind, inner);
				assert_admitted(&ir, &format!("{kind:?} holding {construct:?}"));
			}
		}
		// Reads are admitted in a condition, and so is a BREAK with no loop around it: it raises
		// when it runs.
		for inner in [log(fixture(LogTag::Source)), log(fixture(LogTag::Break))] {
			assert_admitted(&holding(log(fixture(LogTag::Filter)), WHERE, inner), "a read");
		}
		// A LET written in a block of a condition is admitted, as is one written in a closure
		// body. A write is rejected through a closure.
		let nested = |inner: LogOp| {
			container(|rw, stmt| {
				let clause = region_of(rw, stmt, log(fixture(LogTag::Filter)), WHERE);
				let block = region_of(rw, clause, log(LogOp::Block), RegionKind::ValueBlock);
				put(rw, block, log(inner));
			})
		};
		assert_admitted(&nested(fixture(LogTag::Let)), "a LET in a block of a condition");
		let closure = |inner: LogOp| {
			container(|rw, stmt| {
				let clause = region_of(rw, stmt, log(fixture(LogTag::Filter)), WHERE);
				let attrs = ClosureAttrs {
					params: Box::new([]),
					returns: None,
					body_sql: "".into(),
					captures: Box::new([]),
					reads_row: false,
					reads_root: false,
					literal: Default::default(),
				};
				let body = region_of(
					rw,
					clause,
					OpKind::Expr(ExprOp::ClosureCreate(Box::new(attrs))),
					RegionKind::ClosureBody,
				);
				put(rw, body, log(inner));
			})
		};
		assert_admitted(&closure(fixture(LogTag::Let)), "a LET in a closure body");
		assert_rejected(
			&closure(fixture(LogTag::Write)),
			Construct::Write,
			Section4bRow::ConditionClause,
			WHERE,
			"a write in a closure body",
		);

		// PermissionOrAssertFrame, at DEFINE: a table's or a field's permission, a field's
		// assertion. The permission of any other definition is not covered.
		let permissions = RegionKind::StoredClause(StoredRole::Permissions);
		let assertion = RegionKind::StoredClause(StoredRole::Assert);
		for (inner, construct) in [
			(log(fixture(LogTag::Write)), Construct::Write),
			(log(fixture(LogTag::Define)), Construct::Ddl),
		] {
			for (owner, kind) in [
				(define_table(), permissions),
				(define_field(), permissions),
				(define_field(), assertion),
			] {
				let ir = holding(owner, kind, inner.clone());
				assert_rejected(
					&ir,
					construct,
					Section4bRow::PermissionOrAssertFrame,
					kind,
					&format!("{kind:?} holding {construct:?}"),
				);
			}
			assert_admitted(
				&holding(define_param(), permissions, inner.clone()),
				"the permission of a parameter",
			);
		}
		for inner in [fixture(LogTag::Let), fixture(LogTag::Sleep), fixture(LogTag::Source)] {
			assert_admitted(&holding(define_table(), permissions, log(inner)), "a read, a LET");
		}

		// FieldValueFrame, at DEFINE: a write or a definition in a value, default or computed
		// clause of a field. Another definition's clause of the same role is not covered.
		for role in [StoredRole::Value, StoredRole::Default, StoredRole::Computed] {
			let kind = RegionKind::StoredClause(role);
			for (inner, construct) in [
				(log(fixture(LogTag::Write)), Construct::Write),
				(log(fixture(LogTag::Define)), Construct::Ddl),
			] {
				let ir = holding(define_field(), kind, inner.clone());
				assert_rejected(
					&ir,
					construct,
					Section4bRow::FieldValueFrame,
					kind,
					&format!("{kind:?} holding {construct:?}"),
				);
				assert_admitted(&holding(define_table(), kind, inner), "a clause of a table");
			}
			for inner in [fixture(LogTag::Let), fixture(LogTag::Sleep), fixture(LogTag::Source)] {
				assert_admitted(&holding(define_field(), kind, log(inner)), "a read, a LET");
			}
		}

		// IndexColumn: the statement clock and a read of another record. A call whose effects
		// are not known admits, and so does a write.
		let column = RegionKind::StoredClause(StoredRole::IndexColumn);
		let now = OpKind::Expr(ExprOp::Now {
			which: NowWhich::Datetime,
		});
		let deref = OpKind::Expr(ExprOp::Deref {
			table_hint: None,
		});
		for (inner, construct) in [
			(now, Construct::NonDeterministic),
			(deref, Construct::OtherRecordRead),
			(log(fixture(LogTag::Source)), Construct::OtherRecordRead),
		] {
			let ir = holding(define_index(), column, inner);
			assert_rejected(
				&ir,
				construct,
				Section4bRow::IndexColumn,
				column,
				&format!("an index column holding {construct:?}"),
			);
		}
		for inner in
			[OpKind::Expr(ExprOp::Invoke), log(fixture(LogTag::Write)), log(fixture(LogTag::Let))]
		{
			assert_admitted(&holding(define_index(), column, inner), "an index column");
		}
		let ir = holding(define_index(), column, OpKind::Expr(ExprOp::Invoke));
		let env = Env::new();
		assert_eq!(
			constraints::check_with(&ir, &env.cx(), ir.top()).expect("the container is walked"),
			Admission::Admit
		);

		// ExpressionRegion: a block's own LET and an operand block are admitted; the row is an
		// invariant and never produces a rejection.
		let ir = container(|rw, stmt| {
			let block = region_of(rw, stmt, log(LogOp::Block), RegionKind::ValueBlock);
			put(rw, block, log(fixture(LogTag::Let)));
			put(rw, block, log(fixture(LogTag::Use)));
		});
		assert_admitted(&ir, "a LET in a value block");
		for kind in [RegionKind::ValueBlock, RegionKind::ValueIf, RegionKind::Subquery] {
			for construct in ALL {
				assert!(!matches!(
					admits(kind, construct),
					Admission::Reject {
						row: Section4bRow::ExpressionRegion,
						..
					}
				));
			}
		}
	}

	/// Pre-order decides the first rejection, and the innermost constrained region names it.
	#[test]
	fn region_constraints_first_rejection_is_pre_order_and_innermost() {
		let ir = container(|rw, stmt| {
			let clause = region_of(rw, stmt, log(fixture(LogTag::Filter)), WHERE);
			let block = region_of(rw, clause, log(LogOp::Block), RegionKind::ValueBlock);
			put(rw, block, log(fixture(LogTag::Write)));
			put(rw, clause, log(fixture(LogTag::Sleep)));
		});
		assert_rejected(
			&ir,
			Construct::Write,
			Section4bRow::ConditionClause,
			WHERE,
			"nested first",
		);

		let ir = container(|rw, stmt| {
			let clause = region_of(rw, stmt, log(fixture(LogTag::Filter)), WHERE);
			put(rw, clause, log(fixture(LogTag::Sleep)));
			let block = region_of(rw, clause, log(LogOp::Block), RegionKind::ValueBlock);
			put(rw, block, log(fixture(LogTag::Write)));
		});
		assert_rejected(
			&ir,
			Construct::Sleep,
			Section4bRow::ConditionClause,
			WHERE,
			"sibling first",
		);

		// A filter written in the value clause of a field is judged by the filter, which is the
		// region nearest the write.
		let ir = container(|rw, stmt| {
			let value =
				region_of(rw, stmt, define_field(), RegionKind::StoredClause(StoredRole::Value));
			let clause = region_of(rw, value, log(fixture(LogTag::Filter)), WHERE);
			put(rw, clause, log(fixture(LogTag::Write)));
		});
		assert_rejected(&ir, Construct::Write, Section4bRow::ConditionClause, WHERE, "innermost");
		// A write the filter admits is still the value clause's.
		let ir = container(|rw, stmt| {
			let value =
				region_of(rw, stmt, define_field(), RegionKind::StoredClause(StoredRole::Value));
			let clause = region_of(rw, value, log(fixture(LogTag::Project)), PROJECTION);
			put(rw, clause, log(fixture(LogTag::Write)));
		});
		assert_rejected(
			&ir,
			Construct::Write,
			Section4bRow::FieldValueFrame,
			RegionKind::StoredClause(StoredRole::Value),
			"outer",
		);
	}

	/// Frames that hold persisted catalog code are not entered by a walk that reaches them from
	/// the request, and are judged when the walk starts on them.
	#[test]
	fn region_constraints_do_not_judge_persisted_code() {
		let env = Env::new();
		let frames = [
			(log(fixture(LogTag::PermissionFilter)), RegionKind::PermissionFrame),
			(log(fixture(LogTag::Project)), RegionKind::FieldFrame(FieldRole::Computed)),
			(log(fixture(LogTag::Project)), RegionKind::FieldFrame(FieldRole::Value)),
			(log(fixture(LogTag::WriteStage)), RegionKind::WriteStage),
		];
		for (owner, kind) in frames {
			let ir = container(|rw, stmt| {
				let clause = region_of(rw, stmt, log(fixture(LogTag::Filter)), WHERE);
				let frame = region_of(rw, clause, owner, kind);
				put(rw, frame, log(fixture(LogTag::Write)));
			});
			assert_admitted(&ir, &format!("{kind:?} reached from a condition"));
			assert_eq!(
				constraints::check(&ir, ir.top()).expect("the container is walked"),
				Admission::Admit
			);
		}

		let ir = container(|rw, stmt| {
			let frame = region_of(
				rw,
				stmt,
				log(fixture(LogTag::PermissionFilter)),
				RegionKind::PermissionFrame,
			);
			put(rw, frame, log(fixture(LogTag::Write)));
		});
		let frame = region_with_kind(&ir, RegionKind::PermissionFrame);
		let rejected = Admission::Reject {
			construct: Construct::Write,
			row: Section4bRow::PermissionOrAssertFrame,
		};
		assert_eq!(constraints::check(&ir, frame).expect("the frame is walked"), rejected);
		assert_eq!(
			constraints::check_with(&ir, &env.cx(), frame).expect("the frame is walked"),
			rejected
		);
		assert_admitted(&ir, "a permission frame reached from the request");

		// The frame of an assertion is the permission row's, and a value frame the field row's.
		for (role, row) in [
			(FieldRole::Assert, Section4bRow::PermissionOrAssertFrame),
			(FieldRole::Value, Section4bRow::FieldValueFrame),
			(FieldRole::Default, Section4bRow::FieldValueFrame),
			(FieldRole::Computed, Section4bRow::FieldValueFrame),
		] {
			let kind = RegionKind::FieldFrame(role);
			let ir = holding(log(fixture(LogTag::Project)), kind, log(fixture(LogTag::Write)));
			let frame = region_with_kind(&ir, kind);
			assert_eq!(
				constraints::check(&ir, frame).expect("the frame is walked"),
				Admission::Reject {
					construct: Construct::Write,
					row,
				}
			);
		}
	}

	/// An effect answer rejects only when it is known: the top of the lattice and an opaque body
	/// admit.
	#[test]
	fn region_constraints_reject_known_effects_only() {
		let with = |set: EffectSet| {
			let mut effects = OpEffects::top();
			effects.set = set;
			effects
		};
		assert!(!known_write(&OpEffects::top()));
		assert!(!known_write(&with(EffectSet::TOP)));
		assert!(!known_write(&with(EffectSet::WRITES_DATA | EffectSet::OPAQUE_BODY)));
		assert!(!known_write(&with(EffectSet::PURE)));
		assert!(!known_write(&with(EffectSet::READS_DATA)));
		assert!(known_write(&with(EffectSet::WRITES_DATA)));
		assert!(known_write(&with(EffectSet::WRITES_DATA | EffectSet::READS_DATA)));

		assert!(!known_non_deterministic(&OpEffects::top()));
		assert!(!known_non_deterministic(&with(EffectSet::ENTROPY | EffectSet::OPAQUE_BODY)));
		assert!(!known_non_deterministic(&with(EffectSet::PURE)));
		assert!(!known_non_deterministic(&with(EffectSet::READS_DATA)));
		for flag in [EffectSet::ENTROPY, EffectSet::ADVANCES_STATE, EffectSet::CLOCK] {
			assert!(known_non_deterministic(&with(flag)));
		}
	}

	/// The error is an invalid statement naming the construct, the region and the 1-based
	/// statement.
	#[test]
	fn region_constraints_error_text() {
		let ir = holding(log(fixture(LogTag::Filter)), WHERE, log(fixture(LogTag::Write)));
		let found = first(&ir).expect("the write is rejected");
		assert_eq!(found.loc.stmt, 0);
		assert_eq!(
			found.error().to_string(),
			"Invalid statement: a write is not allowed in a WHERE clause (statement 1)"
		);

		let constructs = [
			(Construct::Write, "a write"),
			(Construct::Ddl, "a schema statement"),
			(Construct::Let, "LET"),
			(Construct::Sleep, "SLEEP"),
			(Construct::NonDeterministic, "a non-deterministic expression"),
			(Construct::OtherRecordRead, "a read of another record"),
			(Construct::TxnControl, "a transaction statement"),
			(Construct::Use, "USE"),
		];
		let regions = [
			(WHERE, "a WHERE clause"),
			(ORDER, "an ORDER BY key"),
			(GROUP, "a GROUP BY key"),
			(SPLIT, "a SPLIT clause"),
			(RegionKind::PermissionFrame, "a permission"),
			(RegionKind::StoredClause(StoredRole::Permissions), "a permission"),
			(RegionKind::FieldFrame(FieldRole::Assert), "an ASSERT clause"),
			(RegionKind::StoredClause(StoredRole::Assert), "an ASSERT clause"),
			(RegionKind::FieldFrame(FieldRole::Value), "a VALUE clause"),
			(RegionKind::StoredClause(StoredRole::Value), "a VALUE clause"),
			(RegionKind::FieldFrame(FieldRole::Default), "a DEFAULT clause"),
			(RegionKind::StoredClause(StoredRole::Default), "a DEFAULT clause"),
			(RegionKind::FieldFrame(FieldRole::Computed), "a COMPUTED clause"),
			(RegionKind::StoredClause(StoredRole::Computed), "a COMPUTED clause"),
			(RegionKind::StoredClause(StoredRole::IndexColumn), "an index definition"),
			(RegionKind::ValueBlock, "an expression"),
		];
		for (construct, construct_text) in constructs {
			for (region, region_text) in regions {
				let mut rejection = found.clone();
				rejection.construct = construct;
				rejection.region = region;
				rejection.loc.stmt = 4;
				assert_eq!(
					rejection.message(),
					format!("{construct_text} is not allowed in {region_text} (statement 5)")
				);
				assert_eq!(
					rejection.error().to_string(),
					format!(
						"Invalid statement: {construct_text} is not allowed in {region_text} \
						 (statement 5)"
					)
				);
			}
		}
	}
}

// ---------------------------------------------------------------------------------------------
// The request binder
// ---------------------------------------------------------------------------------------------

/// Tests that bind a request. They run statements through the parser and the expression binder,
/// so they are built with the in-memory datastore a test of the planner is built with.
#[cfg(feature = "kv-mem")]
mod binding {
	use super::*;
	use crate::exec::ir::build::logical::{
		DefinitionRef, bind_request, bind_request_checked, bind_value_statement, enter_row_region,
		inline_of, permissions_of,
	};
	use crate::exec::ir::core::block::BlockData;
	use crate::exec::ir::core::ir::{Walk, WalkOrder};
	use crate::exec::ir::core::value::ValueDef;
	use crate::exec::ir::error::IrError;
	use crate::exec::ir::expr::ops::{NestedStrategy, RaiseKind};
	use crate::exec::ir::iface::conversion::EliminationReason;
	use crate::exec::ir::iface::region_kind::{Construct, LoopRule, ReturnRule, Section4bRow};
	use crate::exec::ir::logical::constraints::RegionRejection;
	use crate::exec::ir::logical::ddl::{DdlForm, DdlOperand, DdlStep, guard_order};
	use crate::exec::ir::logical::ddl_define::template_holes;
	use crate::exec::ir::logical::ops::DecisiveKind;
	use crate::exec::ir::logical::seam::{BindCx, ExprHost, LoopSite, NameRef};
	use crate::exec::ir::{RequestInput, RequestStatement, UnitKind};
	use crate::exec::vm::payload::Refusal;
	use crate::expr::logical::{
		EvalMode, GuardKind, LogicalExpr, LogicalOp, LogicalPermissionFilter, LogicalSource,
		SourceRef,
	};

	fn request_scope() -> IrScope {
		IrScope::Request {
			fragment: None,
		}
	}

	/// Binds `sql` over `catalog`, and answers the container the binder built.
	async fn bind_over(sql: &str, catalog: &dyn SemanticCatalog) -> Ir {
		let env = Env::new();
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(&request_statements, request_scope());
		let cx = EffectCx {
			registry: &env.registry,
			catalog,
			frames: &env.frames,
		};
		bind_request(&input, &cx).await.expect("the request binds")
	}

	/// Binds `sql` as a request, and answers the container the binder built.
	async fn bind(sql: &str) -> Ir {
		use crate::dbs::Capabilities;
		let env = Env::new();
		let capabilities = Capabilities::all();
		let parsed = crate::syn::parse_with_capabilities(
			sql,
			&capabilities,
			&crate::syn::ParserConfig::default(),
		)
		.expect("the request parses")
		.expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(&request_statements, request_scope());
		bind_request(&input, &env.cx()).await.expect("the request binds")
	}

	/// Every op of `ir`, in pre-order from the top region.
	fn all_ops(ir: &Ir) -> Vec<OpId> {
		let mut out = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			out.push(op);
			Walk::Continue
		});
		out
	}

	fn kind_of(ir: &Ir, op: OpId) -> &OpKind {
		&ir.op(op).expect("the op exists").kind
	}

	/// The mode of every transaction of `ir`, in pre-order.
	fn txn_modes(ir: &Ir) -> Vec<TxnRegionMode> {
		all_ops(ir)
			.into_iter()
			.filter_map(|op| {
				let OpKind::Logical(LogOp::Txn {
					mode,
				}) = kind_of(ir, op)
				else {
					return None;
				};
				Some(*mode)
			})
			.collect()
	}

	/// The commit mode of every statement of `ir`, in pre-order.
	fn stmt_commits(ir: &Ir) -> Vec<StatementCommit> {
		all_ops(ir)
			.into_iter()
			.filter_map(|op| {
				let OpKind::Logical(LogOp::Stmt {
					commit,
					..
				}) = kind_of(ir, op)
				else {
					return None;
				};
				Some(*commit)
			})
			.collect()
	}

	/// The kind of every `expr.raise` of `ir`, in pre-order.
	fn raises(ir: &Ir) -> Vec<RaiseKind> {
		all_ops(ir)
			.into_iter()
			.filter_map(|op| {
				let OpKind::Expr(ExprOp::Raise(kind)) = kind_of(ir, op) else {
					return None;
				};
				Some((**kind).clone())
			})
			.collect()
	}

	fn explicit(end: TxnRegionEnd) -> TxnRegionMode {
		TxnRegionMode::Explicit {
			start: TxnStart::Begin,
			end,
			emission: Emission::Stream,
		}
	}

	/// A block binds as a transaction holding a statement per statement, `BEGIN`, `COMMIT` and
	/// `CANCEL` included. Statements outside a block are no transaction, and a transaction
	/// control that cannot act binds as the raise of its error.
	#[tokio::test]
	async fn txn_grouping() {
		let ir = bind("BEGIN; RETURN 1; COMMIT").await;
		assert_eq!(txn_modes(&ir), vec![explicit(TxnRegionEnd::Commit)]);
		assert_eq!(stmt_commits(&ir), vec![StatementCommit::InTxn; 3]);
		assert!(raises(&ir).is_empty());

		let ir = bind("BEGIN; RETURN 1; CANCEL").await;
		assert_eq!(txn_modes(&ir), vec![explicit(TxnRegionEnd::Cancel)]);
		assert_eq!(stmt_commits(&ir), vec![StatementCommit::InTxn; 3]);

		let ir = bind("BEGIN; RETURN 1").await;
		assert_eq!(txn_modes(&ir), vec![explicit(TxnRegionEnd::Unterminated)]);
		assert_eq!(stmt_commits(&ir), vec![StatementCommit::InTxn; 2]);

		let ir = bind("RETURN 1; RETURN 2").await;
		assert!(txn_modes(&ir).is_empty());
		assert_eq!(stmt_commits(&ir), vec![StatementCommit::Auto; 2]);

		let ir = bind("COMMIT").await;
		assert!(txn_modes(&ir).is_empty());
		assert_eq!(stmt_commits(&ir), vec![StatementCommit::Auto]);
		assert_eq!(raises(&ir), vec![RaiseKind::Misplaced(MisplacedControl::CommitOutsideBlock)]);

		let ir = bind("BEGIN; BEGIN; RETURN 1; COMMIT; RETURN 2").await;
		assert_eq!(txn_modes(&ir), vec![explicit(TxnRegionEnd::Commit)]);
		assert_eq!(
			stmt_commits(&ir),
			[vec![StatementCommit::InTxn; 4], vec![StatementCommit::Auto]].concat()
		);
		assert_eq!(raises(&ir), vec![RaiseKind::Misplaced(MisplacedControl::NestedBegin)]);
	}

	/// A statement that follows a terminator is bound. A refusal among such statements is the
	/// error of the whole statement that holds them, which binds as that raise; otherwise the
	/// statements are erased and recorded as unreachable.
	#[tokio::test]
	async fn unreachable_statements_are_bound() {
		let ir = bind("{ RETURN 1; LET $auth = 2 }").await;
		assert_eq!(
			raises(&ir),
			vec![RaiseKind::Refused(Box::new(Refusal::ProtectedParam {
				name: "auth".to_owned()
			}))]
		);
		assert!(
			all_ops(&ir)
				.into_iter()
				.all(|op| !matches!(kind_of(&ir, op), OpKind::Logical(LogOp::Block))),
			"the statement is replaced by the raise"
		);

		let ir = bind("{ RETURN 1; LET $x = 2 }").await;
		assert!(raises(&ir).is_empty());
		assert!(
			all_ops(&ir)
				.into_iter()
				.any(|op| matches!(kind_of(&ir, op), OpKind::Logical(LogOp::Block))),
			"the block stands"
		);
		assert!(
			all_ops(&ir)
				.into_iter()
				.all(|op| !matches!(kind_of(&ir, op), OpKind::Logical(LogOp::Let(_)))),
			"the statement after the RETURN is erased"
		);
		assert!(!ir.eliminated().is_empty());
		assert!(
			ir.eliminated().iter().all(|(_, reason)| *reason == EliminationReason::Unreachable)
		);
	}

	/// The `expr.raise` of a protected `LET` planned with its top-level statement is the whole
	/// statement's, raised before any of it runs: nothing the statement holds is bound, a `LET` in
	/// an `IF` arm that never runs included. The statements around it bind as they would alone.
	#[tokio::test]
	async fn an_eager_refusal_replaces_its_statement() {
		let refused = || {
			vec![RaiseKind::Refused(Box::new(Refusal::ProtectedParam {
				name: "auth".to_owned(),
			}))]
		};
		for sql in [
			"{ CREATE marker:one; LET $auth = 1; }",
			"FOR $i IN [1, 2] { CREATE marker:two; LET $auth = $i; }",
			"IF true { CREATE marker:three; LET $auth = 1 } ELSE { NONE }",
			"IF false { LET $auth = 1 }",
			"{ CREATE marker:four; { LET $auth = 1; }; }",
		] {
			let ir = bind(sql).await;
			assert_eq!(raises(&ir), refused(), "`{sql}`");
			for tag in [LogTag::Write, LogTag::Block, LogTag::If, LogTag::Loop] {
				assert!(ops_of(&ir, tag).is_empty(), "`{sql}` binds a {tag:?}");
			}
		}
		let ir = bind("IF false { LET $auth = 1 }; RETURN 2;").await;
		assert_eq!(raises(&ir), refused());
		assert_eq!(ops_of(&ir, LogTag::Return).len(), 1);
	}

	/// A protected `LET` after a statement that may change the catalog is planned when its list
	/// reaches it: the raise stands at the `LET`, after the statements before it, and a `LET`
	/// that is never reached raises nothing.
	#[tokio::test]
	async fn a_late_refusal_stands_at_its_statement() {
		let ir = bind("{ DEFINE TABLE later; CREATE marker:five; LET $auth = 1; }").await;
		assert_eq!(raises(&ir).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Define).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Block).len(), 1);

		let ir = bind("{ DEFINE TABLE later; RETURN 1; LET $auth = 1; }").await;
		assert!(raises(&ir).is_empty());
		assert_eq!(ops_of(&ir, LogTag::Define).len(), 1);
	}

	/// Every op of tag `tag` in `ir`, in pre-order.
	fn ops_of(ir: &Ir, tag: LogTag) -> Vec<OpId> {
		all_ops(ir)
			.into_iter()
			.filter(|op| matches!(kind_of(ir, *op), OpKind::Logical(log) if log.tag() == tag))
			.collect()
	}

	/// The tags of the logical ops in the first region of `owner`, in order.
	fn body_tags(ir: &Ir, owner: OpId) -> Vec<LogTag> {
		let data = ir.op(owner).expect("the op exists");
		let region = ir
			.region(*data.regions.first().expect("the op owns a region"))
			.expect("the region exists");
		let block = ir
			.block(*region.blocks.first().expect("the region holds a block"))
			.expect("the block exists");
		block
			.ops
			.iter()
			.filter_map(|op| match kind_of(ir, *op) {
				OpKind::Logical(log) => Some(log.tag()),
				OpKind::Expr(_) | OpKind::Phys(_) => None,
			})
			.collect()
	}

	/// The kinds of the regions `owner` holds, in order.
	fn region_kinds_of(ir: &Ir, owner: OpId) -> Vec<RegionKind> {
		ir.op(owner)
			.expect("the op exists")
			.regions
			.iter()
			.map(|region| ir.region(*region).expect("the region exists").kind)
			.collect()
	}

	/// The tags of the logical ops of the statement region of the statement at `index`.
	fn statement_tags(ir: &Ir, index: usize) -> Vec<LogTag> {
		let statements = ops_of(ir, LogTag::Stmt);
		body_tags(ir, statements[index])
	}

	/// A `SELECT` reads its source through a loop of kind `Select` whose body holds the per-row
	/// nodes.
	#[tokio::test]
	async fn select_reads_its_source_through_a_loop() {
		let ir = bind("SELECT * FROM t").await;
		let loops = ops_of(&ir, LogTag::Loop);
		assert_eq!(loops.len(), 1);
		let OpKind::Logical(LogOp::Loop {
			kind,
			sink,
			order,
			..
		}) = kind_of(&ir, loops[0])
		else {
			panic!("the op is a loop");
		};
		assert_eq!(*kind, LoopKind::Select);
		assert_eq!(*sink, LoopSink::Output);
		assert_eq!(*order, SourceOrder::Delivery);
		assert_eq!(region_kinds_of(&ir, loops[0]), vec![RegionKind::Loop]);
		assert_eq!(body_tags(&ir, loops[0]), vec![LogTag::Project, LogTag::Yield]);
		assert_eq!(statement_tags(&ir, 0), vec![LogTag::Source, LogTag::Loop, LogTag::Yield]);
	}

	/// The nodes that need every row consume the output of the loop below them, and the per-row
	/// nodes above them are the body of a loop of their own. `LIMIT` and `START` are operands
	/// bound before the rows they bound are read, `LIMIT` first.
	#[tokio::test]
	async fn breakers_consume_the_output_of_a_loop() {
		let ir = bind("SELECT a FROM t WHERE b = 1 ORDER BY a LIMIT 5 START 2").await;
		let loops = ops_of(&ir, LogTag::Loop);
		assert_eq!(loops.len(), 2);
		assert_eq!(body_tags(&ir, loops[0]), vec![LogTag::Filter, LogTag::Yield]);
		assert_eq!(body_tags(&ir, loops[1]), vec![LogTag::Project, LogTag::Yield]);
		assert_eq!(
			statement_tags(&ir, 0),
			vec![
				LogTag::Source,
				LogTag::Loop,
				LogTag::Sort,
				LogTag::Limit,
				LogTag::Loop,
				LogTag::Yield
			]
		);
		let sorts = ops_of(&ir, LogTag::Sort);
		assert_eq!(region_kinds_of(&ir, sorts[0]), vec![RegionKind::Clause(ClauseRole::OrderKey)]);
		let limits = ops_of(&ir, LogTag::Limit);
		let OpKind::Logical(LogOp::Limit {
			limit,
			start,
		}) = kind_of(&ir, limits[0])
		else {
			panic!("the op is a limit");
		};
		assert_eq!((*limit, *start), (Some(1), Some(2)));
		assert_eq!(ir.op(limits[0]).expect("the op exists").operands.len(), 3);
	}

	/// A node that needs every row directly over its source reads the source through a loop
	/// whose body delivers each row unchanged. The keys of a `GROUP BY` are error scopes and the
	/// fields are projection regions.
	#[tokio::test]
	async fn an_aggregate_reads_its_source_through_a_loop() {
		let ir = bind("SELECT a, count() FROM t GROUP BY a").await;
		let loops = ops_of(&ir, LogTag::Loop);
		assert_eq!(body_tags(&ir, loops[0]), vec![LogTag::Yield]);
		let aggregates = ops_of(&ir, LogTag::Aggregate);
		assert_eq!(aggregates.len(), 1);
		assert_eq!(
			region_kinds_of(&ir, aggregates[0]),
			vec![
				RegionKind::ErrorScope(ErrorScope::ShapeToNone),
				RegionKind::Clause(ClauseRole::Projection),
				RegionKind::Clause(ClauseRole::Projection)
			]
		);
	}

	/// `TIMEOUT` holds the plan it times in its deadline region, and `VERSION` sets the snapshot
	/// of every op beneath it but not its own.
	#[tokio::test]
	async fn timeout_and_version_scope_the_plan_beneath_them() {
		let ir = bind("SELECT * FROM t TIMEOUT 1s").await;
		let timeouts = ops_of(&ir, LogTag::Timeout);
		assert_eq!(timeouts.len(), 1);
		assert_eq!(region_kinds_of(&ir, timeouts[0]), vec![RegionKind::Deadline]);
		assert_eq!(body_tags(&ir, timeouts[0]), vec![LogTag::Source, LogTag::Loop, LogTag::Yield]);

		let ir = bind("SELECT * FROM t VERSION d'2020-01-01T00:00:00Z'").await;
		let scopes = ops_of(&ir, LogTag::VersionScope);
		assert_eq!(scopes.len(), 1);
		let scope = ir.op(scopes[0]).expect("the op exists");
		assert_eq!(scope.operands.len(), 2);
		assert_eq!(scope.snapshot, SnapshotId::new(0));
		let sources = ops_of(&ir, LogTag::Source);
		assert_ne!(ir.op(sources[0]).expect("the op exists").snapshot, SnapshotId::new(0));
	}

	/// The leaves of a plan: a value, the union of several sources, a subquery and `ONLY`.
	#[tokio::test]
	async fn leaves_bind_as_their_ops() {
		let ir = bind("SELECT * FROM [1, 2]").await;
		let values = ops_of(&ir, LogTag::Values);
		assert_eq!(values.len(), 1);
		assert!(matches!(
			kind_of(&ir, values[0]),
			OpKind::Logical(LogOp::Values {
				written: true,
				..
			})
		));

		let ir = bind("SELECT * FROM a, b").await;
		let unions = ops_of(&ir, LogTag::UnionAll);
		assert_eq!(unions.len(), 1);
		assert_eq!(ir.op(unions[0]).expect("the op exists").operands.len(), 2);

		let ir = bind("SELECT * FROM (SELECT * FROM t)").await;
		let subqueries = ops_of(&ir, LogTag::Subquery);
		assert_eq!(subqueries.len(), 1);
		assert_eq!(region_kinds_of(&ir, subqueries[0]), vec![RegionKind::Subquery]);

		let ir = bind("SELECT * FROM ONLY t").await;
		assert_eq!(ops_of(&ir, LogTag::Only).len(), 1);
	}

	/// A permission filter is bound in the frame its guard names, which is not the frame of the
	/// statement.
	#[test]
	fn a_permission_filter_is_judged_in_its_permission_frame() {
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let plan = LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input: Box::new(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(table("t")),
				schema: None,
				with: None,
				for_update: false,
			})),
			predicate: LogicalExpr::Doc,
			auth: None,
			guard: GuardKind::Write,
		});
		{
			let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
			bind_value_statement(&mut cx, &plan).expect("the plan binds");
		}
		let filters = ops_of(&ir, LogTag::PermissionFilter);
		assert_eq!(filters.len(), 1);
		assert_eq!(region_kinds_of(&ir, filters[0]), vec![RegionKind::PermissionFrame]);
		let filter = ir.op(filters[0]).expect("the op exists");
		let frame = ir.frame(filter.frame).expect("the frame exists");
		assert_eq!(frame.eval.mode, EvalMode::MutablePermission);
		let sources = ops_of(&ir, LogTag::Source);
		assert_ne!(filter.frame, ir.op(sources[0]).expect("the op exists").frame);
	}

	/// An `IF` that holds only its `ELSE` arm, its conditions settled, binds as the `IF` of that
	/// arm, whose two regions are those of an `IF` at the position it stands in.
	#[test]
	fn an_if_holding_only_its_else_arm_binds_as_an_if_region() {
		let env = Env::new();
		// The value binds as the top of a unit, which takes the `RETURN` that ends it.
		let mut ir = Ir::new(request_scope(), RegionKind::Subquery);
		let mut facts = FactCache::new();
		let arm = LogicalOp::Sequence(crate::expr::logical::LogicalSequence {
			stmts: vec![LogicalOp::Return(crate::expr::logical::LogicalReturn {
				what: Box::new(LogicalOp::Eval(crate::expr::logical::LogicalEval {
					expr: LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Integer(1)),
				})),
				fetch: Vec::new(),
			})],
		});
		let plan = LogicalOp::If(crate::expr::logical::LogicalIfOp::new(Vec::new(), Some(arm)));
		{
			let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
			bind_value_statement(&mut cx, &plan).expect("the plan binds");
		}
		let ifs = ops_of(&ir, LogTag::If);
		assert_eq!(ifs.len(), 1);
		assert_eq!(region_kinds_of(&ir, ifs[0]), vec![RegionKind::ValueIf, RegionKind::ValueIf]);
	}

	/// A write binds as the loop over its subjects immediately before the write, which consumes the
	/// loop's result. The loop body holds one stage op per stage of the form's order with the
	/// mark the stage's code decides, the data clauses are regions of the stage that evaluates
	/// them, and the condition is the region of the condition stage.
	#[tokio::test]
	async fn a_write_is_a_loop_then_the_write() {
		let ir = bind("UPDATE t SET a = 1, b += 2 WHERE c = 3 RETURN a").await;
		let writes = ops_of(&ir, LogTag::Write);
		assert_eq!(writes.len(), 1);
		let write = ir.op(writes[0]).expect("the write exists");
		let OpKind::Logical(LogOp::Write(attrs)) = &write.kind else {
			panic!("the op is a write");
		};
		assert_eq!(attrs.form, WriteForm::Update);
		assert_eq!(attrs.output, OutputShape::Fields);
		assert_eq!(
			region_kinds_of(&ir, writes[0]),
			vec![RegionKind::Clause(ClauseRole::ReturnClause)]
		);

		let block = ir.block(write.parent).expect("the block exists");
		let at =
			block.ops.iter().position(|op| *op == writes[0]).expect("the write is in its block");
		let subject_loop = block.ops[at - 1];
		assert_eq!(ir.op(subject_loop).expect("the loop exists").results[0], write.operands[0]);
		let OpKind::Logical(LogOp::Loop {
			kind,
			sink,
			order,
			..
		}) = kind_of(&ir, subject_loop)
		else {
			panic!("the write consumes a loop");
		};
		assert_eq!(*kind, LoopKind::Write);
		assert_eq!(*order, SourceOrder::Delivery);
		let facts = WriteFacts {
			form: PlanForm::Update,
			has_data: true,
			on_duplicate_key_update: false,
			ignore: false,
			repeatable: false,
		};
		let run = stage_run(PlanForm::Update, facts);
		assert_eq!(*sink, LoopSink::Write(Box::new(WriteStages(run.clone().into_boxed_slice()))));

		let body = ir.op(subject_loop).expect("the loop exists").regions[0];
		let body =
			ir.block(ir.region(body).expect("the region exists").blocks[0]).expect("the block");
		let stages: Vec<OpId> = body
			.ops
			.iter()
			.copied()
			.filter(|op| matches!(kind_of(&ir, *op), OpKind::Logical(LogOp::WriteStage { .. })))
			.collect();
		assert_eq!(stages.len(), run.len());
		for (op, stage) in stages.iter().zip(&run) {
			let OpKind::Logical(LogOp::WriteStage {
				stage: bound,
				mark,
			}) = kind_of(&ir, *op)
			else {
				panic!("the op is a stage");
			};
			assert_eq!(bound, stage);
			assert_eq!(*mark, static_mark(*stage));
			let hosted = match stage {
				WriteStageKind::ComputeInputData => vec![RegionKind::Clause(ClauseRole::SetValue)],
				WriteStageKind::CheckWhereCondition => {
					vec![RegionKind::Clause(ClauseRole::WriteCond)]
				}
				WriteStageKind::CheckRecordUserAccess
				| WriteStageKind::CheckRecordExists
				| WriteStageKind::CheckTableNotView
				| WriteStageKind::CheckTableNotLightweight(_)
				| WriteStageKind::CheckTableType(_)
				| WriteStageKind::CheckPermissionsQuickCreate
				| WriteStageKind::CheckUpdatePermissions
				| WriteStageKind::CheckDeletePermissions
				| WriteStageKind::CheckCreatePermissions
				| WriteStageKind::RecheckUpdatePermissions
				| WriteStageKind::CheckDataFields
				| WriteStageKind::ProcessRecordData
				| WriteStageKind::ProcessMergeData
				| WriteStageKind::DefaultRecordData
				| WriteStageKind::GenerateRecordId
				| WriteStageKind::ProcessTableFields
				| WriteStageKind::CleanupTableFields
				| WriteStageKind::CleanupTableReferences
				| WriteStageKind::ClearRecordData
				| WriteStageKind::StoreRecordData
				| WriteStageKind::StoreEdgesData
				| WriteStageKind::StoreInlineAdjacencyData
				| WriteStageKind::StoreIndexData
				| WriteStageKind::ReclaimDocId
				| WriteStageKind::PurgeRecordData
				| WriteStageKind::MaterialiseObservedFields
				| WriteStageKind::ProcessTableReferences
				| WriteStageKind::ProcessTableViews(_)
				| WriteStageKind::ProcessTableEvents(_)
				| WriteStageKind::ProcessTableLives(_)
				| WriteStageKind::ProcessChangefeeds
				| WriteStageKind::CheckSelectPermissions(_)
				| WriteStageKind::OutputWrite => vec![],
			};
			assert_eq!(region_kinds_of(&ir, *op), [vec![RegionKind::WriteStage], hosted].concat());
			assert_eq!(body_tags(&ir, *op), vec![LogTag::Yield], "{stage:?} holds no stored code");
		}
		let data = stages[run.iter().position(|s| *s == WriteStageKind::ComputeInputData).unwrap()];
		let region = ir.op(data).expect("the stage exists").regions[1];
		let region = ir.region(region).expect("the region exists");
		let assigns = ir
			.block(region.blocks[0])
			.expect("the block")
			.ops
			.iter()
			.filter(|op| matches!(kind_of(&ir, **op), OpKind::Logical(LogOp::Assign(_))));
		assert_eq!(assigns.count(), 2);
	}

	/// A catalog that answers one view for every table whatever it answers of the table itself.
	struct Disagreeing {
		table: fn() -> CatalogAnswer<Arc<TableSnapshot>>,
		views: Arc<[crate::catalog::TableDefinition]>,
	}

	impl SemanticCatalog for Disagreeing {
		fn table(&self, _name: &TableName) -> CatalogAnswer<Arc<TableSnapshot>> {
			(self.table)()
		}

		fn views(
			&self,
			_table: &TableName,
		) -> CatalogAnswer<Arc<[crate::catalog::TableDefinition]>> {
			CatalogAnswer::Found(Arc::clone(&self.views))
		}
	}

	/// The views a write maintains are stated by the write's own facts: a table the catalog does
	/// not define, or cannot describe, feeds none, whatever the catalog answers of its views, so
	/// the stage hosts no clause region its facts do not state.
	#[tokio::test]
	async fn the_view_clauses_of_a_write_follow_the_facts_it_states() {
		use crate::exec::ir::logical::ops::{Stored, WriteViews};
		use crate::exec::ir::logical::stages::view_table_for_test;

		let views: Arc<[crate::catalog::TableDefinition]> =
			Arc::from(vec![view_table_for_test("v", "a", Some("a > 1"), None)]);
		for (table, expected) in [
			(
				(|| CatalogAnswer::Absent) as fn() -> CatalogAnswer<Arc<TableSnapshot>>,
				Stored::Nothing,
			),
			(|| CatalogAnswer::NotLoaded, Stored::LateBound),
			(|| CatalogAnswer::Unknown, Stored::LateBound),
		] {
			let catalog = Disagreeing {
				table,
				views: Arc::clone(&views),
			};
			let ir = bind_over("UPDATE t SET a = 1", &catalog).await;
			let writes = ops_of(&ir, LogTag::Write);
			let OpKind::Logical(LogOp::Write(attrs)) = kind_of(&ir, writes[0]) else {
				panic!("the op is a write");
			};
			assert_eq!(attrs.stored.views, expected);
			assert_eq!(attrs.stored.view_set, WriteViews::default());
			for op in ops_of(&ir, LogTag::WriteStage) {
				let OpKind::Logical(LogOp::WriteStage {
					stage,
					..
				}) = kind_of(&ir, op)
				else {
					panic!("the op is a stage");
				};
				if matches!(stage, WriteStageKind::ProcessTableViews(_)) {
					assert_eq!(region_kinds_of(&ir, op), [RegionKind::WriteStage]);
				}
			}
		}
	}

	/// The repairs of a delete are stated by the write's own facts: a delete of a table the catalog
	/// does not define is repaired by no field, one of a table it cannot describe states them
	/// late-bound, and a write that purges nothing states none, so the stage that purges hosts no
	/// clause region its facts do not state.
	#[tokio::test]
	async fn the_repairs_of_a_write_follow_the_facts_it_states() {
		use crate::exec::ir::logical::ops::{Stored, WriteRepairs};

		for (table, expected) in [
			(
				(|| CatalogAnswer::Absent) as fn() -> CatalogAnswer<Arc<TableSnapshot>>,
				Stored::Nothing,
			),
			(|| CatalogAnswer::NotLoaded, Stored::LateBound),
			(|| CatalogAnswer::Unknown, Stored::LateBound),
		] {
			let catalog = Disagreeing {
				table,
				views: Arc::from(Vec::new()),
			};
			for (sql, expected) in [("DELETE t", expected), ("UPDATE t SET a = 1", Stored::Nothing)]
			{
				let ir = bind_over(sql, &catalog).await;
				let writes = ops_of(&ir, LogTag::Write);
				let OpKind::Logical(LogOp::Write(attrs)) = kind_of(&ir, writes[0]) else {
					panic!("the op is a write");
				};
				assert_eq!(attrs.stored.referenced, expected, "`{sql}`");
				assert_eq!(attrs.stored.repair_set, WriteRepairs::default());
				for op in ops_of(&ir, LogTag::WriteStage) {
					let OpKind::Logical(LogOp::WriteStage {
						stage,
						..
					}) = kind_of(&ir, op)
					else {
						panic!("the op is a stage");
					};
					if *stage == WriteStageKind::PurgeRecordData {
						assert_eq!(region_kinds_of(&ir, op), [RegionKind::WriteStage]);
					}
				}
			}
		}
	}

	/// The subjects of a creating write are the targets gathered in an array, `RELATE`'s as
	/// `[from, through, to]`, and a write of rows reads the plan below it in delivery order.
	#[tokio::test]
	async fn write_subjects_are_one_operand() {
		for (sql, operands, order) in [
			("CREATE a, b", 2, SourceOrder::Positional),
			("RELATE a:1->likes->b:2", 3, SourceOrder::Positional),
			("DELETE t", 0, SourceOrder::Delivery),
		] {
			let ir = bind(sql).await;
			let loops: Vec<OpId> = ops_of(&ir, LogTag::Loop)
				.into_iter()
				.filter(|op| {
					matches!(
						kind_of(&ir, *op),
						OpKind::Logical(LogOp::Loop {
							kind: LoopKind::Write,
							..
						})
					)
				})
				.collect();
			assert_eq!(loops.len(), 1, "`{sql}`");
			let OpKind::Logical(LogOp::Loop {
				order: bound,
				..
			}) = kind_of(&ir, loops[0])
			else {
				panic!("the op is a loop");
			};
			assert_eq!(*bound, order, "`{sql}`");
			if operands > 0 {
				let source = ir.op(loops[0]).expect("the loop exists").operands[0];
				let array = all_ops(&ir)
					.into_iter()
					.find(|op| ir.op(*op).is_some_and(|data| data.results.contains(&source)))
					.expect("the source has a definition");
				assert!(matches!(kind_of(&ir, array), OpKind::Expr(ExprOp::Array)), "`{sql}`");
				assert_eq!(
					ir.op(array).expect("the array exists").operands.len(),
					operands,
					"`{sql}`"
				);
			}
		}
	}

	/// A `TIMEOUT` is the `logical.timeout` that holds the whole write, and the write carries no
	/// duration of its own.
	#[tokio::test]
	async fn a_write_timeout_is_the_op_around_it() {
		let ir = bind("CREATE t TIMEOUT 1s").await;
		let timeouts = ops_of(&ir, LogTag::Timeout);
		assert_eq!(timeouts.len(), 1);
		let writes = ops_of(&ir, LogTag::Write);
		assert_eq!(writes.len(), 1);
		let timed =
			ir.region(ir.op(timeouts[0]).expect("the op exists").regions[0]).expect("region");
		let block = ir.block(timed.blocks[0]).expect("the block exists");
		assert!(block.ops.contains(&writes[0]), "the write stands in the timeout's region");
	}

	/// An `INSERT` of a literal list is a few ops, and the loop of one holds no row of its own.
	#[tokio::test]
	async fn insert_rows_are_one_value() {
		let ir = bind("INSERT INTO t [{ id: 1 }, { id: 2 }, { id: 3 }]").await;
		let writes = ops_of(&ir, LogTag::Write);
		assert_eq!(writes.len(), 1);
		let write = ir.op(writes[0]).expect("the write exists");
		assert_eq!(write.operands.len(), 2, "the loop's result, then the INTO table");
	}

	/// A place binds a subscript as an operand, a number the statement wrote as an index, and a
	/// filter as a region of the op that carries the place.
	#[tokio::test]
	async fn a_place_binds_what_it_addresses() {
		let ir = bind("UPDATE t SET a.b = 1, c[0] = 2, d[$i] = 3, e[WHERE x > 1] = 4").await;
		let places: Vec<Vec<PlacePart>> = ops_of(&ir, LogTag::Assign)
			.into_iter()
			.map(|op| match kind_of(&ir, op) {
				OpKind::Logical(LogOp::Assign(attrs)) => attrs.place.parts.to_vec(),
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
					panic!("the op is an assignment")
				}
			})
			.collect();
		assert_eq!(
			places,
			[
				vec![PlacePart::Field(strand("a")), PlacePart::Field(strand("b"))],
				vec![PlacePart::Field(strand("c")), PlacePart::Index(1)],
				vec![PlacePart::Field(strand("d")), PlacePart::Dynamic(1)],
				vec![PlacePart::Field(strand("e")), PlacePart::Filter(0)],
			]
		);
		let filtered = ops_of(&ir, LogTag::Assign)[3];
		assert_eq!(ir.op(filtered).expect("the op exists").regions.len(), 1);
	}

	/// A subscript of a place is evaluated with no record in scope: a field it names is a read of
	/// `NONE`, not of the record the clause runs against.
	#[tokio::test]
	async fn a_subscript_of_a_place_reads_no_row() {
		let ir = bind("UPDATE t SET a[b] = 1").await;
		let read = all_ops(&ir)
			.into_iter()
			.find(
				|op| matches!(kind_of(&ir, *op), OpKind::Expr(ExprOp::Field { name }) if name.as_str() == "b"),
			)
			.expect("the subscript reads the field");
		let row = ir.op(read).expect("the op exists").operands[0];
		let ValueDef::OpResult {
			op: defined,
			..
		} = ir.value(row).expect("the row has a definition").def
		else {
			panic!("the row is a block argument");
		};
		assert!(
			matches!(kind_of(&ir, defined), OpKind::Expr(ExprOp::Literal(value)) if matches!(**value, Value::None)),
			"the row of a subscript is NONE"
		);
	}

	/// No definition is readable through the empty catalog, so no permission is added to a table
	/// and every call of a definition stays a call.
	#[test]
	fn catalog_hooks_answer_none() {
		let env = Env::new();
		let cx = env.cx();
		assert!(permissions_of(cx.catalog, &table("t")).is_none());
		assert!(inline_of(cx.catalog, &DefinitionRef::Function("f")).is_none());
	}

	/// The future of `bind_request` holds no binder state across a suspension point, so a planner
	/// future that awaits it is `Send`.
	#[test]
	fn bind_request_future_is_send() {
		fn assert_send<T: Send>(_: &T) {}

		let env = Env::new();
		let parsed = crate::syn::parse("RETURN 1").expect("the request parses").expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(&request_statements, request_scope());
		let cx = env.cx();
		let future = bind_request(&input, &cx);
		assert_send(&future);
	}

	/// A container whose scope is not a request is not bound.
	#[tokio::test]
	async fn a_catalog_scope_is_not_a_request() {
		let env = Env::new();
		let request_statements = RequestStatement::of(&[]);
		let input = RequestInput::request(
			&request_statements,
			IrScope::CatalogVersion {
				version: Default::default(),
			},
		);
		assert!(matches!(bind_request(&input, &env.cx()).await, Err(IrError::Verify(_))));
	}

	/// What one step of a schema statement's block is, for the order of its checks.
	#[derive(Clone, Copy, Debug, PartialEq, Eq)]
	enum Mark {
		NoWrite,
		Allow,
		/// A run of ops that evaluate operands the statement does not write literally.
		Evaluated,
		Statement,
	}

	/// The ops of the block of the statement region at `index`.
	fn statement_ops(ir: &Ir, index: usize) -> Vec<OpId> {
		let statements = ops_of(ir, LogTag::Stmt);
		let region = *ir
			.op(statements[index])
			.expect("the statement exists")
			.regions
			.first()
			.expect("the statement owns a region");
		let block = *ir
			.region(region)
			.expect("the region exists")
			.blocks
			.first()
			.expect("the region holds a block");
		ir.block(block).expect("the block exists").ops.clone()
	}

	/// The order of the checks, the operands the statement evaluates and the statement's own op in
	/// `ops`. A literal is an operand the statement writes, which has no position of its own, and
	/// a consecutive run of evaluated operands is one mark.
	fn marks_of(ir: &Ir, ops: &[OpId]) -> Vec<Mark> {
		let mut out: Vec<Mark> = Vec::new();
		for op in ops {
			let mark = match kind_of(ir, *op) {
				OpKind::Logical(log) => {
					if matches!(
						log,
						LogOp::DdlGuard {
							guard: DdlGuard::RefuseNoWrite,
						}
					) {
						Mark::NoWrite
					} else if matches!(
						log,
						LogOp::DdlGuard {
							guard: DdlGuard::Allow { .. },
						}
					) {
						Mark::Allow
					} else if matches!(
						log,
						LogOp::Define(_) | LogOp::Remove(_) | LogOp::Alter(_) | LogOp::Rebuild(_)
					) {
						Mark::Statement
					} else if matches!(log, LogOp::Yield) {
						continue;
					} else {
						Mark::Evaluated
					}
				}
				OpKind::Expr(ExprOp::Literal(_)) => continue,
				OpKind::Expr(_) | OpKind::Phys(_) => Mark::Evaluated,
			};
			if mark == Mark::Evaluated && out.last() == Some(&Mark::Evaluated) {
				continue;
			}
			out.push(mark);
		}
		out
	}

	/// The marks `guard_order(form)` states for a statement that evaluates the operands `roles`.
	fn expected_marks(form: DdlForm, roles: &[DdlOperand]) -> Vec<Mark> {
		let mut out: Vec<Mark> = Vec::new();
		for step in guard_order(form) {
			let mark = match step {
				DdlStep::RefuseNoWrite => Mark::NoWrite,
				DdlStep::Allow => Mark::Allow,
				DdlStep::Statement => Mark::Statement,
				DdlStep::Operand(role) if roles.contains(role) => Mark::Evaluated,
				DdlStep::Operand(_) => continue,
			};
			if mark == Mark::Evaluated && out.last() == Some(&Mark::Evaluated) {
				continue;
			}
			out.push(mark);
		}
		out
	}

	/// One statement of each form `DEFINE`, `REMOVE`, `ALTER` and `REBUILD` have a text for, with
	/// every operand written. `DEFINE MODEL` has no text form (a model is stored by an import), and
	/// the `MODULE` forms and `REMOVE MODEL` are not written here: the grammar admits the module
	/// statements only with Surrealism modules enabled.
	const WRITTEN: &[(DdlForm, &str)] = &[
		(DdlForm::DefineNamespace, "DEFINE NAMESPACE n"),
		(DdlForm::DefineDatabase, "DEFINE DATABASE d"),
		(DdlForm::DefineFunction, "DEFINE FUNCTION fn::f() { RETURN 1 }"),
		(DdlForm::DefineAnalyzer, "DEFINE ANALYZER a TOKENIZERS blank"),
		(DdlForm::DefineParam, "DEFINE PARAM $p VALUE 1"),
		(DdlForm::DefineTable, "DEFINE TABLE t SCHEMAFULL"),
		(DdlForm::DefineEvent, "DEFINE EVENT e ON t WHEN true THEN (RETURN 1)"),
		(DdlForm::DefineField, "DEFINE FIELD f ON t TYPE int"),
		(DdlForm::DefineIndex, "DEFINE INDEX ix ON t FIELDS f"),
		(DdlForm::DefineUser, "DEFINE USER u ON DATABASE PASSWORD 'x' ROLES VIEWER"),
		(DdlForm::DefineAccess, "DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'k'"),
		(DdlForm::DefineConfig, "DEFINE CONFIG DEFAULT NAMESPACE n DATABASE d"),
		(DdlForm::DefineApi, "DEFINE API '/p' FOR get THEN { RETURN 1 }"),
		(DdlForm::DefineBucket, "DEFINE BUCKET b BACKEND 'memory'"),
		(DdlForm::DefineSequence, "DEFINE SEQUENCE s"),
		(DdlForm::RemoveNamespace, "REMOVE NAMESPACE n"),
		(DdlForm::RemoveDatabase, "REMOVE DATABASE d"),
		(DdlForm::RemoveFunction, "REMOVE FUNCTION fn::f"),
		(DdlForm::RemoveAnalyzer, "REMOVE ANALYZER a"),
		(DdlForm::RemoveAccess, "REMOVE ACCESS a ON DATABASE"),
		(DdlForm::RemoveParam, "REMOVE PARAM $p"),
		(DdlForm::RemoveTable, "REMOVE TABLE t"),
		(DdlForm::RemoveEvent, "REMOVE EVENT e ON t"),
		(DdlForm::RemoveField, "REMOVE FIELD f ON t"),
		(DdlForm::RemoveIndex, "REMOVE INDEX ix ON t"),
		(DdlForm::RemoveUser, "REMOVE USER u ON DATABASE"),
		(DdlForm::RemoveApi, "REMOVE API '/p'"),
		(DdlForm::RemoveBucket, "REMOVE BUCKET b"),
		(DdlForm::RemoveSequence, "REMOVE SEQUENCE s"),
		(DdlForm::RemoveConfig, "REMOVE CONFIG API"),
		(DdlForm::AlterSystem, "ALTER SYSTEM COMPACT"),
		(DdlForm::AlterNamespace, "ALTER NAMESPACE COMPACT"),
		(DdlForm::AlterDatabase, "ALTER DATABASE COMPACT"),
		(DdlForm::AlterTable, "ALTER TABLE t COMPACT"),
		(DdlForm::AlterApi, "ALTER API '/p' COMMENT 'c'"),
		(DdlForm::AlterEvent, "ALTER EVENT e ON t COMMENT 'c'"),
		(DdlForm::AlterIndex, "ALTER INDEX ix ON t PREPARE REMOVE"),
		(DdlForm::AlterSequence, "ALTER SEQUENCE s TIMEOUT 1s"),
		(DdlForm::AlterField, "ALTER FIELD f ON t TYPE string"),
		(DdlForm::AlterParam, "ALTER PARAM $p VALUE 2"),
		(DdlForm::AlterBucket, "ALTER BUCKET b COMMENT 'c'"),
		(DdlForm::DefineType, "DEFINE TYPE e AS 'a' | 'b' COMMENT 'c'"),
		(DdlForm::RemoveType, "REMOVE TYPE e"),
		(DdlForm::AlterType, "ALTER TYPE e ADD VALUE 'c' AFTER 'a'"),
		(DdlForm::AlterConfig, "ALTER CONFIG API PERMISSIONS NONE"),
		(DdlForm::AlterAnalyzer, "ALTER ANALYZER a COMMENT 'c'"),
		(DdlForm::AlterFunction, "ALTER FUNCTION fn::f($x: int) { RETURN $x }"),
		(DdlForm::AlterUser, "ALTER USER u ON DATABASE COMMENT 'c'"),
		(DdlForm::AlterAccess, "ALTER ACCESS a ON DATABASE COMMENT 'c'"),
		(DdlForm::RebuildIndex, "REBUILD INDEX ix ON t"),
	];

	/// A statement that evaluates some of its operands, the roles it evaluates, in the forms whose
	/// order differs the most: a table resolved before the permission check, names resolved between
	/// the checks, and a value read after both.
	const EVALUATED: &[(DdlForm, &str, &[DdlOperand])] = &[
		(DdlForm::DefineParam, "DEFINE PARAM $p VALUE (1 + 2)", &[DdlOperand::Value]),
		(
			DdlForm::DefineEvent,
			"DEFINE EVENT $name ON $table WHEN true THEN (RETURN 1) COMMENT $comment",
			&[DdlOperand::Name, DdlOperand::Table, DdlOperand::Comment],
		),
		(
			DdlForm::DefineTable,
			"DEFINE TABLE $name COMMENT $comment",
			&[DdlOperand::Name, DdlOperand::Comment],
		),
		(
			DdlForm::DefineField,
			"DEFINE FIELD $name ON $table COMMENT $comment",
			&[DdlOperand::FieldName, DdlOperand::Table, DdlOperand::Comment],
		),
		(
			DdlForm::DefineIndex,
			"DEFINE INDEX $name ON $table FIELDS a COMMENT $comment",
			&[DdlOperand::Name, DdlOperand::Table, DdlOperand::Comment],
		),
		(DdlForm::RemoveTable, "REMOVE TABLE $name", &[DdlOperand::Name]),
		(
			DdlForm::RemoveEvent,
			"REMOVE EVENT $name ON $table",
			&[DdlOperand::Table, DdlOperand::Name],
		),
		(
			DdlForm::RemoveField,
			"REMOVE FIELD $name ON $table",
			&[DdlOperand::Table, DdlOperand::FieldName],
		),
		(DdlForm::AlterDatabase, "ALTER DATABASE $name COMPACT", &[DdlOperand::Name]),
		(DdlForm::AlterTable, "ALTER TABLE $name COMPACT", &[DdlOperand::Name]),
		(
			DdlForm::AlterField,
			"ALTER FIELD $name ON $table TYPE string",
			&[DdlOperand::FieldName, DdlOperand::Table],
		),
		(DdlForm::AlterParam, "ALTER PARAM $p VALUE (1 + 2)", &[DdlOperand::Value]),
	];

	/// The table of each form is the order the lowering of the form takes, and a statement binds
	/// to exactly that order: its checks and the operands it evaluates in the order of
	/// `guard_order`, then its op. A written operand evaluates nothing and no op of a statement
	/// precedes its first check; `DEFINE EVENT` resolves its names between `RefuseNoWrite` and
	/// `Allow`, and `DEFINE PARAM` checks both before it evaluates `VALUE`.
	#[tokio::test]
	async fn ddl_order_is_lower_ddl_order() {
		let mut covered = vec![
			DdlForm::DefineModel,
			DdlForm::DefineModule,
			DdlForm::RemoveModel,
			DdlForm::RemoveModule,
			DdlForm::AlterModule,
		];
		for (form, sql) in WRITTEN {
			covered.push(*form);
			let ir = bind(sql).await;
			let ops = statement_ops(&ir, 0);
			assert_eq!(marks_of(&ir, &ops), expected_marks(*form, &[]), "`{sql}`");
			assert!(
				matches!(kind_of(&ir, ops[0]), OpKind::Logical(LogOp::DdlGuard { .. })),
				"`{sql}` binds an op before its first check"
			);
		}
		for form in DdlForm::ALL {
			assert!(covered.contains(&form), "{form:?} has no statement");
		}
		for (form, sql, roles) in EVALUATED {
			let ir = bind(sql).await;
			let ops = statement_ops(&ir, 0);
			assert_eq!(marks_of(&ir, &ops), expected_marks(*form, roles), "`{sql}`");
		}
		assert_eq!(
			expected_marks(DdlForm::DefineEvent, &[DdlOperand::Name, DdlOperand::Table]),
			[Mark::NoWrite, Mark::Evaluated, Mark::Allow, Mark::Statement]
		);
		assert_eq!(
			expected_marks(DdlForm::DefineParam, &[DdlOperand::Value]),
			[Mark::NoWrite, Mark::Allow, Mark::Evaluated, Mark::Statement]
		);
		assert_eq!(
			expected_marks(DdlForm::DefineTable, &[DdlOperand::Name]),
			[Mark::Evaluated, Mark::NoWrite, Mark::Allow, Mark::Statement]
		);
	}

	/// The definition of a `DEFINE`, as the lowering builds it from the statement: its stored
	/// type, the fields an operand fills when the statement runs, and a `StoredClause` region for
	/// each clause it stores.
	#[tokio::test]
	async fn definition_templates_match_main() {
		let comment = TemplateHole::Comment;
		let forms: &[(&str, DdlForm, &str, &[TemplateHole])] = &[
			(
				"DEFINE NAMESPACE n",
				DdlForm::DefineNamespace,
				"NamespaceDefinition",
				&[TemplateHole::Other("namespace_id"), TemplateHole::Name, comment],
			),
			(
				"DEFINE DATABASE d",
				DdlForm::DefineDatabase,
				"DatabaseDefinition",
				&[
					TemplateHole::Other("namespace_id"),
					TemplateHole::Other("database_id"),
					TemplateHole::Name,
					comment,
				],
			),
			(
				"DEFINE FUNCTION fn::f() { RETURN 1 }",
				DdlForm::DefineFunction,
				"StoredFunctionDefinition",
				&[comment, TemplateHole::AuthLimit],
			),
			(
				"DEFINE ANALYZER a TOKENIZERS blank",
				DdlForm::DefineAnalyzer,
				"AnalyzerDefinition",
				&[TemplateHole::Name, comment],
			),
			(
				"DEFINE PARAM $p VALUE 1",
				DdlForm::DefineParam,
				"StoredParamDefinition",
				&[TemplateHole::Value, comment],
			),
			(
				"DEFINE TABLE t SCHEMAFULL",
				DdlForm::DefineTable,
				"StoredTableDefinition",
				&[
					TemplateHole::Other("namespace_id"),
					TemplateHole::Other("database_id"),
					TemplateHole::Other("table_id"),
					TemplateHole::Name,
					comment,
					TemplateHole::Other("cache_fields_ts"),
					TemplateHole::Other("cache_events_ts"),
					TemplateHole::Other("cache_tables_ts"),
					TemplateHole::Other("cache_indexes_ts"),
					TemplateHole::Other("cache_lives_ts"),
					TemplateHole::Other("graph_folded"),
					TemplateHole::Other("graph_doc_ids"),
					TemplateHole::Other("graph_inline_gen"),
				],
			),
			(
				"DEFINE EVENT e ON t WHEN true THEN (RETURN 1)",
				DdlForm::DefineEvent,
				"StoredEventDefinition",
				&[
					TemplateHole::Name,
					TemplateHole::Other("target_table"),
					comment,
					TemplateHole::AuthLimit,
				],
			),
			(
				"DEFINE FIELD f ON t TYPE int",
				DdlForm::DefineField,
				"StoredFieldDefinition",
				&[TemplateHole::Other("name"), TemplateHole::Other("table"), comment],
			),
			(
				"DEFINE INDEX ix ON t FIELDS f",
				DdlForm::DefineIndex,
				"StoredIndexDefinition",
				&[
					TemplateHole::Name,
					TemplateHole::Other("table_name"),
					comment,
					TemplateHole::Other("cols"),
					TemplateHole::Other("index_id"),
				],
			),
			(
				"DEFINE USER u ON DATABASE PASSWORD 'x' ROLES VIEWER",
				DdlForm::DefineUser,
				"UserDefinition",
				&[
					TemplateHole::Name,
					comment,
					TemplateHole::Other("hash"),
					TemplateHole::Other("code"),
					TemplateHole::Other("scram"),
					TemplateHole::Other("token_duration"),
					TemplateHole::Other("session_duration"),
				],
			),
			(
				"DEFINE SEQUENCE s",
				DdlForm::DefineSequence,
				"SequenceDefinition",
				&[
					TemplateHole::Name,
					TemplateHole::Other("batch"),
					TemplateHole::Other("start"),
					TemplateHole::Other("timeout"),
				],
			),
		];
		for (sql, form, ty, holes) in forms {
			let ir = bind(sql).await;
			let defines = ops_of(&ir, LogTag::Define);
			assert_eq!(defines.len(), 1, "`{sql}`");
			let OpKind::Logical(LogOp::Define(attrs)) = kind_of(&ir, defines[0]) else {
				panic!("`{sql}` binds no define");
			};
			assert_eq!(attrs.template.ty, *ty, "`{sql}`");
			assert_eq!(attrs.template.holes.as_slice(), *holes, "`{sql}`");
			assert_eq!(template_holes(*form), Some(*holes), "`{sql}`");
		}

		let stored = |roles: &[StoredRole]| -> Vec<RegionKind> {
			roles.iter().map(|role| RegionKind::StoredClause(*role)).collect()
		};
		let clauses: &[(&str, Vec<RegionKind>)] = &[
			("DEFINE NAMESPACE n", vec![]),
			("DEFINE FUNCTION fn::f() { RETURN 1 }", stored(&[StoredRole::Body])),
			(
				"DEFINE FUNCTION fn::g() { RETURN 1 } PERMISSIONS WHERE true",
				stored(&[StoredRole::Body, StoredRole::Permissions]),
			),
			("DEFINE PARAM $p VALUE 1", vec![]),
			("DEFINE PARAM $p VALUE 1 PERMISSIONS WHERE true", stored(&[StoredRole::Permissions])),
			(
				"DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select WHERE true",
				stored(&[StoredRole::Permissions]),
			),
			(
				"DEFINE TABLE v AS SELECT a FROM t",
				vec![RegionKind::StoredClause(StoredRole::View), RegionKind::Subquery],
			),
			(
				"DEFINE EVENT e ON t WHEN true THEN (RETURN 1)",
				stored(&[StoredRole::When, StoredRole::Then]),
			),
			(
				"DEFINE FIELD f ON t TYPE int VALUE 1 ASSERT true DEFAULT 2",
				stored(&[StoredRole::Value, StoredRole::Assert, StoredRole::Default]),
			),
			("DEFINE FIELD c ON t COMPUTED { 1 }", stored(&[StoredRole::Computed])),
			("DEFINE INDEX ix ON t FIELDS f", vec![]),
			("DEFINE INDEX ix2 ON t FIELDS time::now()", stored(&[StoredRole::IndexColumn])),
			("DEFINE INDEX ix3 ON t FIELDS id.id().val", vec![]),
			("DEFINE INDEX ix4 ON t FIELDS a[$i].b", vec![]),
			(
				"DEFINE ACCESS a ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user) \
				 DURATION FOR TOKEN 1h, FOR SESSION 1h",
				stored(&[StoredRole::Signin]),
			),
		];
		for (sql, kinds) in clauses {
			let ir = bind(sql).await;
			let defines = ops_of(&ir, LogTag::Define);
			assert_eq!(defines.len(), 1, "`{sql}`");
			assert_eq!(&region_kinds_of(&ir, defines[0]), kinds, "`{sql}`");
		}
	}

	/// A credential of `DEFINE USER` and `ALTER USER` is in no op's text form or `Debug` output:
	/// not the plaintext password, and not the hash, code or verifier derived from it.
	#[tokio::test]
	async fn user_credentials_never_print() {
		// The credential is a text no attribute or field name holds.
		let statements = [
			"DEFINE USER u ON ROOT PASSWORD 'plaintext-7f3a'",
			"DEFINE USER u ON ROOT PASSHASH '$argon2id$plaintext-7f3a' ROLES OWNER",
			"ALTER USER u ON ROOT PASSWORD 'plaintext-7f3a'",
		];
		for sql in statements {
			let ir = bind(sql).await;
			for op in all_ops(&ir) {
				let kind = kind_of(&ir, op);
				let text = format!("{} {kind:?}", printed(kind));
				assert!(!text.contains("plaintext-7f3a"), "`{sql}` prints a credential: {text}");
				assert!(!text.contains("$argon2"), "`{sql}` prints a hash: {text}");
			}
		}
		let ir = bind("DEFINE USER u ON ROOT PASSWORD 'plaintext-7f3a'").await;
		let defines = ops_of(&ir, LogTag::Define);
		let OpKind::Logical(LogOp::Define(attrs)) = kind_of(&ir, defines[0]) else {
			panic!("the statement binds no define");
		};
		let DefineObject::User(user) = &attrs.object else {
			panic!("the statement defines no user");
		};
		assert!(user.password.is_some());
	}

	/// A key of `DEFINE ACCESS` is in no op's text form or `Debug` output: not the verification
	/// key, the JWKS URL or the issuer key, in a JWT method or in the JWT half of a record
	/// method.
	#[tokio::test]
	async fn access_keys_never_print() {
		let statements = [
			"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret-verify-key'",
			"DEFINE ACCESS a ON DATABASE TYPE JWT URL 'https://secret-jwks.example/keys' \
			 WITH ISSUER ALGORITHM HS256 KEY 'secret-issue-key'",
			"DEFINE ACCESS a ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS256 \
			 KEY 'secret-verify-key' WITH ISSUER KEY 'secret-issue-key'",
		];
		for sql in statements {
			let ir = bind(sql).await;
			for op in all_ops(&ir) {
				let kind = kind_of(&ir, op);
				let text = format!("{} {kind:?}", printed(kind));
				assert!(!text.contains("secret-"), "`{sql}` prints a credential: {text}");
			}
		}
	}

	/// The attributes of the loop `op`.
	fn loop_of(ir: &Ir, op: OpId) -> (LoopKind, LoopSink, SourceOrder) {
		let OpKind::Logical(LogOp::Loop {
			kind,
			sink,
			order,
			..
		}) = kind_of(ir, op)
		else {
			panic!("the op is a loop");
		};
		(*kind, sink.clone(), *order)
	}

	/// The first block of the first region of `owner`.
	fn first_block(ir: &Ir, owner: OpId) -> &BlockData {
		let data = ir.op(owner).expect("the op exists");
		let region = ir
			.region(*data.regions.first().expect("the op owns a region"))
			.expect("the region exists");
		ir.block(*region.blocks.first().expect("the region holds a block"))
			.expect("the block exists")
	}

	/// The number of block arguments of the first region of `owner`.
	fn body_args(ir: &Ir, owner: OpId) -> usize {
		first_block(ir, owner).args.len()
	}

	/// Every op of `ir` that `keep` admits, in pre-order.
	fn ops_where(ir: &Ir, keep: impl Fn(&OpKind) -> bool) -> Vec<OpId> {
		all_ops(ir).into_iter().filter(|op| keep(kind_of(ir, *op))).collect()
	}

	/// The error `sql` binds to.
	async fn bind_error(sql: &str) -> IrError {
		let env = Env::new();
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(&request_statements, request_scope());
		bind_request(&input, &env.cx()).await.expect_err("the request does not bind")
	}

	/// Every construct that binds as a loop site states its kind, sink and order. The
	/// body of a loop has the element and the position, and the accumulator only for a fold. The
	/// loop of a closure builtin applies the closure value created before it.
	#[tokio::test]
	async fn loop_sites_per_construct() {
		let decisive = LoopSink::Decisive;
		// The statement, the loop's kind and sink, its operand count and the body's argument count.
		let cases: Vec<(&str, LoopKind, LoopSink, usize, usize)> = vec![
			("RETURN array::map([1, 2], |$x| $x + 1)", LoopKind::Closure, LoopSink::Output, 1, 2),
			(
				"RETURN array::filter([1, 2], |$x| $x > 1)",
				LoopKind::Closure,
				LoopSink::Filter,
				1,
				2,
			),
			(
				"RETURN array::any([1, 2], |$x| $x > 1)",
				LoopKind::Closure,
				decisive(DecisiveKind::Any),
				1,
				2,
			),
			(
				"RETURN array::all([1, 2], |$x| $x > 1)",
				LoopKind::Closure,
				decisive(DecisiveKind::All),
				1,
				2,
			),
			(
				"RETURN array::find([1, 2], |$x| $x > 1)",
				LoopKind::Closure,
				decisive(DecisiveKind::Find),
				1,
				2,
			),
			(
				"RETURN array::find_index([1, 2], |$x| $x > 1)",
				LoopKind::Closure,
				decisive(DecisiveKind::FindIndex),
				1,
				2,
			),
			(
				"RETURN array::fold([1, 2], 0, |$a, $x| $a + $x)",
				LoopKind::Closure,
				LoopSink::Fold,
				2,
				3,
			),
			(
				"RETURN array::reduce([1, 2], |$a, $x| $a + $x)",
				LoopKind::Closure,
				LoopSink::Fold,
				1,
				3,
			),
			(
				"RETURN set::map(<set>[1, 2], |$x| $x + 1)",
				LoopKind::Closure,
				LoopSink::Output,
				1,
				2,
			),
			("RETURN [1, 2, 3][WHERE $this > 1]", LoopKind::ElementFilter, LoopSink::Filter, 1, 2),
		];
		for (sql, kind, sink, operands, args) in cases {
			let ir = bind(sql).await;
			let loops = ops_of(&ir, LogTag::Loop);
			assert_eq!(loops.len(), 1, "`{sql}`");
			assert_eq!(loop_of(&ir, loops[0]), (kind, sink, SourceOrder::Positional), "`{sql}`");
			assert_eq!(ir.op(loops[0]).expect("the op exists").operands.len(), operands, "`{sql}`");
			assert_eq!(region_kinds_of(&ir, loops[0]), vec![RegionKind::Loop], "`{sql}`");
			assert_eq!(body_args(&ir, loops[0]), args, "`{sql}`");
			let applies = first_block(&ir, loops[0])
				.ops
				.iter()
				.any(|op| matches!(kind_of(&ir, *op), OpKind::Expr(ExprOp::Invoke)));
			assert_eq!(applies, kind == LoopKind::Closure, "`{sql}`");
		}

		// A closure builtin whose shape is not a loop stays an ordinary call.
		let ir = bind("RETURN array::filter_index([1, 2], |$x| $x > 1)").await;
		assert!(ops_of(&ir, LogTag::Loop).is_empty());
		assert_eq!(ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Call(_)))).len(), 1);
	}

	/// A statement written as the right operand of `INSIDE` or `NOT IN` is read as a membership
	/// set; a statement read any other way is not.
	#[tokio::test]
	async fn a_statement_tested_for_membership_is_a_membership_set() {
		let cases = [
			("RETURN 1 INSIDE (SELECT VALUE id FROM t)", true),
			("RETURN 1 NOT IN (SELECT VALUE id FROM t)", true),
			("RETURN (SELECT VALUE id FROM t) INSIDE [1]", false),
			("RETURN 1 = (SELECT VALUE id FROM t)", false),
			("RETURN 1 CONTAINS (SELECT VALUE id FROM t)", false),
		];
		for (sql, membership) in cases {
			let ir = bind(sql).await;
			let subqueries =
				ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Subquery(_))));
			let read: Vec<bool> = subqueries
				.into_iter()
				.map(|op| crate::exec::ir::expr::membership::is_membership_set(&ir, op))
				.collect();
			assert_eq!(read, [membership], "`{sql}`");
		}
	}

	/// A grouped projection the planner refuses binds as the raise of the planner's refusal in
	/// place of the whole read: no grouping, aggregate call or source of it is left.
	#[tokio::test]
	async fn a_refused_grouping_binds_as_the_planners_refusal() {
		let this = "Found a `$this` parameter refering to the document of a group by select \
		            statement\nSelect statements with a group by currently have no defined \
		            document to refer to";
		let parent = "Found a `$parent` parameter refering to the document of a GROUP select \
		              statement\nSelect statements with a GROUP BY or GROUP ALL currently have \
		              no defined document to refer to";
		let cases = [
			(
				"SELECT count(count()) AS c FROM t GROUP ALL",
				"Nested aggregate functions are not supported",
			),
			(
				"SELECT k, math::sum(count()) AS c FROM t GROUP BY k",
				"Nested aggregate functions are not supported",
			),
			("SELECT $this AS d FROM t GROUP ALL", this),
			("SELECT k, $parent AS p FROM t GROUP BY k", parent),
		];
		for (sql, message) in cases {
			let ir = bind(sql).await;
			assert_eq!(
				raises(&ir),
				vec![RaiseKind::Refused(Box::new(Refusal::Query {
					message: message.to_owned(),
				}))],
				"`{sql}`"
			);
			assert!(
				ops_where(&ir, |kind| matches!(
					kind,
					OpKind::Logical(LogOp::Aggregate(_) | LogOp::Source(_))
						| OpKind::Expr(ExprOp::Agg { .. })
				))
				.is_empty(),
				"`{sql}`: the read is replaced by the raise"
			);
		}
	}

	/// A grouped projection's refusal is the whole statement's where the read is planned with it,
	/// and stands at the read where the read follows a statement that may change the catalog.
	#[tokio::test]
	async fn a_grouping_refusal_stands_where_the_planner_raises_it() {
		let ir = bind("IF true { SELECT count(count()) AS c FROM t GROUP ALL }").await;
		assert_eq!(raises(&ir).len(), 1);
		assert!(ops_of(&ir, LogTag::If).is_empty(), "nothing of the statement runs");

		let ir = bind("{ DEFINE TABLE later; SELECT count(count()) AS c FROM t GROUP ALL }").await;
		assert_eq!(raises(&ir).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Define).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Block).len(), 1);
	}

	/// `array::distinct` over an aggregate call is a plain call around that aggregate, as the
	/// planner reads it; over anything else it is an aggregate of the grouping.
	#[tokio::test]
	async fn distinct_over_an_aggregate_is_a_call_around_it() {
		let ir = bind("SELECT array::distinct(array::group(tags)) AS t FROM t GROUP ALL").await;
		assert!(raises(&ir).is_empty());
		assert_eq!(
			ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Agg { .. }))).len(),
			1
		);
		let distinct = ops_where(&ir, |kind| {
			matches!(
				kind,
				OpKind::Expr(ExprOp::Call(call))
					if matches!(&call.function, crate::expr::Function::Normal(name) if name == "array::distinct")
			)
		});
		assert_eq!(distinct.len(), 1);

		let ir = bind("SELECT array::distinct(tags) AS t FROM t GROUP ALL").await;
		assert_eq!(
			ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Agg { .. }))).len(),
			1
		);
		assert!(ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Call(_)))).is_empty());
	}

	/// A statement in a field of a grouping is planned on its own: an aggregate function it calls
	/// is its own call, not a call of the grouping around it.
	#[tokio::test]
	async fn a_statement_in_a_grouped_field_keeps_its_own_calls() {
		let ir =
			bind("SELECT (SELECT VALUE count() FROM u) AS n, count() AS c FROM t GROUP ALL").await;
		assert_eq!(
			ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Agg { .. }))).len(),
			1
		);
		let calls = ops_where(&ir, |kind| {
			matches!(
				kind,
				OpKind::Expr(ExprOp::Call(call))
					if matches!(&call.function, crate::expr::Function::Normal(name) if name == "count")
			)
		});
		assert_eq!(calls.len(), 1, "the statement's `count()` is a call of its own");
	}

	/// A broadcast is a loop of kind `Broadcast` that outputs its body per element; one that
	/// flattens states it as the loop's step.
	#[test]
	fn a_broadcast_states_whether_it_flattens() {
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let base = LogicalExpr::Array(Vec::new());
		let body = LogicalExpr::Current;
		{
			let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
			for flatten in [false, true] {
				cx.bind_loop_site(LoopSite::Broadcast {
					base: &base,
					body: &body,
					flatten,
				})
				.expect("the broadcast binds");
			}
		}
		let loops = ops_of(&ir, LogTag::Loop);
		assert_eq!(loops.len(), 2);
		for (op, flattens) in loops.iter().zip([false, true]) {
			assert_eq!(
				loop_of(&ir, *op),
				(LoopKind::Broadcast, LoopSink::Output, SourceOrder::Positional)
			);
			assert_eq!(body_args(&ir, *op), 2);
			let OpKind::Logical(LogOp::Loop {
				step,
				..
			}) = kind_of(&ir, *op)
			else {
				panic!("the op is a loop");
			};
			assert_eq!(step.as_deref(), flattens.then_some(&StepAttrs::Flatten));
		}
	}

	/// The lookup the step of the loop `op` states.
	fn lookup_of(ir: &Ir, op: OpId) -> &LookupAttrs {
		let OpKind::Logical(LogOp::Loop {
			step: Some(step),
			..
		}) = kind_of(ir, op)
		else {
			panic!("the loop states a step");
		};
		let StepAttrs::Lookup(lookup) = step.as_ref() else {
			panic!("the step is a lookup");
		};
		lookup
	}

	/// The recursion the step of the loop `op` states.
	fn recurse_of(ir: &Ir, op: OpId) -> &RecurseAttrs {
		let OpKind::Logical(LogOp::Loop {
			step: Some(step),
			..
		}) = kind_of(ir, op)
		else {
			panic!("the loop states a step");
		};
		let StepAttrs::Recurse(recurse) = step.as_ref() else {
			panic!("the step is a recursion");
		};
		recurse
	}

	/// A subject that names the table `name` and nothing else.
	fn plain_subject(name: &str) -> LookupSubjectShape {
		LookupSubjectShape {
			table: table(name),
			referencing_field: None,
			range: None,
		}
	}

	/// The loops of `ir` of kind `kind`, in pre-order.
	fn loops_of_kind(ir: &Ir, kind: LoopKind) -> Vec<OpId> {
		ops_of(ir, LogTag::Loop).into_iter().filter(|op| loop_of(ir, *op).0 == kind).collect()
	}

	/// A graph or reference lookup binds as a loop of kind `GraphStep` per hop, which states the
	/// direction and the subjects of the lookup. A clause of the lookup is a region after the body.
	#[tokio::test]
	async fn a_graph_step_states_its_lookup() {
		let out = LookupKind::Graph(Dir::Out);
		let reference = LookupKind::Reference;
		let where_clause = RegionKind::Clause(ClauseRole::Where);
		// The statement, the lookup's kind and subjects, and the regions after the body.
		let cases: Vec<(&str, LookupKind, Vec<LookupSubjectShape>, Vec<RegionKind>)> = vec![
			("RETURN p:1->knows", out.clone(), vec![plain_subject("knows")], vec![]),
			("RETURN p:1<-knows", LookupKind::Graph(Dir::In), vec![plain_subject("knows")], vec![]),
			(
				"RETURN p:1<->knows",
				LookupKind::Graph(Dir::Both),
				vec![plain_subject("knows")],
				vec![],
			),
			(
				"RETURN p:1->(knows, likes)",
				out.clone(),
				vec![plain_subject("knows"), plain_subject("likes")],
				vec![],
			),
			(
				"RETURN p:1->(knows WHERE score > 1)",
				out,
				vec![plain_subject("knows")],
				vec![where_clause],
			),
			("RETURN p:1<~comment", reference.clone(), vec![plain_subject("comment")], vec![]),
			(
				"RETURN p:1<~(comment FIELD author WHERE text != '')",
				reference,
				vec![LookupSubjectShape {
					table: table("comment"),
					referencing_field: Some("author".to_owned()),
					range: None,
				}],
				vec![where_clause],
			),
		];
		for (sql, kind, subjects, clauses) in cases {
			let ir = bind(sql).await;
			let steps = loops_of_kind(&ir, LoopKind::GraphStep);
			assert_eq!(steps.len(), 1, "`{sql}`");
			let step = steps[0];
			assert_eq!(
				loop_of(&ir, step),
				(LoopKind::GraphStep, LoopSink::Output, SourceOrder::Delivery),
				"`{sql}`"
			);
			let lookup = lookup_of(&ir, step);
			assert_eq!(lookup.kind, kind, "`{sql}`");
			assert_eq!(lookup.subjects.to_vec(), subjects, "`{sql}`");
			assert_eq!(lookup.cond, clauses.contains(&where_clause), "`{sql}`");
			let mut regions = vec![RegionKind::Loop];
			regions.extend(clauses);
			assert_eq!(region_kinds_of(&ir, step), regions, "`{sql}`");
			assert_eq!(body_args(&ir, step), 2, "`{sql}`");
			assert_eq!(ir.op(step).expect("the op exists").operands.len(), 1, "`{sql}`");
			verify_op(&ir, step).expect("the step verifies");
		}

		// Each hop of a path is a step of its own, the later reading the earlier's result.
		let ir = bind("RETURN p:1->knows->person").await;
		let steps = loops_of_kind(&ir, LoopKind::GraphStep);
		assert_eq!(steps.len(), 2);
		assert_eq!(lookup_of(&ir, steps[0]).subjects.to_vec(), vec![plain_subject("knows")]);
		assert_eq!(lookup_of(&ir, steps[1]).subjects.to_vec(), vec![plain_subject("person")]);
	}

	/// A lookup with clauses of its own binds as the statement of its clauses over the records it
	/// reaches: an `expr.subquery` whose `logical.values` reads a hop that shows the records
	/// whole and states no clause, over the path bound before the statement.
	#[tokio::test]
	async fn a_lookup_with_clauses_of_its_own_binds_as_the_statement_of_its_clauses() {
		let ir = bind("RETURN p:1->(SELECT id FROM knows ORDER BY id DESC LIMIT 10 START 2)").await;
		let steps = loops_of_kind(&ir, LoopKind::GraphStep);
		assert_eq!(steps.len(), 1);
		let lookup = lookup_of(&ir, steps[0]);
		assert_eq!(lookup.fields, Some(FieldsShape::Select(Box::new([FieldShape::All]))));
		assert_eq!((lookup.order.as_ref(), lookup.limit, lookup.start), (None, None, None));
		assert!(!lookup.cond && !lookup.only && !lookup.only_path);
		assert_eq!(ir.op(steps[0]).expect("the op exists").operands.len(), 1);
		assert_eq!(region_kinds_of(&ir, steps[0]), vec![RegionKind::Loop]);
		// The hop reads the path bound before the statement, never a free name.
		let source = ir.op(steps[0]).expect("the op exists").operands[0];
		let ValueDef::OpResult {
			op: producer,
			..
		} = ir.value(source).expect("the source exists").def
		else {
			panic!("the source is an op's result");
		};
		assert!(!matches!(
			kind_of(&ir, producer),
			OpKind::Expr(ExprOp::FreeParam { .. } | ExprOp::SessionParam { .. })
		));
		let subqueries = ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Subquery(_))));
		assert_eq!(subqueries.len(), 1);
		let statement = ir.op(subqueries[0]).expect("the op exists").regions[0];
		assert_ne!(ir.enclosing_region(producer), statement);
		assert_eq!(ops_of(&ir, LogTag::Values).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Sort).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Limit).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Project).len(), 1);
		verify_op(&ir, steps[0]).expect("the step verifies");
	}

	/// `ONLY` on a lookup of the path below a run of hops is taken off that lookup, and the run's
	/// last hop states that the run's answer is unwrapped.
	#[test]
	fn only_below_a_run_of_hops_is_stated_by_its_last_hop() {
		use crate::expr::logical::{LogicalLiteral, LogicalLookup, LogicalLookupSubject};
		let hop = |name: &str| LogicalLookup {
			kind: LookupKind::Graph(Dir::Out),
			expr: None,
			only: false,
			what: vec![LogicalLookupSubject::Table {
				table: table(name),
				referencing_field: None,
			}],
			cond: None,
			split: None,
			group: None,
			order: None,
			limit: None,
			start: None,
			alias: None,
			trace: crate::expr::logical::LookupTrace::None,
		};
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		// `[]->(x ONLY LIMIT 1)`, paged, so the run of `->y` starts above it.
		let mut paged = hop("x");
		paged.only = true;
		paged.limit = Some(LogicalExpr::Literal(LogicalLiteral::Integer(1)));
		let base = LogicalExpr::Lookup {
			base: Box::new(LogicalExpr::Array(Vec::new())),
			lookup: Box::new(paged),
		};
		let last = hop("y");
		{
			let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
			cx.bind_loop_site(LoopSite::GraphStep {
				base: &base,
				lookup: &last,
			})
			.expect("the lookup binds");
		}
		let steps = loops_of_kind(&ir, LoopKind::GraphStep);
		assert_eq!(steps.len(), 2);
		let of = |name: &str| {
			steps
				.iter()
				.map(|step| lookup_of(&ir, *step))
				.find(|lookup| lookup.subjects[0].table == table(name))
				.expect("the hop binds")
		};
		let (below, top) = (of("x"), of("y"));
		assert!(!below.only && !below.only_path);
		assert_eq!(below.limit, Some(1));
		assert!(top.only_path && !top.only);
	}

	/// A lookup over a key range binds the operands of the range's bounds after the source, and a
	/// last hop paged by a literal binds its `LIMIT` after them.
	#[test]
	fn a_lookup_over_a_key_range_states_its_bound_operands() {
		use crate::expr::logical::{
			LogicalLiteral, LogicalLookup, LogicalLookupSubject, LogicalRecordIdKey,
			LogicalRecordIdKeyRange,
		};
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let base = LogicalExpr::Array(Vec::new());
		let lookup = LogicalLookup {
			kind: LookupKind::Graph(Dir::Out),
			expr: None,
			only: false,
			what: vec![LogicalLookupSubject::Range {
				table: table("knows"),
				range: LogicalRecordIdKeyRange {
					start: Bound::Included(LogicalRecordIdKey::Number(1)),
					end: Bound::Excluded(LogicalRecordIdKey::Array(vec![LogicalExpr::Literal(
						LogicalLiteral::Integer(2),
					)])),
				},
				referencing_field: None,
			}],
			cond: None,
			split: None,
			group: None,
			order: None,
			limit: Some(LogicalExpr::Literal(LogicalLiteral::Integer(3))),
			start: None,
			alias: Some(OutputPath::field("friends")),
			trace: crate::expr::logical::LookupTrace::None,
		};
		{
			let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
			cx.bind_loop_site(LoopSite::GraphStep {
				base: &base,
				lookup: &lookup,
			})
			.expect("the lookup binds");
		}
		let steps = loops_of_kind(&ir, LoopKind::GraphStep);
		assert_eq!(steps.len(), 1);
		let attrs = lookup_of(&ir, steps[0]);
		assert_eq!(
			attrs.subjects[0].range,
			Some(KeyRange {
				start: Bound::Included(KeyBound::Number(1)),
				end: Bound::Excluded(KeyBound::Array {
					len: 1,
				}),
			})
		);
		assert_eq!(attrs.alias, Some(OutputPath::field("friends")));
		// The source, the one operand of the end bound, then `LIMIT`.
		assert_eq!(ir.op(steps[0]).expect("the op exists").operands.len(), 3);
		assert_eq!((attrs.limit, attrs.start), (Some(2), None));
		assert_eq!((attrs.split, attrs.group), (None, None));
		assert_eq!(region_kinds_of(&ir, steps[0]), vec![RegionKind::Loop]);
	}

	/// A recursion binds as a loop of kind `Recurse` that states its depth, the form of its path
	/// and its instruction. The step's body is the loop's body region.
	#[tokio::test]
	async fn a_recursion_states_its_depth_form_and_instruction() {
		let cases: Vec<(&str, Recurse, RecursePathForm, Option<RecurseInstructionShape>)> = vec![
			(
				"RETURN p:1.{1..3}(->knows->person)",
				Recurse::Range(Some(1), Some(3)),
				RecursePathForm::Nested,
				None,
			),
			("RETURN p:1.{2}->knows->person", Recurse::Fixed(2), RecursePathForm::Trailing, None),
			(
				"RETURN p:1.{1..3+collect}(->knows->person)",
				Recurse::Range(Some(1), Some(3)),
				RecursePathForm::Nested,
				Some(RecurseInstructionShape::Collect {
					inclusive: false,
				}),
			),
		];
		for (sql, depth, form, instruction) in cases {
			let ir = bind(sql).await;
			let recursions = loops_of_kind(&ir, LoopKind::Recurse);
			assert_eq!(recursions.len(), 1, "`{sql}`");
			let op = recursions[0];
			assert_eq!(
				loop_of(&ir, op),
				(LoopKind::Recurse, LoopSink::Output, SourceOrder::Positional),
				"`{sql}`"
			);
			let attrs = recurse_of(&ir, op);
			assert_eq!(attrs.depth, depth, "`{sql}`");
			assert_eq!(attrs.form, form, "`{sql}`");
			assert_eq!(attrs.instruction, instruction, "`{sql}`");
			assert_eq!(region_kinds_of(&ir, op), vec![RegionKind::Loop], "`{sql}`");
			assert_eq!(body_args(&ir, op), 2, "`{sql}`");
			assert_eq!(ir.op(op).expect("the op exists").operands.len(), 1, "`{sql}`");
			// The steps of the path are the loops of the body, one per hop.
			assert_eq!(loops_of_kind(&ir, LoopKind::GraphStep).len(), 2, "`{sql}`");
			verify_op(&ir, op).expect("the recursion verifies");
		}
	}

	/// The instruction that names a value to find takes it as an operand after the source, and a
	/// recursion that writes no path yields each depth's value as it is.
	#[test]
	fn a_recursion_states_the_operand_of_its_instruction() {
		use crate::expr::logical::{LogicalLiteral, LogicalRecurse, LogicalRecurseInstruction};
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let recurse = LogicalRecurse {
			base: LogicalExpr::Array(Vec::new()),
			recurse: Recurse::Range(None, None),
			path: None,
			instruction: Some(LogicalRecurseInstruction::Shortest {
				expects: LogicalExpr::Literal(LogicalLiteral::Integer(9)),
				inclusive: true,
			}),
		};
		{
			let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
			cx.bind_loop_site(LoopSite::Recurse(&recurse)).expect("the recursion binds");
		}
		let recursions = loops_of_kind(&ir, LoopKind::Recurse);
		assert_eq!(recursions.len(), 1);
		let attrs = recurse_of(&ir, recursions[0]);
		assert_eq!(attrs.form, RecursePathForm::Absent);
		assert_eq!(
			attrs.instruction,
			Some(RecurseInstructionShape::Shortest {
				inclusive: true,
			})
		);
		assert_eq!(ir.op(recursions[0]).expect("the op exists").operands.len(), 2);
		// The body yields the element: its only op is the yield.
		assert_eq!(body_tags(&ir, recursions[0]), vec![LogTag::Yield]);
	}

	/// `RETURN … FETCH` of a value is a `logical.fetch` over the value, whose regions are the
	/// paths, and the `RETURN` yields the fetched value.
	#[tokio::test]
	async fn return_fetch_fetches_into_the_returned_value() {
		let ir = bind("RETURN { a: 1 } FETCH a, b").await;
		let fetches = ops_of(&ir, LogTag::Fetch);
		assert_eq!(fetches.len(), 1);
		assert_eq!(
			region_kinds_of(&ir, fetches[0]),
			vec![
				RegionKind::Clause(ClauseRole::Projection),
				RegionKind::Clause(ClauseRole::Projection)
			]
		);
		assert!(ops_of(&ir, LogTag::Loop).is_empty());
		let returns = ops_of(&ir, LogTag::Return);
		assert_eq!(returns.len(), 1);
		let fetched = ir.op(fetches[0]).expect("the op exists").results[0];
		assert_eq!(ir.op(returns[0]).expect("the op exists").operands.as_slice(), &[fetched]);
	}

	/// `RETURN … FETCH` of a read fetches into each row, inside the loop that reads them.
	#[test]
	fn return_fetch_of_a_read_fetches_into_each_row() {
		let env = Env::new();
		// The value binds as the top of a unit, which takes the `RETURN` that ends it.
		let mut ir = Ir::new(request_scope(), RegionKind::Subquery);
		let mut facts = FactCache::new();
		let plan = LogicalOp::Return(crate::expr::logical::LogicalReturn {
			what: Box::new(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(table("t")),
				schema: None,
				with: None,
				for_update: false,
			})),
			fetch: vec![LogicalExpr::Field(strand("friend"))],
		});
		{
			let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
			bind_value_statement(&mut cx, &plan).expect("the plan binds");
		}
		let loops = loops_of_kind(&ir, LoopKind::Select);
		assert_eq!(loops.len(), 1);
		assert_eq!(body_tags(&ir, loops[0]), vec![LogTag::Fetch, LogTag::Yield]);
	}

	/// The tuple rows of `ir`, each with its op and its places, in pre-order.
	fn tuple_rows(ir: &Ir) -> Vec<(OpId, Vec<Place>)> {
		ops_where(ir, |kind| matches!(kind, OpKind::Expr(ExprOp::TupleRow { .. })))
			.into_iter()
			.filter_map(|op| match kind_of(ir, op) {
				OpKind::Expr(ExprOp::TupleRow {
					places,
				}) => Some((op, places.to_vec())),
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
			})
			.collect()
	}

	/// Every tuple of an `INSERT … VALUES` is one `expr.tuple_row`, whatever its places hold: the
	/// operands are the values and then the subscripts of the places, the row's only use is the
	/// array of the statement's rows, and no object stands in for it.
	#[tokio::test]
	async fn an_insert_tuple_is_a_tuple_row() {
		let ir = bind("INSERT INTO t (name.first, name.last, age) VALUES ('a', 'b', 1)").await;
		let rows = tuple_rows(&ir);
		assert_eq!(rows.len(), 1);
		let (op, places) = &rows[0];
		assert_eq!(places.iter().map(|place| place.parts.len()).collect::<Vec<_>>(), vec![2, 2, 1]);
		assert_eq!(
			places[0].parts.as_ref(),
			[PlacePart::Field(strand("name")), PlacePart::Field(strand("first"))]
		);
		assert_eq!(ir.op(*op).expect("the op exists").operands.len(), 3);
		assert!(
			ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Object { .. }))).is_empty()
		);
		assert!(region_kinds_of(&ir, *op).is_empty());

		// One row per tuple, in the order the tuples were written.
		let ir = bind("INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)").await;
		assert_eq!(tuple_rows(&ir).len(), 3);

		// A place that holds an index or a subscript is bound as its row's place, and the
		// subscripts follow the values in the operands.
		let ir = bind("INSERT INTO t (a[0], b[$n], c.d, a.b) VALUES (1, 2, 3, 4)").await;
		let rows = tuple_rows(&ir);
		assert_eq!(rows.len(), 1);
		let (op, places) = &rows[0];
		assert_eq!(places.len(), 4);
		let subscripts = places
			.iter()
			.flat_map(|place| place.parts.iter())
			.filter(|part| matches!(part, PlacePart::Dynamic(_) | PlacePart::Index(_)))
			.count();
		assert!(subscripts >= 1, "{places:?}");
		assert_eq!(ir.op(*op).expect("the op exists").operands.len(), 4 + subscripts);
		for part in places.iter().flat_map(|place| place.parts.iter()) {
			if let PlacePart::Dynamic(slot) | PlacePart::Index(slot) = part {
				assert!(usize::from(*slot) >= 4, "a subscript slot follows the values: {slot}");
			}
		}

		// A later place below an earlier one that ended a value, and a later value at an earlier
		// place, are written as they stand: the row is not restructured.
		let ir = bind("INSERT INTO t (a, a.b, a) VALUES (1, 2, 3)").await;
		let rows = tuple_rows(&ir);
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0].1.len(), 3);
	}

	/// An `INSERT` keeps the text of its `INTO` expression as written, and no other statement holds
	/// one.
	#[tokio::test]
	async fn an_insert_keeps_the_text_of_its_into() {
		let into_of = |ir: &Ir| -> Vec<Option<Box<str>>> {
			ops_of(ir, LogTag::Write)
				.into_iter()
				.filter_map(|op| match kind_of(ir, op) {
					OpKind::Logical(LogOp::Write(attrs)) => Some(attrs.into_text.clone()),
					OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => None,
				})
				.collect()
		};
		let ir = bind("INSERT INTO t (a) VALUES (1)").await;
		assert_eq!(into_of(&ir), vec![Some("t".into())]);
		let ir = bind("INSERT INTO $table (a) VALUES (1)").await;
		assert_eq!(into_of(&ir), vec![Some("$table".into())]);
		let ir = bind("INSERT [{ a: 1 }]").await;
		assert_eq!(into_of(&ir), vec![None]);
		let ir = bind("CREATE t SET a = 1").await;
		assert_eq!(into_of(&ir), vec![None]);
	}

	/// A write names its one table when its targets, its rows or its `INTO` say so, as a record id
	/// among them does, and names none when they name more than one or are evaluated.
	#[tokio::test]
	async fn a_write_names_the_one_table_its_subjects_say() {
		let table_of = |ir: &Ir| -> Vec<Option<TableName>> {
			ops_of(ir, LogTag::Write)
				.into_iter()
				.filter_map(|op| match kind_of(ir, op) {
					OpKind::Logical(LogOp::Write(attrs)) => Some(attrs.stored.table.clone()),
					OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => None,
				})
				.collect()
		};
		for (sql, named) in [
			("CREATE t:1", Some("t")),
			("CREATE t:1, t:2", Some("t")),
			("CREATE t SET a = 1", Some("t")),
			("CREATE t:1, u:1", None),
			("UPDATE t:1 SET a = 1", Some("t")),
			("DELETE t WHERE a = 1", Some("t")),
			("INSERT INTO t (a) VALUES (1)", Some("t")),
			("INSERT INTO $table (a) VALUES (1)", None),
			("INSERT [{ id: t:1 }, { id: t:2, a: 1 }]", Some("t")),
			("INSERT [{ id: t:1 }, { id: u:1 }]", None),
			("INSERT { a: 1 }", None),
			("RELATE a:1->r->b:1", Some("r")),
			("CREATE type::record('t', $id)", Some("t")),
			("CREATE type::record($table, 1)", None),
			("CREATE |t:3|", Some("t")),
			("UPDATE |t:1..3| SET a = 1", Some("t")),
			("DELETE type::table('t')", Some("t")),
			("UPDATE [t:1, t:2] SET a = 1", Some("t")),
			("UPDATE [t:1, u:2] SET a = 1", None),
			("DELETE $rows", None),
		] {
			let ir = bind(sql).await;
			assert_eq!(table_of(&ir), vec![named.map(table)], "{sql}");
		}
	}

	/// A closure's captures are operands of its creation and block arguments of its isolated body,
	/// after the timestamp and the parameters. A name the closure reads that nothing binds where
	/// it is written is free in the body, whatever a later scope binds.
	#[tokio::test]
	async fn a_closure_captures_what_it_reads() {
		let creations =
			|ir: &Ir| ops_where(ir, |kind| matches!(kind, OpKind::Expr(ExprOp::ClosureCreate(_))));

		let ir = bind("RETURN { LET $k = 2; array::map([1], |$x| $x + $k) }").await;
		let made = creations(&ir);
		assert_eq!(made.len(), 1);
		assert_eq!(ir.op(made[0]).expect("the op exists").operands.len(), 1);
		assert_eq!(region_kinds_of(&ir, made[0]), vec![RegionKind::ClosureBody]);
		assert_eq!(body_args(&ir, made[0]), 3);

		let ir = bind("RETURN { LET $k = 2; array::map([1], |$x| $x + $y) }").await;
		let made = creations(&ir);
		assert_eq!(made.len(), 1);
		assert!(ir.op(made[0]).expect("the op exists").operands.is_empty());
		assert_eq!(body_args(&ir, made[0]), 2);
		assert!(
			first_block(&ir, made[0])
				.ops
				.iter()
				.any(|op| matches!(kind_of(&ir, *op), OpKind::Expr(ExprOp::FreeParam { .. })))
		);
	}

	/// A function body reads its declared arguments, the row names it is passed and the
	/// statement timestamp, and nothing its caller bound.
	#[test]
	fn function_body_hides_caller_names() {
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
		let caller = cx.none_literal().expect("the literal binds");
		cx.declare(strand("x"), caller);
		assert_eq!(cx.lookup("x"), NameRef::Value(caller));
		let owner = cx
			.emit(
				OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
				&[],
				&[TypeKnowledge::Unknown],
			)
			.expect("the owner binds");
		cx.within_function_body(owner, &[strand("p")], &[], &[strand("this")], |cx| {
			assert_eq!(cx.lookup("x"), NameRef::Free);
			assert!(matches!(cx.lookup("p"), NameRef::Value(_)));
			assert!(matches!(cx.lookup("this"), NameRef::Value(_)));
			assert!(cx.timestamp().is_some());
			Ok(())
		})
		.expect("the body binds");
		assert_eq!(cx.lookup("x"), NameRef::Value(caller));
	}

	/// Whether `value` is defined in `region` or in a region beneath it.
	fn defined_within(
		ir: &Ir,
		value: ValueId,
		region: crate::exec::ir::iface::ids::RegionId,
	) -> bool {
		let mut at = match ir.value(value).expect("the value exists").def {
			ValueDef::OpResult {
				op,
				..
			} => ir.enclosing_region(op),
			ValueDef::BlockArg {
				block,
				..
			} => ir.block(block).expect("the block exists").parent,
		};
		loop {
			if at == region {
				return true;
			}
			match ir.parent_op(at) {
				Some(owner) => at = ir.enclosing_region(owner),
				None => return false,
			}
		}
	}

	/// A `FunctionBody` region reads no value defined outside it. Every operand of every op in
	/// it, and in the regions beneath it, is defined in the region: an argument or a result. A
	/// name the body reads that it does not bind, a caller's among them, is an `expr.free_param`
	/// of the body, resolved when it runs.
	#[test]
	fn a_function_body_region_reads_no_value_defined_outside_it() {
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let owner = {
			let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
			let caller = cx.none_literal().expect("the literal binds");
			cx.declare(strand("x"), caller);
			let owner = cx
				.emit(
					OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
					&[],
					&[TypeKnowledge::Unknown],
				)
				.expect("the owner binds");
			cx.within_function_body(owner, &[strand("p")], &[], &[], |cx| {
				let NameRef::Value(argument) = cx.lookup("p") else {
					return Err(IrError::Stale("the argument"));
				};
				let free = match cx.lookup("x") {
					NameRef::Free => cx.emit_value(
						OpKind::Expr(ExprOp::FreeParam {
							name: strand("x"),
						}),
						&[],
						TypeKnowledge::Unknown,
					)?,
					NameRef::Value(_) => return Err(IrError::Stale("a caller's name resolved")),
				};
				cx.emit(OpKind::Expr(ExprOp::Array), &[argument, free], &[TypeKnowledge::Unknown])?;
				Ok(())
			})
			.expect("the body binds");
			owner
		};
		let region = ir.op(owner).expect("the owner exists").regions[0];
		assert_eq!(ir.region(region).expect("the region exists").kind, RegionKind::FunctionBody);
		let mut seen = 0usize;
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			let data = ir.op(op).expect("the op exists");
			for operand in &data.operands {
				seen += 1;
				assert!(
					defined_within(&ir, *operand, region),
					"an operand of {:?} is defined outside the function body",
					data.kind
				);
			}
			Walk::Continue
		});
		assert_eq!(seen, 2, "the array reads the argument and the free name");
	}

	/// The region a write's clause or an index's column is evaluated in binds `$this` and `$self`
	/// to the row it receives, and the names the write exposes to its own arguments: `$input` of
	/// `ON DUPLICATE KEY UPDATE` and `$before` and `$after` of a `RETURN`. `$value` is no name of
	/// the row there: it reads the parameter of the frame around the statement.
	#[test]
	fn a_write_clause_binds_its_row_names_to_its_arguments() {
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
		let owner = cx
			.emit(
				OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
				&[],
				&[TypeKnowledge::Unknown],
			)
			.expect("the owner binds");
		let bound = |cx: &BindCx<'_>, name: &str| match cx.lookup(name) {
			NameRef::Value(value) => Some(value),
			NameRef::Free => None,
		};
		let set = RegionKind::Clause(ClauseRole::SetValue);
		enter_row_region(&mut cx, owner, set, |cx, row| {
			for name in ["this", "self"] {
				assert_eq!(bound(cx, name), Some(row), "${name} in a SET");
			}
			for name in ["value", "input", "before", "after", "event"] {
				assert_eq!(bound(cx, name), None, "${name} in a SET");
			}
			Ok(())
		})
		.expect("the clause binds");
		// A column or the guard of an index reads the image it is evaluated over as `$this`,
		// `$self` and `@`, not the target the loop around the stage holds.
		let column = RegionKind::Clause(ClauseRole::IndexColumn);
		enter_row_region(&mut cx, owner, column, |cx, row| {
			for name in ["this", "self"] {
				assert_eq!(bound(cx, name), Some(row), "${name} in an index column");
			}
			for name in ["value", "input", "before", "after"] {
				assert_eq!(bound(cx, name), None, "${name} in an index column");
			}
			Ok(())
		})
		.expect("the clause binds");
		let duplicate = RegionKind::Clause(ClauseRole::DuplicateUpdate);
		enter_row_region(&mut cx, owner, duplicate, |cx, row| {
			assert_eq!(bound(cx, "this"), Some(row));
			let input = bound(cx, "input").expect("$input is bound");
			assert_ne!(input, row);
			assert_eq!(bound(cx, "before"), None);
			Ok(())
		})
		.expect("the clause binds");
		let returned = RegionKind::Clause(ClauseRole::ReturnClause);
		enter_row_region(&mut cx, owner, returned, |cx, row| {
			assert_eq!(bound(cx, "this"), Some(row));
			assert_eq!(bound(cx, "after"), Some(row));
			let before = bound(cx, "before").expect("$before is bound");
			assert_ne!(before, row);
			assert_eq!(bound(cx, "input"), None);
			Ok(())
		})
		.expect("the clause binds");
	}

	/// A region isolated from above hides the names of the region around it and binds the row
	/// names of its own frame: `$this` and `$self` name its row, and a field's `VALUE`, `DEFAULT`
	/// and `ASSERT` frames bind the value they are about, the old value and the supplied one.
	#[test]
	fn an_isolated_frame_binds_its_own_row_names() {
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
		let caller = cx.none_literal().expect("the literal binds");
		cx.declare(strand("x"), caller);
		cx.declare(strand("parent"), caller);
		let owner = cx
			.emit(
				OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
				&[],
				&[TypeKnowledge::Unknown],
			)
			.expect("the owner binds");
		let bound = |cx: &BindCx<'_>, name: &str| match cx.lookup(name) {
			NameRef::Value(value) => Some(value),
			NameRef::Free => None,
		};
		for kind in [
			RegionKind::PermissionFrame,
			RegionKind::FieldFrame(FieldRole::Computed),
			RegionKind::StoredClause(StoredRole::Computed),
			RegionKind::StoredClause(StoredRole::Permissions),
		] {
			enter_row_region(&mut cx, owner, kind, |cx, row| {
				for name in ["this", "self", "value"] {
					assert_eq!(bound(cx, name), Some(row), "${name} in {kind:?}");
				}
				assert_eq!(bound(cx, "x"), None, "a caller name in {kind:?}");
				assert_eq!(bound(cx, "parent"), None, "$parent in {kind:?}");
				Ok(())
			})
			.expect("the frame binds");
		}
		for kind in [
			RegionKind::FieldFrame(FieldRole::Value),
			RegionKind::StoredClause(StoredRole::Default),
			RegionKind::StoredClause(StoredRole::Assert),
		] {
			enter_row_region(&mut cx, owner, kind, |cx, row| {
				assert_eq!(bound(cx, "this"), Some(row), "$this in {kind:?}");
				let value = bound(cx, "value").expect("$value is bound");
				assert_ne!(value, row, "$value in {kind:?}");
				assert_eq!(bound(cx, "after"), Some(value), "$after in {kind:?}");
				assert!(bound(cx, "before").is_some(), "$before in {kind:?}");
				assert!(bound(cx, "input").is_some(), "$input in {kind:?}");
				Ok(())
			})
			.expect("the frame binds");
		}
		assert_eq!(cx.lookup("x"), NameRef::Value(caller));
	}

	/// A field read or `@` where no row frame is entered reads a `NONE` literal, and a row frame
	/// answers its row.
	#[test]
	fn a_read_outside_a_row_frame_reads_none() {
		let env = Env::new();
		let mut ir = Ir::new(request_scope(), RegionKind::Request);
		let mut facts = FactCache::new();
		let mut cx = BindCx::new(&mut ir, &mut facts, env.cx()).expect("the context builds");
		assert_eq!(cx.frame_row(), None);
		let none = cx.row().expect("a rowless read binds");
		assert!(matches!(
			cx.ir().value(none).and_then(|data| match data.def {
				ValueDef::OpResult {
					op,
					..
				} => cx.ir().op(op).map(|op| op.kind.clone()),
				ValueDef::BlockArg {
					..
				} => None,
			}),
			Some(OpKind::Expr(ExprOp::Literal(literal))) if *literal == Value::None
		));
		let row = cx.none_literal().expect("the literal binds");
		let previous = cx.replace_row(Some(row));
		assert_eq!(previous, None);
		assert_eq!(cx.row().expect("a row is read"), row);
	}

	/// A bare field read at the top of a statement, in a condition and in a closure written where
	/// no row frame is entered binds, as a read of no document; in a function body it binds as a
	/// read of the row of the caller.
	#[tokio::test]
	async fn a_bare_read_outside_a_row_frame_reads_no_document() {
		for sql in ["RETURN a;", "IF active { 1 };", "LET $x = name;", "LET $f = || foo;"] {
			let env = Env::new();
			let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
			let request_statements = RequestStatement::of(&parsed);
			let input = RequestInput::request(&request_statements, request_scope());
			assert!(bind_request(&input, &env.cx()).await.is_ok(), "{sql}");
		}
		let env = Env::new();
		let parsed = crate::syn::parse("DEFINE FUNCTION fn::f() { RETURN name; };")
			.expect("the request parses")
			.expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(&request_statements, request_scope());
		assert!(bind_request(&input, &env.cx()).await.is_ok());
	}

	/// The element of a `SELECT` loop answers to `$this` and `$self`, so a read of one is a value
	/// and not a free name. `$value` is no name of the element: it reads the parameter of the
	/// frame around the statement.
	#[tokio::test]
	async fn select_row_names_are_bound() {
		let ir = bind("SELECT VALUE [$this, $self] FROM t").await;
		assert!(
			ops_where(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::FreeParam { .. })))
				.is_empty()
		);
		let ir = bind("SELECT VALUE $value FROM t").await;
		let free = ops_where(
			&ir,
			|kind| matches!(kind, OpKind::Expr(ExprOp::FreeParam { name }) if name.as_str() == "value"),
		);
		assert_eq!(free.len(), 1);
	}

	/// The first rejection of the request `sql` binds to, from the checked binding.
	async fn rejection(sql: &str) -> Option<RegionRejection> {
		let env = Env::new();
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(&request_statements, request_scope());
		bind_request_checked(&input, &env.cx()).await.expect("the request binds").1
	}

	/// A statement the user wrote in a region that does not admit it binds, and the checked
	/// binding answers the rejection: the construct, the row, the region and the statement. The
	/// same constructs in a region that admits them, and calls and `LET`s that are the code's
	/// own, answer none.
	#[tokio::test]
	async fn the_checked_binding_answers_the_region_rejection() {
		let permissions = RegionKind::StoredClause(StoredRole::Permissions);
		let stored = |role| RegionKind::StoredClause(role);
		let cases: Vec<(&str, Construct, Section4bRow, RegionKind, u32)> = vec![
			(
				"CREATE t:1; SELECT VALUE id FROM t WHERE (CREATE w:1 RETURN NONE) = [];",
				Construct::Write,
				Section4bRow::ConditionClause,
				RegionKind::Clause(ClauseRole::Where),
				2,
			),
			(
				"CREATE t:1; SELECT VALUE id FROM t WHERE { DEFINE TABLE zz; true };",
				Construct::Ddl,
				Section4bRow::ConditionClause,
				RegionKind::Clause(ClauseRole::Where),
				2,
			),
			(
				"CREATE t:1; SELECT VALUE id FROM t WHERE { SLEEP 1ms; true };",
				Construct::Sleep,
				Section4bRow::ConditionClause,
				RegionKind::Clause(ClauseRole::Where),
				2,
			),
			(
				"DEFINE TABLE p PERMISSIONS FOR select WHERE (CREATE plog:1 RETURN NONE) = [];",
				Construct::Write,
				Section4bRow::PermissionOrAssertFrame,
				permissions,
				1,
			),
			(
				"DEFINE FIELD f ON t ASSERT (CREATE alog:1 RETURN NONE) = [];",
				Construct::Write,
				Section4bRow::PermissionOrAssertFrame,
				stored(StoredRole::Assert),
				1,
			),
			(
				"DEFINE FIELD f ON v VALUE (CREATE log2:1 RETURN NONE);",
				Construct::Write,
				Section4bRow::FieldValueFrame,
				stored(StoredRole::Value),
				1,
			),
			(
				"DEFINE FIELD f ON v COMPUTED (CREATE log2:1 RETURN NONE);",
				Construct::Write,
				Section4bRow::FieldValueFrame,
				stored(StoredRole::Computed),
				1,
			),
			(
				"DEFINE FIELD f ON v DEFAULT (CREATE log2:1 RETURN NONE);",
				Construct::Write,
				Section4bRow::FieldValueFrame,
				stored(StoredRole::Default),
				1,
			),
			(
				"DEFINE INDEX i ON q FIELDS time::now();",
				Construct::NonDeterministic,
				Section4bRow::IndexColumn,
				stored(StoredRole::IndexColumn),
				1,
			),
			(
				"DEFINE INDEX i ON q FIELDS (SELECT VALUE v FROM other:1);",
				Construct::OtherRecordRead,
				Section4bRow::IndexColumn,
				stored(StoredRole::IndexColumn),
				1,
			),
		];
		for (sql, construct, row, region, statement) in cases {
			let found = rejection(sql).await.unwrap_or_else(|| panic!("{sql}: nothing rejected"));
			assert_eq!(
				(found.construct, found.row, found.region),
				(construct, row, region),
				"{sql}"
			);
			assert_eq!(found.loc.stmt + 1, statement, "{sql}");
		}

		for sql in [
			"CREATE t:1; SELECT VALUE id FROM t;",
			"SELECT VALUE (CREATE w:1 RETURN NONE) FROM t;",
			"{ LET $a = 1; SLEEP 1ms; $a };",
			"CREATE t:1; SELECT VALUE id FROM t WHERE { LET $a = 1; $a == 1 };",
			"DEFINE FIELD f ON v VALUE 1;",
			"DEFINE INDEX i ON q FIELDS a;",
			"SELECT * FROM [{ a: 1 }] WHERE [1, 2].map(|$x| { LET $y = $x; RETURN $y + 1 }) = [2, 3];",
		] {
			assert_eq!(rejection(sql).await, None, "{sql}");
		}
	}

	/// The kinds of the regions of every op of `ir`.
	fn every_region_kind(ir: &Ir) -> Vec<RegionKind> {
		all_ops(ir).into_iter().flat_map(|op| region_kinds_of(ir, op)).collect()
	}

	/// A block written as a statement of a block binds as a region of kind `Block`, which catches
	/// a `RETURN`, so `{ { RETURN 1 }; RETURN 2 }` answers 2. A block standing as a value is a
	/// `ValueBlock`, which catches one too, and an arm of a statement `IF` is a `StatementIf`,
	/// which passes the `RETURN` outwards.
	#[tokio::test]
	async fn a_block_written_as_a_statement_of_a_block_is_a_block_region() {
		let ir = bind("{ { RETURN 1 }; RETURN 2 }").await;
		let blocks = ops_of(&ir, LogTag::Block);
		assert_eq!(blocks.len(), 2);
		for block in blocks {
			let kinds = region_kinds_of(&ir, block);
			assert_eq!(kinds, vec![RegionKind::Block]);
			assert_eq!(kinds[0].return_rule(), ReturnRule::PassesOutward);
		}

		let ir = bind("{ LET $x = { RETURN 1 }; $x }").await;
		let kinds: Vec<RegionKind> = ops_of(&ir, LogTag::Block)
			.into_iter()
			.flat_map(|op| region_kinds_of(&ir, op))
			.collect();
		assert_eq!(kinds, vec![RegionKind::Block, RegionKind::ValueBlock]);

		let ir = bind("{ IF rand::bool() { RETURN 1 }; 2 }").await;
		let ifs = ops_of(&ir, LogTag::If);
		assert_eq!(ifs.len(), 1);
		let kinds = region_kinds_of(&ir, ifs[0]);
		assert_eq!(kinds, vec![RegionKind::StatementIf, RegionKind::StatementIf]);
		for kind in kinds {
			assert_eq!(kind.return_rule(), ReturnRule::PassesOutward);
		}
	}

	/// A read, a write or a schema statement of a statement list is the `logical.statement` that
	/// holds it, in a region of its own; any other statement stands in the list. A request's own
	/// statement is not one.
	#[tokio::test]
	async fn a_list_binds_its_statements_as_groups() {
		let ir = bind("{ SELECT * FROM t; LET $x = 1; CREATE t:1; $x }").await;
		let statements = ops_of(&ir, LogTag::Statement);
		assert_eq!(statements.len(), 2);
		for op in &statements {
			assert_eq!(
				region_kinds_of(&ir, *op),
				vec![RegionKind::Statement(StatementCommit::InTxn)]
			);
		}
		assert!(body_tags(&ir, statements[0]).contains(&LogTag::Loop));
		assert!(body_tags(&ir, statements[1]).contains(&LogTag::Write));
		let blocks = ops_of(&ir, LogTag::Block);
		assert_eq!(blocks.len(), 1);
		let listed = body_tags(&ir, blocks[0]);
		assert_eq!(listed.iter().filter(|tag| **tag == LogTag::Statement).count(), 2);
		assert!(listed.contains(&LogTag::Let));
		assert!(!listed.contains(&LogTag::Loop) && !listed.contains(&LogTag::Write));

		let ir = bind("SELECT * FROM t").await;
		assert!(ops_of(&ir, LogTag::Statement).is_empty());
		let ir = bind("FOR $i IN [1, 2] { UPDATE t SET a = $i }").await;
		assert_eq!(ops_of(&ir, LogTag::Statement).len(), 1);
	}

	/// A statement that may change the catalog says so.
	#[tokio::test]
	async fn a_schema_statement_of_a_list_marks_that_it_changes_the_catalog() {
		let ir = bind("{ DEFINE TABLE a; SELECT * FROM a }").await;
		let flags: Vec<bool> = ops_of(&ir, LogTag::Statement)
			.into_iter()
			.filter_map(|op| match kind_of(&ir, op) {
				OpKind::Logical(LogOp::Statement {
					changes_catalog,
					..
				}) => Some(*changes_catalog),
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
			})
			.collect();
		assert_eq!(flags, vec![true, false]);
	}

	/// Where the container serves late-bound statements, a group after a statement of its list
	/// that may change the catalog names the statement as written, in an `IF` arm and after the
	/// `IF` alike, and yields `NONE` in its place; a group before it is bound where it stands.
	#[tokio::test]
	async fn a_group_after_a_schema_statement_is_served_as_written() {
		let env = Env::new();
		let sql = "{ SELECT * FROM a; DEFINE TABLE b; IF $x { SELECT * FROM b }; CREATE c:1 }";
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let mut input = RequestInput::request(&statements, request_scope());
		input.serves_late = true;
		let (ir, _, notes) = crate::exec::ir::build::logical::bind_request_noted(
			&input,
			&env.cx(),
			Some(&crate::exec::surface::FULL),
		)
		.await
		.expect("the request binds");
		let served: Vec<Option<u32>> = ops_of(&ir, LogTag::Statement)
			.into_iter()
			.filter_map(|op| match kind_of(&ir, op) {
				OpKind::Logical(LogOp::Statement {
					served,
					..
				}) => Some(*served),
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
			})
			.collect();
		assert_eq!(served, vec![None, None, Some(0), Some(1)]);
		assert_eq!(notes.served.len(), 2);
		assert!(!matches!(&*notes.served[0], crate::expr::logical::LogicalOp::Write(_)));
		assert!(matches!(&*notes.served[1], crate::expr::logical::LogicalOp::Write(_)));
		// Without serving, every group is bound where it stands.
		let ir = bind(sql).await;
		assert!(ops_of(&ir, LogTag::Statement).into_iter().all(|op| matches!(
			kind_of(&ir, op),
			OpKind::Logical(LogOp::Statement {
				served: None,
				..
			})
		)));
	}

	/// The value of a `LET` or a `RETURN` of a list that is a statement is the value of its group.
	/// A `LET` that is a whole statement of the request sets a parameter of the session, and its
	/// value is bound where it stands.
	#[tokio::test]
	async fn the_value_of_a_let_or_return_that_is_a_statement_is_a_group() {
		let reads_a_group = |ir: &Ir, tag: LogTag| {
			let ops = ops_of(ir, tag);
			assert_eq!(ops.len(), 1);
			let operand = ir.op(ops[0]).expect("the op exists").operands[0];
			match ir.value(operand).map(|data| data.def) {
				Some(ValueDef::OpResult {
					op,
					..
				}) => matches!(kind_of(ir, op), OpKind::Logical(LogOp::Statement { .. })),
				Some(ValueDef::BlockArg {
					..
				})
				| None => false,
			}
		};
		let ir = bind("{ LET $rows = SELECT * FROM t; RETURN $rows }").await;
		assert_eq!(ops_of(&ir, LogTag::Statement).len(), 1);
		assert!(reads_a_group(&ir, LogTag::Let));
		let ir = bind("{ RETURN SELECT * FROM t }").await;
		assert_eq!(ops_of(&ir, LogTag::Statement).len(), 1);
		assert!(reads_a_group(&ir, LogTag::Return));
		let ir = bind("LET $rows = SELECT * FROM t").await;
		assert!(ops_of(&ir, LogTag::Statement).is_empty());
	}

	/// A `FOR` states the parameter it binds its element to, as written.
	#[tokio::test]
	async fn a_for_states_its_parameter() {
		let ir = bind("FOR $item IN [1, 2] { CREATE t SET a = $item }").await;
		let loops = ops_of(&ir, LogTag::Loop);
		let stated: Vec<_> = loops
			.iter()
			.filter_map(|op| match ir.op(*op).map(|data| &data.kind) {
				Some(OpKind::Logical(LogOp::Loop {
					kind: LoopKind::For,
					step,
					..
				})) => Some(step.clone()),
				_ => None,
			})
			.collect();
		assert_eq!(stated, vec![Some(Box::new(StepAttrs::Param(Strand::new("item"))))]);
	}

	/// Each clause written inside the body of a `FOR` binds in a region that passes a `BREAK` or
	/// `CONTINUE` to the loop: the body of the `FOR`, the per-element loop of the statement it
	/// holds, and every clause region of that statement.
	#[tokio::test]
	async fn clauses_in_a_for_body_pass_loop_signals() {
		for sql in [
			"FOR $i IN [1, 2] { SELECT * FROM t WHERE a = $i }",
			"FOR $i IN [1, 2] { SELECT * FROM t ORDER BY a LIMIT $i }",
			"FOR $i IN [1, 2] { UPDATE t SET a = $i WHERE b = $i }",
			"FOR $i IN [1, 2] { CREATE t CONTENT { a: $i } }",
			"FOR $i IN [1, 2] { SELECT VALUE (SELECT VALUE id FROM t) FROM t }",
		] {
			let ir = bind(sql).await;
			let kinds = every_region_kind(&ir);
			let clauses: Vec<RegionKind> = kinds
				.iter()
				.copied()
				.filter(|kind| {
					matches!(kind, RegionKind::Clause(_) | RegionKind::Loop | RegionKind::Subquery)
				})
				.collect();
			assert!(!clauses.is_empty(), "`{sql}` binds no clause region");
			for kind in clauses {
				assert_eq!(kind.loop_rule(), LoopRule::Passes, "`{sql}`: {kind:?}");
			}
			assert!(kinds.contains(&RegionKind::StatementFor), "`{sql}`");
		}
	}

	/// Stored clauses, events, permissions, function bodies and closure bodies bind in regions
	/// that are barriers to a loop: the loop they name is not written in them.
	#[tokio::test]
	async fn stored_code_and_bodies_are_loop_barriers() {
		for sql in [
			"DEFINE FIELD f ON t TYPE int VALUE 1 ASSERT true DEFAULT 2",
			"DEFINE FIELD c ON t COMPUTED { 1 }",
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select WHERE true",
			"DEFINE EVENT e ON t WHEN true THEN (RETURN 1)",
			"DEFINE FUNCTION fn::f() { RETURN 1 }",
			"RETURN array::map([1], |$x| $x)",
		] {
			let ir = bind(sql).await;
			let barriers: Vec<RegionKind> = every_region_kind(&ir)
				.into_iter()
				.filter(|kind| {
					matches!(
						kind,
						RegionKind::StoredClause(_)
							| RegionKind::FieldFrame(_)
							| RegionKind::PermissionFrame
							| RegionKind::EventFrame
							| RegionKind::FunctionBody
							| RegionKind::ClosureBody
					)
				})
				.collect();
			assert!(!barriers.is_empty(), "`{sql}` binds no stored code");
			for kind in barriers {
				assert_eq!(kind.loop_rule(), LoopRule::Barrier, "`{sql}`: {kind:?}");
			}
		}
	}

	/// The surface text each op of `ir` carries, in pre-order.
	fn written_texts(ir: &Ir) -> Vec<String> {
		all_ops(ir)
			.into_iter()
			.filter_map(|op| ir.op(op).and_then(|data| data.loc.text()).map(str::to_owned))
			.collect()
	}

	/// A statement under `EXPLAIN` records the text of each expression it binds on the op that
	/// computes it, down to the operands of a conjunct, which is the text a plan prints.
	#[tokio::test]
	async fn an_explained_statement_carries_the_text_of_its_expressions() {
		let ir = bind("SELECT * FROM person WHERE age > 18 AND name = 'a' EXPLAIN").await;
		let texts = written_texts(&ir);
		for expected in ["age > 18", "name = 'a'", "age", "18"] {
			assert!(texts.iter().any(|text| text == expected), "{expected:?} in {texts:?}");
		}
		assert!(
			texts.iter().any(|text| text == "age > 18 AND name = 'a'"),
			"the chain of conjuncts carries its own text: {texts:?}"
		);
	}

	/// A statement that is not explained carries no text: the planning of every query binds it,
	/// and only an `EXPLAIN` reads it.
	#[tokio::test]
	async fn a_statement_that_is_not_explained_carries_no_text() {
		let ir = bind("SELECT * FROM person WHERE age > 18 AND name = 'a'").await;
		assert_eq!(written_texts(&ir), Vec::<String>::new());
	}

	/// A `KILL` carries its id as the statement wrote it, whether or not the statement is
	/// explained, because the errors that name the statement print it.
	#[tokio::test]
	async fn a_kill_carries_its_id_as_written() {
		for (sql, written) in [
			("KILL $id", "$id"),
			(
				"KILL u'e72bee20-f49b-11ec-b939-0242ac120002'",
				"u'e72bee20-f49b-11ec-b939-0242ac120002'",
			),
		] {
			let ir = bind(sql).await;
			let kills: Vec<OpId> = all_ops(&ir)
				.into_iter()
				.filter(|op| matches!(kind_of(&ir, *op), OpKind::Logical(LogOp::Kill)))
				.collect();
			let [kill] = kills[..] else {
				panic!("`{sql}` binds one kill, found {kills:?}");
			};
			assert_eq!(ir.op(kill).and_then(|data| data.loc.text()), Some(written), "{sql}");
		}
	}

	/// The text is a label: two containers that differ only in it print the same, locations
	/// included, so the text form that keys a plan by what it computes does not hold it.
	#[tokio::test]
	async fn written_text_is_not_part_of_the_text_form() {
		use crate::exec::ir::core::print::{PrintOpts, print};

		let opts = PrintOpts {
			locations: true,
			..PrintOpts::default()
		};

		let explained = bind("SELECT * FROM person WHERE age > 18 EXPLAIN").await;
		let mut stripped = explained.clone();
		let located: Vec<OpId> = all_ops(&stripped);
		let mut facts = crate::exec::ir::core::facts::FactCache::new();
		let mut rw = crate::exec::ir::core::rewriter::Rewriter::new(&mut stripped, &mut facts);
		for op in located {
			let loc = rw.ir().op(op).expect("the op exists").loc.clone();
			let plain = crate::exec::ir::core::location::Location {
				origin: crate::exec::ir::core::location::LocOrigin::Query,
				..loc
			};
			rw.set_location(op, plain).expect("the location is set");
		}
		assert!(!written_texts(&explained).is_empty());
		assert!(written_texts(&stripped).is_empty());
		assert_eq!(
			print(&explained, explained.top(), opts),
			print(&stripped, stripped.top(), opts)
		);
	}

	// -----------------------------------------------------------------------------------------
	// Planning a container as a unit
	// -----------------------------------------------------------------------------------------

	/// Binds the one statement of `sql` as `unit` and judges it as the checked binding does.
	async fn bound_as(sql: &str, unit: UnitKind) -> Result<(Ir, Option<RegionRejection>), IrError> {
		let env = Env::new();
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let input = RequestInput {
			statements: &statements,
			scope: request_scope(),
			unit,
			serves_late: false,
			late_bound: false,
		};
		bind_request_checked(&input, &env.cx()).await
	}

	/// The kind of the region each `RETURN`, `BREAK` and `CONTINUE` of `ir` ends or leaves, in
	/// pre-order.
	fn target_kinds(ir: &Ir) -> Vec<RegionKind> {
		use crate::exec::ir::core::walk::{ControlTarget, control_target};
		all_ops(ir)
			.into_iter()
			.filter_map(|op| match control_target(ir, op) {
				ControlTarget::Region(region) => ir.region(region).map(|held| held.kind),
				ControlTarget::Escapes(_)
				| ControlTarget::Barrier(_)
				| ControlTarget::NotControl => None,
			})
			.collect()
	}

	fn top_kind(ir: &Ir) -> RegionKind {
		ir.region(ir.top()).expect("the top region exists").kind
	}

	/// The statement that stands in a clause the table rejects a write in.
	const WRITE_IN_WHERE: &str = "SELECT VALUE id FROM t WHERE (CREATE w:1 RETURN NONE) = []";

	/// A request statement is planned as it is in a whole request: the top region is a request,
	/// the statement has a region of its own that a `RETURN` ends, and the constraint check runs
	/// and names the statement by its position in the request.
	#[tokio::test]
	async fn a_request_statement_is_a_statement_of_a_request() {
		for unit in [
			UnitKind::RequestStatement {
				position: 2,
			},
			UnitKind::ServedStatement {
				position: 2,
			},
		] {
			let (ir, rejection) = bound_as("RETURN 1", unit.clone()).await.expect("the unit binds");
			assert_eq!(top_kind(&ir), RegionKind::Request, "{unit:?}");
			assert_eq!(
				target_kinds(&ir),
				vec![RegionKind::Statement(StatementCommit::Auto)],
				"{unit:?}"
			);
			assert!(rejection.is_none(), "{unit:?}");
			let (_, rejection) =
				bound_as(WRITE_IN_WHERE, unit.clone()).await.expect("the unit binds");
			let rejection = rejection.unwrap_or_else(|| panic!("{unit:?} is checked"));
			assert_eq!(rejection.construct, Construct::Write);
			assert!(rejection.message().ends_with("(statement 3)"), "{}", rejection.message());
			let (ir, _) = bound_as("FOR $i IN [1] { BREAK }", unit).await.expect("the unit binds");
			assert_eq!(target_kinds(&ir), vec![RegionKind::StatementFor]);
		}
	}

	/// A statement unit is the whole of a statement region: `RETURN` ends it, a `BREAK` with no
	/// loop around it raises, and a write in a condition is rejected only for a statement of a
	/// `MATCH`, whose read is written in the request, and not for the read unit of a table, which
	/// the planner synthesises.
	#[tokio::test]
	async fn a_statement_unit_has_a_statement_region_for_its_top() {
		let auto = RegionKind::Statement(StatementCommit::Auto);
		for (unit, checked) in [(UnitKind::TableRead, false), (UnitKind::MatchStage, true)] {
			let (ir, rejection) = bound_as("RETURN 1", unit.clone()).await.expect("binds");
			assert_eq!(top_kind(&ir), auto, "{unit:?}");
			assert_eq!(target_kinds(&ir), vec![auto], "{unit:?}");
			assert!(rejection.is_none(), "{unit:?}");
			let (ir, _) = bound_as("BREAK", unit.clone()).await.expect("binds");
			assert_eq!(raises(&ir), vec![RaiseKind::InvalidControlFlow], "{unit:?}");
			assert!(target_kinds(&ir).is_empty(), "{unit:?}");
			let (_, judged) = bound_as(WRITE_IN_WHERE, unit.clone()).await.expect("binds");
			assert_eq!(judged.is_some(), checked, "{unit:?}");
			assert_eq!(unit.checked(), checked);
		}
	}

	/// The body of a function and the stored code of a write are statement lists in a region
	/// that catches a `RETURN`, is a barrier to `BREAK` and `CONTINUE`, and is never judged.
	#[tokio::test]
	async fn a_list_unit_catches_return_and_bars_break() {
		let units = [
			(UnitKind::FunctionBody, RegionKind::FunctionBody),
			(UnitKind::Cascade, RegionKind::WriteStage),
			(
				UnitKind::StageBundle {
					table: TableName::new("t"),
					action: crate::doc::Action::Update,
				},
				RegionKind::WriteStage,
			),
			(
				UnitKind::ViewRows {
					table: TableName::new("t"),
				},
				RegionKind::WriteStage,
			),
			(
				UnitKind::Subscription {
					table: TableName::new("t"),
				},
				RegionKind::SubscriberFrame,
			),
		];
		for (unit, top) in units {
			let (ir, rejection) =
				bound_as("{ RETURN 1; 2 }", unit.clone()).await.expect("the unit binds");
			assert_eq!(top_kind(&ir), top, "{unit:?}");
			assert_eq!(target_kinds(&ir), vec![top], "{unit:?}");
			assert!(rejection.is_none(), "{unit:?}");
			let (ir, _) = bound_as("{ BREAK; 2 }", unit.clone()).await.expect("the unit binds");
			assert_eq!(raises(&ir), vec![RaiseKind::InvalidControlFlow], "{unit:?}");
			let (ir, _) =
				bound_as("{ FOR $i IN [1] { BREAK } }", unit.clone()).await.expect("binds");
			assert_eq!(target_kinds(&ir), vec![RegionKind::StatementFor], "{unit:?}");
			let (_, rejection) =
				bound_as(&format!("{{ {WRITE_IN_WHERE} }}"), unit.clone()).await.expect("binds");
			assert!(rejection.is_none(), "{unit:?} is stored or synthesised code");
		}
	}

	/// A subquery, a closure body and a guard are values: the statement is the value of the region,
	/// a `RETURN` ends the region, and a `BREAK` with no `FOR` around the unit raises. A subquery
	/// is judged when it is written in the request and is not when the code that holds it is
	/// stored.
	#[tokio::test]
	async fn a_value_unit_catches_return_and_is_judged_by_where_it_is_written() {
		let units = [
			(
				UnitKind::Subquery {
					stored: false,
				},
				RegionKind::Subquery,
				true,
			),
			(
				UnitKind::Subquery {
					stored: true,
				},
				RegionKind::Subquery,
				false,
			),
			(UnitKind::ClosureBody, RegionKind::ClosureBody, false),
			(UnitKind::Guard, RegionKind::PermissionFrame, false),
		];
		for (unit, top, checked) in units {
			let (ir, _) = bound_as("RETURN 1", unit.clone()).await.expect("the unit binds");
			assert_eq!(top_kind(&ir), top, "{unit:?}");
			assert_eq!(target_kinds(&ir), vec![top], "{unit:?}");
			let (ir, _) = bound_as("BREAK", unit.clone()).await.expect("the unit binds");
			assert_eq!(raises(&ir), vec![RaiseKind::InvalidControlFlow], "{unit:?}");
			let (_, rejection) = bound_as(WRITE_IN_WHERE, unit.clone()).await.expect("binds");
			assert_eq!(rejection.is_some(), checked, "{unit:?}");
		}
	}

	/// A guard is a value under a permission frame that takes the row after the timestamp: `$this`,
	/// `$self` and `$value` are that row and no name of the guard is read as a free name, a
	/// `RETURN` ends it and the constraint check never judges it.
	#[tokio::test]
	async fn a_guard_binds_its_row_names_to_the_argument_after_the_timestamp() {
		let (ir, rejection) =
			bound_as("RETURN [$this, $self, $value]", UnitKind::Guard).await.expect("binds");
		assert!(rejection.is_none());
		assert_eq!(top_kind(&ir), RegionKind::PermissionFrame);
		let args = &ir.block(ir.region(ir.top()).expect("top").blocks[0]).expect("block").args;
		assert_eq!(args.len(), 2, "the timestamp and the row");
		assert_eq!(ir.value(args[0]).map(|value| value.ty.clone()), Some(timestamp_arg()));
		assert!(
			!all_ops(&ir)
				.into_iter()
				.any(|op| matches!(kind_of(&ir, op), OpKind::Expr(ExprOp::FreeParam { .. }))),
			"the row names are the argument, not free names"
		);
		let (ir, rejection) = bound_as(WRITE_IN_WHERE, UnitKind::Guard).await.expect("binds");
		assert!(rejection.is_none(), "a guard is definer code");
		assert_eq!(top_kind(&ir), RegionKind::PermissionFrame);
	}

	/// Every container planned as a unit other than a request takes the statement timestamp as
	/// the first argument of its top block, and the context binds the clock to it. A function body
	/// takes the row of its caller after it.
	#[tokio::test]
	async fn a_unit_takes_the_statement_timestamp_as_its_first_argument() {
		for (unit, count) in
			[(UnitKind::FunctionBody, 2), (UnitKind::TableRead, 1), (UnitKind::ClosureBody, 1)]
		{
			let (ir, _) = bound_as("{ 1 }", unit.clone()).await.expect("binds");
			let args = &ir.block(ir.region(ir.top()).expect("top").blocks[0]).expect("block").args;
			assert_eq!(args.len(), count, "{unit:?}");
			assert_eq!(
				ir.value(args[0]).map(|value| value.ty.clone()),
				Some(timestamp_arg()),
				"{unit:?}"
			);
		}
	}

	/// A loop body is extracted from a planned container, and a unit holds exactly one statement.
	#[tokio::test]
	async fn a_unit_that_cannot_be_bound_from_a_statement_is_an_invariant() {
		use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
		// The binder reports a broken invariant as an op-local verification failure.
		let invariant = |error: &IrError| {
			matches!(
				error,
				IrError::Invariant { .. }
					| IrError::Verify(VerifyError {
						rule: VerifyRule::PerOp,
						..
					})
			)
		};
		assert!(bound_as("RETURN 1", UnitKind::LoopBody).await.as_ref().is_err_and(invariant));
		let env = Env::new();
		let parsed = crate::syn::parse("RETURN 1; RETURN 2").expect("parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let input = RequestInput {
			statements: &statements,
			scope: request_scope(),
			unit: UnitKind::FunctionBody,
			serves_late: false,
			late_bound: false,
		};
		assert!(bind_request_checked(&input, &env.cx()).await.as_ref().is_err_and(invariant));
	}

	/// A statement handed over bound is bound as the statement the parser would have produced,
	/// and the planner's refusal of the whole statement is the statement's value.
	#[tokio::test]
	async fn a_logical_statement_binds_as_the_parsed_one_does() {
		use crate::expr::logical::lower;
		let env = Env::new();
		let parsed = crate::syn::parse("RETURN 1 + 2").expect("parses").expressions;
		let [surrealdb_expr::parser::TopLevelExpr::Expr(expr)] = parsed.as_slice() else {
			panic!("one expression statement");
		};
		let op = lower(expr.clone()).expect("the statement has a logical form");
		let bound = [RequestStatement::Logical(&op)];
		let from_logical = bind_request(&RequestInput::request(&bound, request_scope()), &env.cx())
			.await
			.expect("binds");
		let from_text = bind("RETURN 1 + 2").await;
		let print = |ir: &Ir| {
			crate::exec::ir::core::print::print(
				ir,
				ir.top(),
				crate::exec::ir::core::print::PrintOpts::default(),
			)
		};
		assert_eq!(print(&from_logical), print(&from_text));
	}
}
