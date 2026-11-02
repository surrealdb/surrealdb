//! The shared interface types of the IR.
//!
//! Every type here is used by more than one module, so each is defined once in this module.
//! Nothing here depends on any particular implementation.

pub(crate) mod bounds;
pub(crate) mod catalog;
pub(crate) mod choose;
pub(crate) mod conversion;
pub(crate) mod effects;
pub(crate) mod egraph;
pub(crate) mod estimate;
pub(crate) mod fallibility;
pub(crate) mod fold;
pub(crate) mod frame;
pub(crate) mod identity;
pub(crate) mod ids;
pub(crate) mod r#loop;
pub(crate) mod policy;
pub(crate) mod premise;
pub(crate) mod region_kind;
pub(crate) mod statements;
pub(crate) mod types;
pub(crate) mod unit;

// These re-exports are the surface other modules import from. Not every item is used in every
// build, so unused-import lints are allowed.
#[allow(unused_imports)]
pub(crate) use catalog::{
	BackendCaps, EffectCx, PhysCx, PhysicalCatalog, SemanticCatalog, SourceKey,
};
#[allow(unused_imports)]
pub(crate) use choose::{
	ArmBody, ArmEquivalence, ArmLabel, ArmSpec, ChooseGranularity, ChooseOrigin, ChooseSpec,
	ExhaustiveFact, GenericArm, Guard, IndexRef, MAX_CHOOSE_ARMS, ValueKindClass,
};
#[allow(unused_imports)]
pub(crate) use conversion::{
	Dialect, DialectSet, EliminationReason, ExprDialect, LOGICAL_PHASE, LegalityTarget,
	LogicalDialect, PHYSICAL_PHASE, Phase, PhysDialect,
};
#[allow(unused_imports)]
pub(crate) use effects::{
	Determinism, ElemPath, FieldSet, IrFootprint, IrResource, KeyOf, Multiplicity, OpEffects,
	Rollback,
};
#[allow(unused_imports)]
pub(crate) use egraph::{
	EGRAPH_DEFAULT, EgraphLimits, EgraphRule, EgraphRuleSet, KindClass, PhysFactReq, RuleDialect,
	SideCondition,
};
#[allow(unused_imports)]
pub(crate) use estimate::{Estimate, EstimateSource, SizeBucket};
#[allow(unused_imports)]
pub(crate) use fallibility::{ContextDep, Fallibility, FallibilityClass};
#[allow(unused_imports)]
pub(crate) use fold::{
	FoldConsumer, FoldOutcome, FoldRequest, NotFoldable, NotYetFolder, PlanTimeFolder,
};
#[allow(unused_imports)]
pub(crate) use frame::{AuthorityFrame, FrameTable, Snapshot};
#[allow(unused_imports)]
pub(crate) use identity::{AttrDigest, ValueKey};
#[allow(unused_imports)]
pub(crate) use r#loop::{
	ApplyCut, BarrierCause, Exit, ExitCause, LoopMode, LoopRefusal, SourceDelivery,
};
#[allow(unused_imports)]
pub(crate) use policy::{LoopFacts, PlacementAttr, PlacementBoundary};
#[allow(unused_imports)]
pub(crate) use premise::{
	CatalogObjectKey, CatalogScope, CatalogVersionVector, FrameClass, OpenRegionShape, Premise,
	PremiseCheck, PremiseSet, PremiseSource, ProgramKey, RegionContext, ShapeDigest, SlotId,
	TxnContext, ValueBucket,
};
#[allow(unused_imports)]
pub(crate) use region_kind::{
	Admission, ClauseRole, Construct, ErrorScope, FieldRole, FrameRule, LoopRule, RegionKind,
	RegionProperties, ReturnRule, Section4bRow, StatementCommit, StoredRole, TxnKind, Visibility,
};
#[allow(unused_imports)]
pub(crate) use statements::{
	AccessDurationShape, AccessOp, AccessSecrets, AccessSubjectShape, AccessTypeShape, AlterAccess,
	AlterApiClauseShape, AlterDefaultShape, AlterEvent, AlterField, AlterFunction, AlterObject,
	AlterTable, AlterTemplate, AlterUser, ApiActionShape, ApiConfigShape, BearerShape, ConfigShape,
	ContentMode, DdlChecks, DdlGuard, DefaultConfigShape, DefaultShape, DefineAccess, DefineApi,
	DefineField, DefineFunction, DefineObject, DefineTable, DefineUser, DefinitionTemplate,
	EnumEdit, IndexColumn, IndexSpec, InfoSubject, JwtIssueShape, JwtShape, JwtVerifyShape,
	LiveShape, MatchBindings, MatchColumnShape, MatchData, MatchInsertEdge, MatchInsertNode,
	MatchOutputShape, MatchPattern, MiddlewareShape, OnDeleteShape, OperandSlot, OutputShape,
	PermissionShape, PermissionsShape, Place, PlacePart, RecordAccessShape, ReferenceShape,
	RegionSlot, RemoveObject, ShowSince, TemplateHole, UnaddressedPart, UserDurationShape,
	UserSecrets, ViewShape, WriteFlags, WriteForm,
};
#[allow(unused_imports)]
pub(crate) use types::{FactSource, HintSource, TypeKnowledge};
#[allow(unused_imports)]
pub(crate) use unit::{LoadUnitSpec, UnitKind, UnitStaticKey};
