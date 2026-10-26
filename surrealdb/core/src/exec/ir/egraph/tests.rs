//! Tests of the e-graph rewrite path.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use egg::{EGraph, Id};
use smallvec::{SmallVec, smallvec};
use surrealdb_strand::{Strand, TableName};
use web_time::{Duration, Instant};

use super::analysis::{IrAnalysis, SeedInfo, add_with_facts, merge_facts};
use super::decl::{
	AttrSrc, BinderId, BuildCx, ClassFacts, ClassRef, CostHook, CostNode, EGRAPH_LITERAL_MAX_BYTES,
	ENodeSnapshot, EgraphView, EpochKey, HeadPat, LeafId, MATCH_CAP_PER_RULE, Match, NodeIdentity,
	NodeShape, NodeSpec, NodeSpecShape, OrderSig, Pat, PayloadId, PhysTableFacts, PlanCost, Rhs,
	RuleDecl, RuleTier, StmtMap, TagBitmap, Tmpl, tag_index,
};
use super::elaborate::{
	Abort, Basis, ElabPlan, ElabRule, Elimination, NoSurgery, Operand, StatementSurgery, Step,
	apply, plan, write_logical,
};
use super::extract::{
	Candidate, Term, best_costs, extract_phys, logical_candidates, structural_key,
};
use super::guards::{
	UnionWhy, deterministic, footprint_disjoint, guarded_union, holds, infallible, kind_admits,
	pure,
};
use super::import::{
	ImportStop, Imported, IslandRoot, OpRole, RegionRole, Renumber, SeedCopy, import, import_seed,
	importable, islands, literal_exceeds,
};
use super::lang::{ENode, region_kind_key, snapshot_of};
use super::reg::offer;
use super::rule::{NodeKey, RCheck, RuleReject, RuleSet, check, node_key, registered, tags_in};
use super::rules_logical::{
	AND_FALSE_TRUTHY, CATALOGUE, MUL_ZERO_INT_L, MUL_ZERO_INT_R, OR_TRUE_TRUTHY, REGISTERED,
	tags_of,
};
use super::runner::{
	Budget, ROUND_L_ITERATIONS, RoundReport, TierFilter, resaturate_needed, saturate,
};
use super::surgery::CONTAINER_SURGERY;
use super::{
	EgraphMode, EgraphOutcome, FallbackReason, ForcedPlanning, Gate, Mode, OrderLicence,
	PlanStmtCx, Scan, SeedFuture, SeedHost, SkipReason, counts, decide, gate, is_choice_point,
	plan_statement,
};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::loop_legality;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope, Walk, WalkOrder};
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::parse::parse;
use crate::exec::ir::core::pattern::Limit;
use crate::exec::ir::core::print::{PrintOpts, print};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Provenance, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::verify::{VerifyAt, verify};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag, NestedStrategy, NowWhich, ShortCircuitOp};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::bounds::FOLD_MAX_BYTES;
use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};
use crate::exec::ir::iface::conversion::{EliminationReason, LOGICAL_PHASE, Phase};
use crate::exec::ir::iface::effects::{Determinism, FieldSet, IrFootprint, IrResource, KeyOf};
use crate::exec::ir::iface::egraph::{
	EGRAPH_DEFAULT, EgraphLimits, EgraphRuleSet, KindClass, PhysFactReq, RuleDialect, SideCondition,
};
use crate::exec::ir::iface::fallibility::{Fallibility, FallibilityClass};
use crate::exec::ir::iface::fold::{FoldConsumer, FoldRequest};
use crate::exec::ir::iface::frame::{AuthorityFrame, FrameTable};
use crate::exec::ir::iface::identity::AttrDigest;
use crate::exec::ir::iface::ids::{
	BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId, WriteEpoch,
};
use crate::exec::ir::iface::r#loop::{LoopMode, LoopRefusal};
use crate::exec::ir::iface::policy::LoopFacts;
use crate::exec::ir::iface::region_kind::{
	ClauseRole, ErrorScope, FieldRole, RegionKind, StatementCommit, StoredRole, TxnKind,
};
use crate::exec::ir::iface::types::{FactSource, HintSource, TypeKnowledge};
use crate::exec::ir::logical::ops::{
	LetAttrs, LetBinding, LogOp, LogTag, LoopKind, LoopSink, SourceOrder,
};
use crate::exec::ir::logical::verify::timestamp_arg;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::phys::ops::{PhysOp, PhysTag};
use crate::exec::ir::registry;
use crate::exec::vm::program::UnitFrame;
use crate::expr::logical::{EffectSet, EvalFrame, JoinKind, RelationAlias};
use crate::expr::{BinaryOperator, Function, Kind, With};
use crate::val::{Number, Value};

#[test]
fn tag_index_matches_all() {
	let mut set = TagBitmap::default();
	for (position, tag) in OpTag::ALL.iter().enumerate() {
		assert_eq!(tag_index(*tag), position);
		assert_eq!(OpTag::ALL[tag_index(*tag)], *tag);
		assert!(!set.contains(*tag));
		set.insert(*tag);
		assert!(set.contains(*tag));
	}
	assert!(set.intersects(&set.clone()));
}

#[test]
fn enode_sizes() {
	assert!(std::mem::size_of::<NodeIdentity>() <= 24);
	assert!(std::mem::size_of::<ENode>() <= 48);
}

// The e-graph a run owns crosses an await point while the planner holds it.
const _: () = {
	const fn assert_send<T: Send>() {}
	assert_send::<EGraph<ENode, IrAnalysis>>();
};

/// Facts of a pure, infallible, deterministic class in frame `frame` and snapshot 0.
fn blank(frame: u16) -> ClassFacts {
	ClassFacts {
		effects: EffectSet::PURE,
		fallibility: Fallibility::INFALLIBLE,
		determinism: Determinism::Deterministic,
		frame: Some(FrameId::new(frame)),
		snapshot: Some(SnapshotId::new(0)),
		reads_written: false,
		ty: TypeKnowledge::Unknown,
		free_binders: SmallVec::new(),
		in_permission_frame: false,
		provenance: SmallVec::new(),
		ordinal: 0,
		seed_order: None,
		total_order_ok: false,
		estimate: None,
	}
}

/// A view over hand-built facts, class `n` being `classes[n]`.
struct FactsView {
	classes: Vec<ClassFacts>,
}

impl EgraphView for FactsView {
	fn find(&self, c: ClassRef) -> ClassRef {
		c
	}

	fn facts(&self, c: ClassRef) -> &ClassFacts {
		&self.classes[c.0 as usize]
	}

	fn nodes(&self, _c: ClassRef) -> SmallVec<[ENodeSnapshot; 4]> {
		SmallVec::new()
	}

	fn payload(&self, _p: PayloadId) -> Option<&OpKind> {
		None
	}

	fn table(&self, _t: &TableName) -> Option<&PhysTableFacts> {
		None
	}

	fn total_order_ok(&self, c: ClassRef) -> bool {
		self.facts(c).total_order_ok
	}

	fn is_seed(&self, _n: &ENodeSnapshot) -> bool {
		false
	}

	fn add(&mut self, spec: NodeSpec) -> ClassRef {
		spec.frame_of
	}
}

/// Whether `cond` holds with `?x` bound to `classes[0]` and, when there is one, `?y` to
/// `classes[1]`.
fn holds_over(cond: SideCondition, classes: Vec<ClassFacts>) -> bool {
	let mut vars = SmallVec::new();
	vars.push(("x", ClassRef(0)));
	if classes.len() > 1 {
		vars.push(("y", ClassRef(1)));
	}
	let m = Match {
		root: ClassRef(0),
		vars,
		nodes: SmallVec::new(),
	};
	holds(
		&cond,
		&m,
		&FactsView {
			classes,
		},
	)
}

fn with(f: impl FnOnce(&mut ClassFacts)) -> ClassFacts {
	let mut facts = blank(0);
	f(&mut facts);
	facts
}

fn fact(kind: Kind) -> TypeKnowledge {
	TypeKnowledge::Fact {
		kind,
		source: FactSource::Literal,
	}
}

#[test]
fn side_condition_semantics() {
	// Infallible: no failure dependency at all.
	assert!(holds_over(SideCondition::Infallible("x"), vec![blank(0)]));
	for failing in [Fallibility::VALUE, Fallibility::CONTEXT, Fallibility::DATA] {
		let facts = with(|f| f.fallibility = failing);
		assert!(!holds_over(SideCondition::Infallible("x"), vec![facts]));
	}
	assert!(infallible(Fallibility::INFALLIBLE));
	assert!(!infallible(Fallibility::DATA));

	// FallibilityAtMost: the strongest dependency is at most the class.
	let at_most = SideCondition::FallibilityAtMost("x", FallibilityClass::Value);
	assert!(holds_over(at_most, vec![blank(0)]));
	assert!(holds_over(at_most, vec![with(|f| f.fallibility = Fallibility::VALUE)]));
	assert!(!holds_over(at_most, vec![with(|f| f.fallibility = Fallibility::CONTEXT)]));
	assert!(!holds_over(at_most, vec![with(|f| f.fallibility = Fallibility::DATA)]));

	// Pure: a function of the row and literals; a clock read is not.
	assert!(holds_over(SideCondition::Pure("x"), vec![blank(0)]));
	assert!(holds_over(SideCondition::Pure("x"), vec![with(|f| f.effects = EffectSet::READS_ROW)]));
	assert!(!holds_over(
		SideCondition::Pure("x"),
		vec![with(|f| f.effects = EffectSet::READS_ROW.union(EffectSet::CLOCK))]
	));
	assert!(!holds_over(
		SideCondition::Pure("x"),
		vec![with(|f| f.effects = EffectSet::READS_DATA)]
	));
	assert!(pure(EffectSet::PURE));
	assert!(pure(EffectSet::READS_ROW));
	assert!(!pure(EffectSet::CLOCK));
	assert!(!pure(EffectSet::READS_ROW.union(EffectSet::READS_PRINCIPAL)));

	// Deterministic: a read of stored data counts only where no write reaches it.
	for (class, reads_written, expected) in [
		(Determinism::Deterministic, false, true),
		(Determinism::Deterministic, true, true),
		(Determinism::PerStatement, true, true),
		(Determinism::WithinStatementUnlessWritten, false, true),
		(Determinism::WithinStatementUnlessWritten, true, false),
		(Determinism::NonDeterministic, false, false),
	] {
		let facts = with(|f| {
			f.determinism = class;
			f.reads_written = reads_written;
		});
		assert_eq!(holds_over(SideCondition::Deterministic("x"), vec![facts]), expected);
		assert_eq!(deterministic(class, reads_written), expected);
	}

	// SameFrame: equal and known.
	let same = SideCondition::SameFrame("x", "y");
	assert!(holds_over(same, vec![blank(0), blank(0)]));
	assert!(!holds_over(same, vec![blank(0), blank(1)]));
	assert!(!holds_over(same, vec![with(|f| f.frame = None), with(|f| f.frame = None)]));

	// NotPermissionConjunct.
	assert!(holds_over(SideCondition::NotPermissionConjunct("x"), vec![blank(0)]));
	assert!(!holds_over(
		SideCondition::NotPermissionConjunct("x"),
		vec![with(|f| f.in_permission_frame = true)]
	));

	// FootprintDisjointFromStatementWrites and NoWriteBetween read `reads_written`.
	let disjoint = SideCondition::FootprintDisjointFromStatementWrites("x");
	assert!(holds_over(disjoint, vec![blank(0)]));
	assert!(!holds_over(disjoint, vec![with(|f| f.reads_written = true)]));
	let between = SideCondition::NoWriteBetween("x", "y");
	assert!(holds_over(between, vec![blank(0), blank(0)]));
	assert!(!holds_over(between, vec![with(|f| f.reads_written = true), blank(0)]));
	assert!(!holds_over(between, vec![blank(0), with(|f| f.reads_written = true)]));

	// ConsumedAsTruthy is proved by the pattern's shape.
	assert!(holds_over(SideCondition::ConsumedAsTruthy("x"), vec![blank(0)]));

	// TypeFact: a guaranteed kind of the class, never a hint.
	let int = SideCondition::TypeFact("x", KindClass::Int);
	assert!(holds_over(int, vec![with(|f| f.ty = fact(Kind::Int))]));
	assert!(!holds_over(int, vec![with(|f| f.ty = fact(Kind::Float))]));
	assert!(!holds_over(int, vec![with(|f| f.ty = fact(Kind::String))]));
	assert!(!holds_over(int, vec![blank(0)]));
	assert!(!holds_over(
		int,
		vec![with(|f| {
			f.ty = TypeKnowledge::Hint {
				kind: Kind::Int,
				source: HintSource::Annotation,
			}
		})]
	));
	let number = SideCondition::TypeFact("x", KindClass::Number);
	for kind in [Kind::Int, Kind::Float, Kind::Decimal, Kind::Number] {
		assert!(holds_over(number, vec![with(|f| f.ty = fact(kind))]));
	}
	assert!(!holds_over(number, vec![with(|f| f.ty = fact(Kind::String))]));
	let any = SideCondition::TypeFact("x", KindClass::Any);
	assert!(!holds_over(any, vec![with(|f| f.ty = fact(Kind::Int))]));
	assert!(!holds_over(any, vec![with(|f| f.ty = fact(Kind::Any))]));
	assert!(kind_admits(KindClass::Bool, &Kind::Bool));
	assert!(kind_admits(KindClass::String, &Kind::String));
	assert!(kind_admits(KindClass::Object, &Kind::Object));
	assert!(kind_admits(KindClass::Array, &Kind::Array(Box::new(Kind::Int), None)));
	assert!(kind_admits(KindClass::Record, &Kind::Record(Vec::new())));
	assert!(!kind_admits(KindClass::Array, &Kind::Object));

	// PhysicalFact: no table fact answers, so the guard refuses.
	for req in [PhysFactReq::IndexOn("x"), PhysFactReq::UniqueKey("x"), PhysFactReq::OrderedBy("x")]
	{
		assert!(!holds_over(SideCondition::PhysicalFact(req), vec![blank(0)]));
	}

	// A variable the match does not bind is false, never a panic.
	assert!(!holds_over(SideCondition::Infallible("absent"), vec![blank(0)]));
	assert!(!holds_over(SideCondition::SameFrame("x", "absent"), vec![blank(0)]));
	assert!(!holds_over(SideCondition::NoWriteBetween("absent", "x"), vec![blank(0)]));
	assert!(!holds_over(SideCondition::TypeFact("absent", KindClass::Int), vec![blank(0)]));
}

fn data_resource(table: &str, fields: FieldSet) -> IrResource {
	IrResource::Data {
		table: Some(TableName::from(table)),
		fields,
		key: KeyOf::Any,
	}
}

fn some_fields(names: &[&str]) -> FieldSet {
	FieldSet::Some(names.iter().map(|name| Strand::from(*name)).collect())
}

#[test]
fn footprint_disjoint_follows_the_row_fields() {
	let none = IrFootprint {
		reads: SmallVec::new(),
		writes: SmallVec::new(),
	};
	let writes = IrFootprint {
		reads: SmallVec::new(),
		writes: smallvec![data_resource("person", some_fields(&["f"]))],
	};
	let person = TableName::from("person");
	let other = TableName::from("other");

	// A read of the row's field `f` of `person` meets the write of `f`.
	assert!(!footprint_disjoint(&none, Some((&person, &some_fields(&["f"]))), &writes));
	assert!(!footprint_disjoint(&none, Some((&person, &FieldSet::All)), &writes));
	// Another field, or another table, does not.
	assert!(footprint_disjoint(&none, Some((&person, &some_fields(&["g"]))), &writes));
	assert!(footprint_disjoint(&none, Some((&other, &some_fields(&["f"]))), &writes));
	// No row source and no read of the footprint.
	assert!(footprint_disjoint(&none, None, &writes));
	// A read the footprint names meets the same write.
	let reads = IrFootprint {
		reads: smallvec![data_resource("person", FieldSet::All)],
		writes: SmallVec::new(),
	};
	assert!(!footprint_disjoint(&reads, None, &writes));
	// A statement that writes nothing meets no read.
	assert!(footprint_disjoint(&reads, Some((&person, &FieldSet::All)), &none));
	// An unknown write meets every read.
	let unknown = IrFootprint {
		reads: SmallVec::new(),
		writes: smallvec![IrResource::Unknown],
	};
	assert!(!footprint_disjoint(&reads, None, &unknown));
}

#[test]
fn class_merge_reports_exact_change() {
	let mut a = blank(0);
	let did = merge_facts(&mut a, &blank(0));
	assert!(!did.0 && !did.1, "equal facts change neither side");

	// Joining a class with more effects changes `a` only: the result is `b`.
	let mut a = blank(0);
	let did = merge_facts(&mut a, &with(|f| f.effects = EffectSet::READS_ROW));
	assert!(did.0 && !did.1);
	assert_eq!(a.effects, EffectSet::READS_ROW);

	// Joining a class with fewer effects changes `b`'s view only.
	let mut a = with(|f| f.effects = EffectSet::READS_ROW);
	let did = merge_facts(&mut a, &blank(0));
	assert!(!did.0 && did.1);

	// Each field joins by its own rule: minimum ordinal, intersection of binders, AND of order.
	let mut a = with(|f| {
		f.ordinal = 7;
		f.free_binders = smallvec![BinderId(1), BinderId(2)];
		f.total_order_ok = true;
	});
	let did = merge_facts(
		&mut a,
		&with(|f| {
			f.ordinal = 3;
			f.free_binders = smallvec![BinderId(2), BinderId(3)];
			f.total_order_ok = true;
		}),
	);
	assert!(did.0 && did.1);
	assert_eq!(a.ordinal, 3);
	assert_eq!(a.free_binders.as_slice(), &[BinderId(2)]);
	assert!(a.total_order_ok);
}

/// An op node with no kids: payload `payload`, in `frame` and snapshot `snapshot`.
fn op_node(payload: u32, frame: u16, snapshot: u16) -> ENode {
	ENode {
		id: NodeIdentity {
			shape: NodeShape::Op {
				tag: OpTag::Expr(ExprTag::Literal),
				payload: PayloadId(payload),
			},
			frame: FrameId::new(frame),
			snapshot: SnapshotId::new(snapshot),
			epoch: EpochKey::None,
		},
		kids: SmallVec::new(),
	}
}

fn leaf_node(leaf: u32, frame: u16) -> ENode {
	ENode {
		id: NodeIdentity {
			shape: NodeShape::Opaque {
				leaf: LeafId(leaf),
			},
			frame: FrameId::new(frame),
			snapshot: SnapshotId::new(0),
			epoch: EpochKey::None,
		},
		kids: SmallVec::new(),
	}
}

fn graph() -> EGraph<ENode, IrAnalysis> {
	let mut analysis = IrAnalysis::new();
	analysis.frames = vec![false, false];
	EGraph::new(analysis)
}

fn add(eg: &mut EGraph<ENode, IrAnalysis>, node: ENode, facts: ClassFacts) -> Id {
	add_with_facts(eg, node, facts)
}

#[test]
fn union_guards() {
	let rule = |root: Id| UnionWhy::Rule {
		root,
	};

	// A frame or a snapshot that differs refuses the union.
	let mut eg = graph();
	let a = add(&mut eg, op_node(0, 0, 0), blank(0));
	let b = add(&mut eg, op_node(1, 1, 0), blank(1));
	assert!(!guarded_union(&mut eg, a, b, rule(a)));
	assert_eq!(eg.analysis.union_refused, 1);
	assert_ne!(eg.find(a), eg.find(b));
	let c = add(&mut eg, op_node(2, 0, 1), with(|f| f.snapshot = Some(SnapshotId::new(1))));
	assert!(!guarded_union(&mut eg, a, c, rule(a)));
	assert_eq!(eg.analysis.union_refused, 2);

	// Two opaque leaves are never joined; an opaque leaf and an op may be.
	let l0 = add(&mut eg, leaf_node(0, 0), blank(0));
	let l1 = add(&mut eg, leaf_node(1, 0), blank(0));
	assert!(!guarded_union(&mut eg, l0, l1, rule(l0)));
	assert_eq!(eg.analysis.union_refused, 3);
	assert!(guarded_union(&mut eg, l0, a, rule(l0)));
	eg.rebuild();
	assert_eq!(eg.find(l0), eg.find(a));
	assert!(!guarded_union(&mut eg, l1, l0, rule(l1)));
	assert_eq!(eg.analysis.union_refused, 4);

	// The joined class may not depend on a binder the matched root does not.
	let mut eg = graph();
	let root = add(&mut eg, op_node(0, 0, 0), blank(0));
	let bound = add(&mut eg, op_node(1, 0, 0), with(|f| f.free_binders = smallvec![BinderId(0)]));
	assert!(!guarded_union(&mut eg, root, bound, rule(root)));
	assert_eq!(eg.analysis.union_refused, 1);
	let seeing = add(&mut eg, op_node(2, 0, 0), with(|f| f.free_binders = smallvec![BinderId(0)]));
	let bound_too =
		add(&mut eg, op_node(3, 0, 0), with(|f| f.free_binders = smallvec![BinderId(0)]));
	assert!(guarded_union(&mut eg, seeing, bound_too, rule(seeing)));
	eg.rebuild();
	assert_eq!(eg.analysis.union_refused, 1);

	// A pair already in one class is not a refusal and joins nothing.
	assert!(!guarded_union(&mut eg, seeing, bound_too, rule(seeing)));
	assert_eq!(eg.analysis.union_refused, 1);

	// A union the guards admit joins the classes, and the join takes the minimum ordinal.
	let mut eg = graph();
	let x = add(&mut eg, op_node(0, 0, 0), with(|f| f.ordinal = 5));
	let y = add(&mut eg, op_node(1, 0, 0), with(|f| f.ordinal = 2));
	assert!(guarded_union(&mut eg, x, y, rule(x)));
	eg.rebuild();
	assert_eq!(eg.find(x), eg.find(y));
	assert_eq!(eg[x].data.ordinal, 2);
	assert_eq!(eg.analysis.union_refused, 0);
}

/// A seed whose replacement is not a value of the incumbent or of a candidate copy is a defect of
/// the seeding: the union guard refuses it, and a debug build asserts.
#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "U4"))]
fn union_guard_u4_refuses_a_foreign_seed() {
	let mut eg = graph();
	let a = add(&mut eg, op_node(0, 0, 0), blank(0));
	let b = add(&mut eg, op_node(1, 0, 0), blank(0));
	let foreign = UnionWhy::Seed {
		from_known: false,
		to_phys_of_same_copy: true,
	};
	assert!(!guarded_union(&mut eg, a, b, foreign));
	assert_eq!(eg.analysis.union_refused, 1);
	let logical_to = UnionWhy::Seed {
		from_known: true,
		to_phys_of_same_copy: false,
	};
	assert!(!guarded_union(&mut eg, a, b, logical_to));
	assert_eq!(eg.analysis.union_refused, 2);
}

#[test]
fn a_seed_union_the_guards_admit_joins() {
	let mut eg = graph();
	let a = add(&mut eg, op_node(0, 0, 0), blank(0));
	let b = add(&mut eg, op_node(1, 0, 0), blank(0));
	let seed = UnionWhy::Seed {
		from_known: true,
		to_phys_of_same_copy: true,
	};
	assert!(guarded_union(&mut eg, a, b, seed));
	assert_eq!(eg.analysis.union_refused, 0);
}

// ---------------------------------------------------------------------------------------------
// Import
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

fn spec(kind: OpKind, operands: &[ValueId], results: usize, frame: FrameId) -> OpSpec {
	OpSpec {
		kind,
		operands: operands.iter().copied().collect(),
		results: vec![TypeKnowledge::Unknown; results].into_iter().collect(),
		frame,
		snapshot: SnapshotId::new(0),
		loc: Location {
			stmt: 0,
			span: None,
			origin: LocOrigin::Synthesized,
		},
	}
}

/// One statement of a request, built through the rewriter. Each helper answers `NotYet` when the
/// container's bodies are missing, so a test over a fixture fails at run time.
struct Fixture {
	ir: Ir,
	facts: FactCache,
	stmt: OpId,
	block: BlockId,
}

impl Fixture {
	/// A `logical.stmt` whose region holds the statement timestamp as its one block argument.
	fn new() -> Self {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let stmt = rw
			.create(
				InsertPoint::End(top),
				spec(
					OpKind::Logical(LogOp::Stmt {
						commit: StatementCommit::Auto,
						control: None,
					}),
					&[],
					1,
					FrameId::new(0),
				),
			)
			.expect("the statement is created");
		let (_, block) = rw
			.create_region(stmt, RegionKind::Statement(StatementCommit::Auto), &[timestamp_arg()])
			.expect("the statement region is created");
		Self {
			ir,
			facts,
			stmt,
			block,
		}
	}

	fn add_in(
		&mut self,
		block: BlockId,
		kind: OpKind,
		operands: &[ValueId],
		results: usize,
		frame: FrameId,
	) -> OpId {
		Rewriter::new(&mut self.ir, &mut self.facts)
			.create(InsertPoint::End(block), spec(kind, operands, results, frame))
			.expect("the op is created")
	}

	/// Adds an op to the statement's block, in the default frame.
	fn add(&mut self, kind: OpKind, operands: &[ValueId], results: usize) -> OpId {
		self.add_in(self.block, kind, operands, results, FrameId::new(0))
	}

	/// Ends the statement's block with the yield that terminates it.
	fn close(&mut self) {
		self.add(closing_yield(), &[], 0);
	}

	/// The first result of `op`.
	fn result(&self, op: OpId) -> ValueId {
		self.ir.op(op).expect("the op exists").results[0]
	}

	/// The statement timestamp.
	fn timestamp(&self) -> ValueId {
		self.ir.block(self.block).expect("the block exists").args[0]
	}

	/// A region of `kind` with `args` block arguments, owned by `owner`.
	fn region(&mut self, owner: OpId, kind: RegionKind, args: usize) -> (RegionId, BlockId) {
		Rewriter::new(&mut self.ir, &mut self.facts)
			.create_region(owner, kind, &vec![TypeKnowledge::Unknown; args])
			.expect("the region is created")
	}

	fn permission_frame(&mut self) -> FrameId {
		Rewriter::new(&mut self.ir, &mut self.facts)
			.intern_frame(AuthorityFrame {
				eval: EvalFrame::permission(None),
				unit: UnitFrame::unprincipled(),
			})
			.expect("the frame is interned")
	}

	fn import_with(&mut self, env: &Env, limits: EgraphLimits) -> Result<Imported, ImportStop> {
		import(&self.ir, &mut self.facts, self.stmt, &env.cx(), limits)
	}

	fn import(&mut self, env: &Env) -> Imported {
		self.import_with(env, EGRAPH_DEFAULT).expect("the statement imports")
	}
}

fn literal(value: Value) -> OpKind {
	OpKind::Expr(ExprOp::Literal(Box::new(value)))
}

fn truthy() -> OpKind {
	OpKind::Expr(ExprOp::Truthy)
}

fn closing_yield() -> OpKind {
	OpKind::Expr(ExprOp::Yield)
}

/// The canonical class of `value`.
fn class_of(imp: &Imported, value: ValueId) -> Id {
	imp.class(value).expect("the import gave the value a class")
}

/// The shape of the first node of `class`.
fn shape_of(imp: &Imported, class: Id) -> NodeShape {
	imp.eg[imp.eg.find(class)].nodes[0].id.shape
}

fn is_opaque(shape: NodeShape) -> bool {
	matches!(shape, NodeShape::Opaque { .. })
}

#[test]
fn identity_follows_value_key() {
	let env = Env::new();
	let mut fx = Fixture::new();
	let permission = fx.permission_frame();

	// A permission conjunct and a same-text WHERE conjunct differ in frame: two classes. Two
	// conjuncts of one frame are one.
	let where_true = fx.add(literal(Value::Bool(true)), &[], 1);
	let where_again = fx.add(literal(Value::Bool(true)), &[], 1);
	let permission_true = fx.add_in(fx.block, literal(Value::Bool(true)), &[], 1, permission);

	// A re-bound LET is a new value: the same expression over each binding is two classes, and
	// over one binding is one.
	let bind = |fx: &mut Fixture| {
		fx.add(
			OpKind::Logical(LogOp::Let(Box::new(LetAttrs {
				name: Strand::from("a"),
				binding: LetBinding::Local,
				ty: None,
			}))),
			&[],
			1,
		)
	};
	let first = bind(&mut fx);
	let second = bind(&mut fx);
	let (first_value, second_value) = (fx.result(first), fx.result(second));
	let on_first = fx.add(truthy(), &[first_value], 1);
	let on_first_again = fx.add(truthy(), &[first_value], 1);
	let on_second = fx.add(truthy(), &[second_value], 1);

	// Two sequence advances are two leaves.
	let advance = || OpKind::Expr(ExprOp::call(Function::Normal("sequence::nextval".to_owned())));
	let first_advance = fx.add(advance(), &[], 1);
	let second_advance = fx.add(advance(), &[], 1);

	// Two clock reads of one statement read one timestamp: one class.
	let timestamp = fx.timestamp();
	let first_now = fx.add(
		OpKind::Expr(ExprOp::Now {
			which: NowWhich::Datetime,
		}),
		&[timestamp],
		1,
	);
	let second_now = fx.add(
		OpKind::Expr(ExprOp::Now {
			which: NowWhich::Datetime,
		}),
		&[timestamp],
		1,
	);

	let imp = fx.import(&env);
	let class = |op: OpId| class_of(&imp, fx.result(op));
	assert_ne!(class(where_true), class(permission_true));
	assert_eq!(class(where_true), class(where_again));
	assert_ne!(class(on_first), class(on_second));
	assert_eq!(class(on_first), class(on_first_again));
	assert_ne!(class(first_advance), class(second_advance));
	assert!(is_opaque(shape_of(&imp, class(first_advance))));
	assert!(is_opaque(shape_of(&imp, class(second_advance))));
	assert_eq!(class(first_now), class(second_now));

	// The permission frame is recorded as one, and the default frame is not.
	assert!(imp.eg.analysis.is_permission_frame(permission));
	assert!(!imp.eg.analysis.is_permission_frame(FrameId::new(0)));
	assert!(imp.eg[class(permission_true)].data.in_permission_frame);
	assert!(!imp.eg[class(where_true)].data.in_permission_frame);

	// The incumbent and the import order of each op node.
	assert_eq!(imp.eg.analysis.incumbents.get(&where_true).copied(), Some(class(where_true)));
	assert!(imp.eg.analysis.ordinals[&where_true] < imp.eg.analysis.ordinals[&permission_true]);
}

#[test]
fn write_epochs_are_renumbered_per_graph() {
	let mut fx = Fixture::new();
	let first = fx.add(literal(Value::Null), &[], 1);
	let second = fx.add(literal(Value::None), &[], 1);
	let mut rn = Renumber::default();
	assert_eq!(rn.epoch(None), EpochKey::None);
	// Stable epochs: equal numbers stay equal, and numbers are handed out in the order met.
	assert_eq!(rn.epoch(Some(WriteEpoch::Stable(40))), EpochKey::Stable(0));
	assert_eq!(rn.epoch(Some(WriteEpoch::Stable(7))), EpochKey::Stable(1));
	assert_eq!(rn.epoch(Some(WriteEpoch::Stable(40))), EpochKey::Stable(0));
	// Unique epochs: each op is its own number, and an op keeps its number.
	assert_eq!(rn.epoch(Some(WriteEpoch::Unique(first))), EpochKey::Unique(0));
	assert_eq!(rn.epoch(Some(WriteEpoch::Unique(second))), EpochKey::Unique(1));
	assert_eq!(rn.epoch(Some(WriteEpoch::Unique(first))), EpochKey::Unique(0));
	// A stable and a unique epoch never compare equal.
	assert_ne!(EpochKey::Stable(0), EpochKey::Unique(0));
}

#[test]
fn literal_size_is_bounded_by_the_limit() {
	assert_eq!(EGRAPH_LITERAL_MAX_BYTES, FOLD_MAX_BYTES / 16);
	let text = |len: usize| Value::String(Strand::from("x".repeat(len)));
	assert!(!literal_exceeds(&Value::None));
	assert!(!literal_exceeds(&Value::Bool(true)));
	assert!(!literal_exceeds(&text(EGRAPH_LITERAL_MAX_BYTES)));
	assert!(literal_exceeds(&text(EGRAPH_LITERAL_MAX_BYTES + 1)));
	// A container is measured by its elements, and the measure stops at the first overrun.
	let many = Value::Array(vec![Value::Null; EGRAPH_LITERAL_MAX_BYTES + 1].into());
	assert!(literal_exceeds(&many));
	assert!(!literal_exceeds(&Value::Array(vec![Value::Null; 8].into())));
}

/// Every region kind an op node's region may not be encoded for.
fn always_opaque_kinds() -> Vec<RegionKind> {
	let mut kinds = vec![
		RegionKind::Request,
		RegionKind::Transaction(TxnKind::Explicit),
		RegionKind::Transaction(TxnKind::Session),
		RegionKind::Statement(StatementCommit::Auto),
		RegionKind::Statement(StatementCommit::InTxn),
		RegionKind::Block,
		RegionKind::ValueBlock,
		RegionKind::StatementIf,
		RegionKind::StatementFor,
		RegionKind::ValueFor,
		RegionKind::FunctionBody,
		RegionKind::ClosureBody,
		RegionKind::EventFrame,
		RegionKind::SubscriberFrame,
		RegionKind::ChooseArm,
		RegionKind::Pipeline,
		RegionKind::JoinLookupArm,
		RegionKind::WriteStage,
		RegionKind::CatalogUnit,
		RegionKind::Deadline,
	];
	for role in [FieldRole::Value, FieldRole::Default, FieldRole::Assert, FieldRole::Computed] {
		kinds.push(RegionKind::FieldFrame(role));
	}
	for scope in [ErrorScope::ShapeToNone, ErrorScope::MethodBody, ErrorScope::PermissionCheck] {
		kinds.push(RegionKind::ErrorScope(scope));
	}
	for role in [
		StoredRole::Value,
		StoredRole::Assert,
		StoredRole::Default,
		StoredRole::Computed,
		StoredRole::Permissions,
		StoredRole::When,
		StoredRole::Then,
		StoredRole::Body,
		StoredRole::View,
		StoredRole::IndexColumn,
		StoredRole::Authenticate,
		StoredRole::Context,
		StoredRole::Signup,
		StoredRole::Signin,
		StoredRole::Action,
		StoredRole::Fallback,
		StoredRole::LiveFields,
		StoredRole::LiveCond,
		StoredRole::LiveFetch,
	] {
		kinds.push(RegionKind::StoredClause(role));
	}
	kinds
}

#[test]
fn opaque_regions_never_entered() {
	// The table: the kinds that are never region nodes, and the kinds that may be.
	let opaque = always_opaque_kinds();
	assert!(opaque.iter().all(|kind| !importable(*kind)));
	let importable_kinds = [
		RegionKind::Clause(ClauseRole::Where),
		RegionKind::Clause(ClauseRole::Projection),
		RegionKind::LazyOperand,
		RegionKind::Loop,
		RegionKind::Subquery,
		RegionKind::ValueIf,
		RegionKind::PermissionFrame,
	];
	assert!(importable_kinds.iter().all(|kind| importable(*kind)));

	// Each always-opaque kind, owned by an op, imports as an opaque leaf that holds none of the
	// region's ops.
	let env = Env::new();
	let mut fx = Fixture::new();
	let operand = fx.add(literal(Value::Bool(true)), &[], 1);
	let operand_value = fx.result(operand);
	let mut owned = Vec::new();
	for kind in &opaque {
		let owner = fx.add(truthy(), &[operand_value], 1);
		let (region, block) = fx.region(owner, *kind, 0);
		let inner = fx.add_in(block, literal(Value::Null), &[], 1, FrameId::new(0));
		let inner_value = fx.result(inner);
		fx.add_in(block, closing_yield(), &[inner_value], 0, FrameId::new(0));
		owned.push((owner, region, inner));
	}

	// A `Clause(Where)` whose body holds an op that is not encodable is never entered: its owner
	// is a leaf. One whose body is encodable is a region node.
	let guarded = fx.add(truthy(), &[operand_value], 1);
	let (guarded_region, guarded_block) =
		fx.region(guarded, RegionKind::Clause(ClauseRole::Where), 0);
	let pending = fx.add_in(guarded_block, literal(Value::Null), &[], 1, FrameId::new(0));
	let pending_value = fx.result(pending);
	fx.add_in(guarded_block, OpKind::Logical(LogOp::Sleep), &[pending_value], 0, FrameId::new(0));
	fx.add_in(guarded_block, closing_yield(), &[pending_value], 0, FrameId::new(0));
	let plain = fx.add(truthy(), &[operand_value], 1);
	let (plain_region, plain_block) = fx.region(plain, RegionKind::Clause(ClauseRole::Where), 0);
	let body = fx.add_in(plain_block, literal(Value::Null), &[], 1, FrameId::new(0));
	let body_value = fx.result(body);
	fx.add_in(plain_block, closing_yield(), &[body_value], 0, FrameId::new(0));

	// A literal over the limit is a leaf, and one at the limit is not.
	let text = |len: usize| literal(Value::String(Strand::from("x".repeat(len))));
	let at_limit = fx.add(text(EGRAPH_LITERAL_MAX_BYTES), &[], 1);
	let over_limit = fx.add(text(EGRAPH_LITERAL_MAX_BYTES + 1), &[], 1);

	// An owner the import holds as a leaf never has its region reached; an owner that is a node
	// holds the region as a leaf.
	let classified =
		islands(&fx.ir, &mut fx.facts, crate::exec::ir::analysis::cx::builtin_registry(), fx.stmt);
	for (owner, region, inner) in &owned {
		let expected = match classified.ops.get(owner) {
			Some(OpRole::Node) => Some(&RegionRole::Leaf),
			Some(OpRole::Leaf) => None,
			other => panic!("{owner:?} is classified as {other:?}"),
		};
		assert_eq!(classified.regions.get(region), expected, "{owner:?}");
		assert!(!classified.ops.contains_key(inner), "{owner:?} entered a region it must not");
	}
	assert_eq!(classified.ops.get(&guarded), Some(&OpRole::Leaf));
	assert_eq!(classified.regions.get(&guarded_region), None);
	assert!(!classified.ops.contains_key(&pending));
	assert_eq!(classified.regions.get(&plain_region), Some(&RegionRole::Node));
	assert_eq!(classified.ops.get(&body), Some(&OpRole::Node));
	assert_eq!(classified.ops.get(&over_limit), Some(&OpRole::Leaf));
	assert_eq!(classified.ops.get(&at_limit), Some(&OpRole::Node));

	let imp = fx.import(&env);
	for (owner, _, _) in &owned {
		let class = class_of(&imp, fx.result(*owner));
		if classified.ops.get(owner) == Some(&OpRole::Leaf) {
			assert!(is_opaque(shape_of(&imp, class)), "{owner:?}");
			continue;
		}
		let node = &imp.eg[class].nodes[0];
		assert_eq!(node.kids.len(), 2);
		assert!(is_opaque(shape_of(&imp, node.kids[1])));
	}
	assert!(is_opaque(shape_of(&imp, class_of(&imp, fx.result(guarded)))));
	let plain_node = &imp.eg[class_of(&imp, fx.result(plain))].nodes[0];
	assert!(matches!(shape_of(&imp, plain_node.kids[1]), NodeShape::Region { .. }));
	assert!(is_opaque(shape_of(&imp, class_of(&imp, fx.result(over_limit)))));
	assert!(!is_opaque(shape_of(&imp, class_of(&imp, fx.result(at_limit)))));
}

#[test]
fn import_stops_at_the_node_limit() {
	let env = Env::new();
	let mut fx = Fixture::new();
	fx.add(literal(Value::Bool(true)), &[], 1);
	fx.add(literal(Value::Bool(false)), &[], 1);
	let limits = EgraphLimits {
		nodes: 1,
		..EGRAPH_DEFAULT
	};
	assert!(matches!(fx.import_with(&env, limits), Err(ImportStop::Limit(Limit::Nodes))));
}

// ---------------------------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------------------------

fn build_false(_: &BuildCx<'_>) -> OpKind {
	literal(Value::Bool(false))
}

fn noop_apply(_: &mut dyn EgraphView, _: &Match) -> SmallVec<[ClassRef; 1]> {
	SmallVec::new()
}

/// An admitted rule under the name `name`: the fixtures below each break it in one way.
fn rule(name: &'static str) -> RuleDecl {
	RuleDecl {
		name,
		..AND_FALSE_TRUTHY.clone()
	}
}

/// A template node that builds a literal, in the frame of the node bound as `t`.
const fn built(tag: OpTag, kids: &'static [Tmpl]) -> Tmpl {
	Tmpl::Node {
		tag,
		attrs: AttrSrc::Build {
			name: "n",
			build: build_false,
		},
		frame_of: "t",
		kids,
	}
}

const LITERAL_TAG: OpTag = OpTag::Expr(ExprTag::Literal);
const LOGICAL_KID: Tmpl = built(OpTag::Logical(LogTag::Filter), &[]);

/// `check` fails `decl` at exactly `want`.
fn rejected_by(decl: &RuleDecl, want: RCheck) {
	let got: RuleReject = check(decl, &[]).expect_err("the rule is rejected");
	assert_eq!(got.check, want, "{got}");
	assert_eq!(got.rule, decl.name);
}

/// `truthy(x ? lazy)`, where `?x` and `?y` are variables and `lazy` is `?x` inside a
/// `LazyOperand` region, for the moved-variable fixtures.
const LAZY_LHS: Pat = Pat::Node {
	bind: Some("t"),
	head: HeadPat {
		tag: OpTag::Expr(ExprTag::Truthy),
		attr: None,
	},
	kids: &[Pat::Node {
		bind: None,
		head: HeadPat {
			tag: OpTag::Expr(ExprTag::ShortCircuit),
			attr: None,
		},
		kids: &[
			Pat::Var("y"),
			Pat::Region {
				kind: RegionKind::LazyOperand,
				binder: None,
				kids: &[Pat::Var("x")],
			},
		],
	}],
};

#[test]
fn registration_rejects() {
	// A logical rule never names a physical tag or fact.
	let phys = |tag| Pat::Node {
		bind: None,
		head: HeadPat {
			tag,
			attr: None,
		},
		kids: &[Pat::Var("x")],
	};
	rejected_by(
		&RuleDecl {
			lhs: phys(OpTag::Phys(PhysTag::TableScan)),
			..rule("r1_lhs")
		},
		RCheck::R1,
	);
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Tmpl(built(OpTag::Phys(PhysTag::TableScan), &[])),
			..rule("r1_rhs")
		},
		RCheck::R1,
	);
	rejected_by(
		&RuleDecl {
			when: &[
				SideCondition::PhysicalFact(PhysFactReq::IndexOn("x")),
				SideCondition::Infallible("x"),
				SideCondition::Deterministic("x"),
			],
			..rule("r1_fact")
		},
		RCheck::R1,
	);

	// An implementation rule builds a physical root and no logical node.
	rejected_by(
		&RuleDecl {
			dialect: RuleDialect::Implementation,
			..rule("r2_root")
		},
		RCheck::R2,
	);
	rejected_by(
		&RuleDecl {
			dialect: RuleDialect::Implementation,
			rhs: Rhs::Tmpl(built(OpTag::Phys(PhysTag::TableScan), &[LOGICAL_KID])),
			..rule("r2_logical_kid")
		},
		RCheck::R2,
	);

	// A rule with no side condition states why it is sound.
	rejected_by(
		&RuleDecl {
			when: &[],
			sound: None,
			..rule("r3")
		},
		RCheck::R3,
	);

	// Every name is bound by the left side.
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Tmpl(Tmpl::Var("y")),
			..rule("r4_var")
		},
		RCheck::R4,
	);
	rejected_by(
		&RuleDecl {
			lhs: Pat::Var("x"),
			..rule("r4_root")
		},
		RCheck::R4,
	);
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Tmpl(Tmpl::Node {
				tag: LITERAL_TAG,
				attrs: AttrSrc::Build {
					name: "n",
					build: build_false,
				},
				frame_of: "nope",
				kids: &[],
			}),
			..rule("r4_frame")
		},
		RCheck::R4,
	);
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Tmpl(Tmpl::Node {
				tag: LITERAL_TAG,
				attrs: AttrSrc::CopyOf("t"),
				frame_of: "t",
				kids: &[],
			}),
			..rule("r4_copy_tag")
		},
		RCheck::R4,
	);
	rejected_by(
		&RuleDecl {
			when: &[SideCondition::Infallible("y")],
			..rule("r4_condition")
		},
		RCheck::R4,
	);

	// A dropped variable is infallible and deterministic.
	rejected_by(
		&RuleDecl {
			when: &[SideCondition::Infallible("x")],
			..rule("r5_tmpl")
		},
		RCheck::R5,
	);
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Named {
				name: "n",
				apply: noop_apply,
			},
			drops: &["x"],
			when: &[SideCondition::Infallible("x")],
			..rule("r5_named")
		},
		RCheck::R5,
	);
	let answered = RuleDecl {
		rhs: Rhs::Named {
			name: "n",
			apply: noop_apply,
		},
		answers: &["x"],
		when: &[SideCondition::Infallible("x")],
		..rule("r5_answered")
	};
	assert_eq!(check(&answered, &[]), Ok(()));

	// A duplicated variable is infallible and deterministic.
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Tmpl(built(LITERAL_TAG, &[Tmpl::Var("x"), Tmpl::Var("x")])),
			when: &[SideCondition::Infallible("x")],
			..rule("r6_tmpl")
		},
		RCheck::R6,
	);
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Named {
				name: "n",
				apply: noop_apply,
			},
			duplicates: &["x"],
			when: &[SideCondition::Infallible("x")],
			..rule("r6_named")
		},
		RCheck::R6,
	);

	// A variable moved out of a lazy operand is infallible and deterministic, and
	// `ConsumedAsTruthy` names a direct operand of `expr.truthy`.
	let moved = |when| RuleDecl {
		lhs: LAZY_LHS,
		rhs: Rhs::Tmpl(built(LITERAL_TAG, &[Tmpl::Var("x"), Tmpl::Var("y")])),
		when,
		..rule("r7_moved")
	};
	rejected_by(
		&moved(&[SideCondition::Infallible("y"), SideCondition::Deterministic("y")]),
		RCheck::R7,
	);
	assert_eq!(
		check(
			&moved(&[
				SideCondition::Infallible("x"),
				SideCondition::Deterministic("x"),
				SideCondition::Infallible("y"),
				SideCondition::Deterministic("y"),
			]),
			&[],
		),
		Ok(())
	);
	rejected_by(
		&RuleDecl {
			when: &[
				SideCondition::Infallible("x"),
				SideCondition::Deterministic("x"),
				SideCondition::ConsumedAsTruthy("x"),
			],
			..rule("r7_truthy")
		},
		RCheck::R7,
	);
	let direct = RuleDecl {
		lhs: Pat::Node {
			bind: Some("t"),
			head: HeadPat {
				tag: OpTag::Expr(ExprTag::Truthy),
				attr: None,
			},
			kids: &[Pat::Var("x")],
		},
		rhs: Rhs::Tmpl(Tmpl::Var("x")),
		when: &[SideCondition::ConsumedAsTruthy("x")],
		..rule("r7_direct")
	};
	assert_eq!(check(&direct, &[]), Ok(()));

	// Attributes are built only for the template tags.
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Tmpl(built(OpTag::Expr(ExprTag::Binary), &[])),
			..rule("r8")
		},
		RCheck::R8,
	);

	// A new region that reuses a matched binder is refused.
	rejected_by(
		&RuleDecl {
			rhs: Rhs::Tmpl(Tmpl::Region {
				kind: RegionKind::LazyOperand,
				binder_of: Some("nope"),
				kids: &[],
			}),
			..rule("r9")
		},
		RCheck::R9,
	);

	// Names are unique, and a set holds at most 64 rules.
	let first = rule("same");
	let got = check(&rule("same"), &[&first]).expect_err("the name repeats");
	assert_eq!(got.check, RCheck::R10);
	let many: Vec<&'static RuleDecl> = vec![&AND_FALSE_TRUTHY; 65];
	let got = RuleSet::new(&many).expect_err("65 rules are too many");
	assert_eq!(got.check, RCheck::R10);
	assert!(got.detail.contains("64"), "{got}");

	// The catalogue passes every check.
	assert_eq!(RuleSet::new(CATALOGUE).expect("the catalogue is admitted").len(), CATALOGUE.len());
}

const CATALOGUE_GOLDEN: &str = "\
(rule and_false_truthy :dialect logical :tier normal :needs-total-order false
  :lhs (expr.truthy@t (expr.short_circuit{and} ?x (region LazyOperand (expr.literal{false}))))
  :rhs (expr.literal[build false frame t])
  :when ((infallible ?x) (deterministic ?x))
  :sound \"rooted at expr.truthy, so the result is consumed as a boolean: a false right operand decides it, and the left operand cannot raise, draw or write\")
(rule or_true_truthy :dialect logical :tier normal :needs-total-order false
  :lhs (expr.truthy@t (expr.short_circuit{or} ?x (region LazyOperand (expr.literal{true}))))
  :rhs (expr.literal[build true frame t])
  :when ((infallible ?x) (deterministic ?x))
  :sound \"rooted at expr.truthy, so the result is consumed as a boolean: a true right operand decides it, and the left operand cannot raise, draw or write\")
(rule mul_zero_int_r :dialect logical :tier normal :needs-total-order false
  :lhs (expr.binary@b{mul} ?x (expr.literal{int 0}))
  :rhs (expr.literal[build int 0 frame b])
  :when ((type-fact ?x int) (infallible ?x) (deterministic ?x))
  :sound \"an int zero operand gives int zero when the other operand is an int; 1.5 * 0 is a float and NONE * 0 raises, and the type fact excludes both\")
(rule mul_zero_int_l :dialect logical :tier normal :needs-total-order false
  :lhs (expr.binary@b{mul} (expr.literal{int 0}) ?x)
  :rhs (expr.literal[build int 0 frame b])
  :when ((type-fact ?x int) (infallible ?x) (deterministic ?x))
  :sound \"an int zero operand gives int zero when the other operand is an int; 1.5 * 0 is a float and NONE * 0 raises, and the type fact excludes both\")";

#[test]
fn rule_catalogue_golden() {
	let printed = CATALOGUE.iter().map(|decl| decl.to_string()).collect::<Vec<_>>().join("\n");
	assert_eq!(printed, CATALOGUE_GOLDEN);
	RuleSet::new(CATALOGUE).expect("the catalogue is admitted");
}

#[test]
fn a_named_rule_prints_its_declarations() {
	let named = RuleDecl {
		rhs: Rhs::Named {
			name: "n",
			apply: noop_apply,
		},
		answers: &["x"],
		when: &[SideCondition::Infallible("x")],
		..rule("named")
	};
	let printed = named.to_string();
	assert!(printed.contains(":rhs (apply n)"), "{printed}");
	assert!(printed.contains(":answers (?x)"), "{printed}");
	assert!(printed.contains(":drops ()"), "{printed}");
	assert!(printed.contains(":duplicates ()"), "{printed}");
}

#[test]
fn rule_tag_lists_match_the_declarations() {
	for decl in CATALOGUE {
		let listed = tags_of(decl).expect("the catalogue rule lists its tags");
		assert_eq!(listed, tags_in(decl).as_slice(), "{}", decl.name);
	}
}

/// An admitted rule named `name` in `dialect`, whose right side is an applier that adds nothing.
fn dialect_rule(name: &'static str, dialect: RuleDialect) -> &'static RuleDecl {
	Box::leak(Box::new(RuleDecl {
		name,
		dialect,
		rhs: Rhs::Named {
			name,
			apply: noop_apply,
		},
		..SWAP.clone()
	}))
}

/// `count` admitted rules of `dialect` whose names start with `prefix`.
fn dialect_rules(prefix: &str, dialect: RuleDialect, count: usize) -> Vec<&'static RuleDecl> {
	(0..count)
		.map(|n| {
			let name: &'static str = Box::leak(format!("{prefix}_{n}").into_boxed_str());
			dialect_rule(name, dialect)
		})
		.collect()
}

#[test]
fn offer_registers_each_declaration() {
	let mut set = EgraphRuleSet::new();
	offer(&mut set, CATALOGUE);
	let names: Vec<&str> = set.iter().map(|rule| rule.name()).collect();
	let declared: Vec<&str> = CATALOGUE.iter().map(|decl| decl.name).collect();
	assert_eq!(names, declared);
	registered();
}

#[test]
fn from_sets_keeps_each_set_to_its_dialect() {
	let logical_a = dialect_rule("logical_a", RuleDialect::LogicalRewrite);
	let logical_b = dialect_rule("logical_b", RuleDialect::LogicalRewrite);
	let implementation = dialect_rule("implementation_a", RuleDialect::Implementation);

	assert_eq!(RuleSet::from_sets(CATALOGUE, &[]).expect("the catalogue is logical").len(), 4);

	// Logical rules rank first, then implementation rules.
	let set = RuleSet::from_sets(&[logical_a, logical_b], &[implementation])
		.expect("each rule is in the set of its dialect");
	assert_eq!(set.len(), 3);
	let ranks = |dialect| set.of_dialect(dialect).map(|(rank, _)| rank).collect::<Vec<_>>();
	assert_eq!(ranks(RuleDialect::LogicalRewrite), [0, 1]);
	assert_eq!(ranks(RuleDialect::Implementation), [2]);

	// A rule in the other dialect's set is a dialect-separation failure that names the set.
	let got = RuleSet::from_sets(&[implementation], &[]).expect_err("an implementation rule");
	assert_eq!((got.check, got.rule), (RCheck::R1, "implementation_a"));
	assert!(got.detail.contains("logical"), "{got}");
	let got = RuleSet::from_sets(&[], &[logical_a]).expect_err("a logical rule");
	assert_eq!((got.check, got.rule), (RCheck::R2, "logical_a"));
	assert!(got.detail.contains("implementation"), "{got}");

	// The uniqueness and cap checks span both sets.
	let same_logical = dialect_rule("same", RuleDialect::LogicalRewrite);
	let same_implementation = dialect_rule("same", RuleDialect::Implementation);
	let got = RuleSet::from_sets(&[same_logical], &[same_implementation])
		.expect_err("a name repeated across the sets");
	assert_eq!((got.check, got.rule), (RCheck::R10, "same"));

	// The 64-rule cap counts both sets: each half fits alone and the two together do not.
	let logical = dialect_rules("cap_logical", RuleDialect::LogicalRewrite, 40);
	let implementations = dialect_rules("cap_implementation", RuleDialect::Implementation, 40);
	RuleSet::from_sets(&logical, &[]).expect("40 logical rules fit");
	RuleSet::from_sets(&[], &implementations).expect("40 implementation rules fit");
	let got = RuleSet::from_sets(&logical, &implementations).expect_err("80 rules exceed the cap");
	assert_eq!(got.check, RCheck::R10);
	assert_eq!(got.rule, implementations[24].name);
}

#[test]
fn registered_rules_are_the_registered_list() {
	let set = registered();
	assert_eq!(set.len(), REGISTERED.len());
	assert_eq!(set.of_dialect(RuleDialect::Implementation).count(), 0);
	for (rank, decl) in REGISTERED.iter().enumerate() {
		let rule = set.rule(rank).expect("the set holds one rule per registered declaration");
		assert_eq!(rule.name(), decl.name);
		assert_eq!(rule.dialect(), RuleDialect::LogicalRewrite);
	}
}

/// `truthy(1 <op> <right>)`, the right operand in a `LazyOperand` region. Answers the truthy op.
fn truthy_of_short_circuit(fx: &mut Fixture, op: ShortCircuitOp, right: bool) -> OpId {
	let one = fx.add(literal(Value::Number(Number::Int(1))), &[], 1);
	let left = fx.result(one);
	truthy_over(fx, left, op, right)
}

/// `truthy(<left> <op> <right>)`, the right operand in a `LazyOperand` region. Answers the truthy
/// op.
fn truthy_over(fx: &mut Fixture, left: ValueId, op: ShortCircuitOp, right: bool) -> OpId {
	let short = fx.add(
		OpKind::Expr(ExprOp::ShortCircuit {
			op,
		}),
		&[left],
		1,
	);
	let (_, block) = fx.region(short, RegionKind::LazyOperand, 0);
	let lit = fx.add_in(block, literal(Value::Bool(right)), &[], 1, FrameId::new(0));
	let lit = fx.result(lit);
	fx.add_in(block, closing_yield(), &[lit], 0, FrameId::new(0));
	let short = fx.result(short);
	fx.add(truthy(), &[short], 1)
}

/// A literal op whose result carries the type knowledge the builder gives that literal. Answers
/// the result.
fn typed_literal(fx: &mut Fixture, value: Value) -> ValueId {
	let ty = result_type(&ExprOp::Literal(Box::new(value.clone())), &[]);
	let mut op = spec(literal(value), &[], 1, FrameId::new(0));
	op.results = std::iter::once(ty).collect();
	let op = Rewriter::new(&mut fx.ir, &mut fx.facts)
		.create(InsertPoint::End(fx.block), op)
		.expect("the op is created");
	fx.result(op)
}

/// `<left> <op> <right>` over two int literals, each typed as the builder types it. Answers the
/// binary op.
fn binary_of(fx: &mut Fixture, op: BinaryOperator, left: i64, right: i64) -> OpId {
	let left = typed_literal(fx, Value::Number(Number::Int(left)));
	let right = typed_literal(fx, Value::Number(Number::Int(right)));
	fx.add(
		OpKind::Expr(ExprOp::Binary {
			op,
		}),
		&[left, right],
		1,
	)
}

/// Builds the fixture of one registered rule.
type FixtureBuilder = fn(&mut Fixture) -> OpId;

#[test]
fn gate_pre_match() {
	// Over the registered rules, the fixture of the i-th registered rule sets bit i and no other.
	let fixtures: [(&RuleDecl, FixtureBuilder); 4] = [
		(&AND_FALSE_TRUTHY, lr5),
		(&OR_TRUE_TRUTHY, lr6),
		(&MUL_ZERO_INT_R, lr7r),
		(&MUL_ZERO_INT_L, lr7l),
	];
	let held = registered();
	for (decl, build) in fixtures {
		let mut fx = Fixture::new();
		let op = build(&mut fx);
		let rank = REGISTERED
			.iter()
			.position(|rule| std::ptr::eq(*rule, decl))
			.expect("every catalogue rule is registered");
		assert_eq!(held.pre_match(&fx.ir, op), 1u64 << rank, "{}", decl.name);
	}
	let set = registered();
	let mask = |build: &dyn Fn(&mut Fixture) -> OpId| {
		let mut fx = Fixture::new();
		let op = build(&mut fx);
		set.pre_match(&fx.ir, op)
	};
	assert_eq!(mask(&|fx| truthy_of_short_circuit(fx, ShortCircuitOp::And, false)), 0b0001);
	assert_eq!(mask(&|fx| truthy_of_short_circuit(fx, ShortCircuitOp::Or, true)), 0b0010);
	assert_eq!(mask(&|fx| binary_of(fx, BinaryOperator::Multiply, 7, 0)), 0b0100);
	assert_eq!(mask(&|fx| binary_of(fx, BinaryOperator::Multiply, 0, 7)), 0b1000);
	// Only `expr.binary{add}`: no rule roots at it.
	assert_eq!(mask(&|fx| binary_of(fx, BinaryOperator::Add, 7, 0)), 0);
	// A `truthy` over a plain literal has no short-circuit below it.
	assert_eq!(
		mask(&|fx| {
			let lit = fx.add(literal(Value::Bool(true)), &[], 1);
			let lit = fx.result(lit);
			fx.add(truthy(), &[lit], 1)
		}),
		0
	);
}

/// Adds `kind` as a node of the hand-built graph `eg`, with facts of the class numbered `ordinal`.
fn put(
	eg: &mut EGraph<ENode, IrAnalysis>,
	cx: &EffectCx<'_>,
	kind: OpKind,
	kids: &[Id],
	ordinal: u32,
) -> Id {
	let tag = kind.tag();
	let payload = eg.analysis.intern_payload(Arc::new(kind), cx);
	let mut node = op_node(payload.0, 0, 0);
	node.id.shape = NodeShape::Op {
		tag,
		payload,
	};
	node.kids = kids.iter().copied().collect();
	add(
		eg,
		node,
		with(|facts| {
			facts.ordinal = ordinal;
		}),
	)
}

#[test]
fn a_catalogue_rule_searches_and_applies() {
	let env = Env::new();
	let cx = env.cx();
	let set = RuleSet::new(CATALOGUE).expect("the catalogue is admitted");
	let rule = set.rule(0).expect("the catalogue has a first rule");
	let mut eg = graph();

	let left = add(&mut eg, leaf_node(0, 0), with(|facts| facts.ordinal = 0));
	let right = put(&mut eg, &cx, literal(Value::Bool(false)), &[], 1);
	let mut lazy = leaf_node(1, 0);
	lazy.id.shape = NodeShape::Region {
		kind_key: region_kind_key(RegionKind::LazyOperand),
		binder: BinderId::NONE,
		results: 1,
	};
	lazy.kids = smallvec![right];
	let lazy = add(&mut eg, lazy, with(|facts| facts.ordinal = 2));
	let short = put(
		&mut eg,
		&cx,
		OpKind::Expr(ExprOp::ShortCircuit {
			op: ShortCircuitOp::And,
		}),
		&[left, lazy],
		3,
	);
	let root = put(&mut eg, &cx, truthy(), &[short], 4);
	eg.rebuild();

	let (matches, capped) = rule.search(&eg);
	assert!(!capped);
	assert_eq!(matches.len(), 1);
	assert_eq!(matches[0].root, ClassRef::from(root));

	let applied = rule.apply(&mut eg, &matches[0], &cx);
	assert!(applied.unioned);
	eg.rebuild();
	assert_eq!(eg.analysis.union_refused, 0);
	assert!(eg[root].nodes.iter().any(|node| matches!(
		node.id.shape,
		NodeShape::Op {
			tag: OpTag::Expr(ExprTag::Literal),
			..
		}
	)));

	// The built literal is the one the match already holds, so a second apply joins nothing and
	// adds nothing.
	let (again, _) = rule.search(&eg);
	assert_eq!(again.len(), 1);
	let applied = rule.apply(&mut eg, &again[0], &cx);
	assert!(!applied.unioned && !applied.added);
}

// ---------------------------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------------------------

const BINARY_TAG: OpTag = OpTag::Expr(ExprTag::Binary);

/// `?x <op> ?y` is `?y <op> ?x` for every binary operator.
///
/// The right side copies the attributes of the node the left side matched, so a snapshot that
/// went stale would build the wrong operator.
static SWAP: RuleDecl = RuleDecl {
	name: "swap",
	dialect: RuleDialect::LogicalRewrite,
	tier: RuleTier::Normal,
	lhs: Pat::Node {
		bind: Some("b"),
		head: HeadPat {
			tag: BINARY_TAG,
			attr: None,
		},
		kids: &[Pat::Var("x"), Pat::Var("y")],
	},
	rhs: Rhs::Tmpl(Tmpl::Node {
		tag: BINARY_TAG,
		attrs: AttrSrc::CopyOf("b"),
		frame_of: "b",
		kids: &[Tmpl::Var("y"), Tmpl::Var("x")],
	}),
	when: &[],
	sound: Some("a test rule that only reorders the operands of a binary op"),
	answers: &[],
	drops: &[],
	duplicates: &[],
	needs_total_order: false,
};

/// [`SWAP`] with the new node taking the frame of `?x`, so a root in another frame refuses the
/// union.
fn swap_across_frames() -> &'static RuleDecl {
	let decl = RuleDecl {
		name: "swap_across_frames",
		rhs: Rhs::Tmpl(Tmpl::Node {
			tag: BINARY_TAG,
			attrs: AttrSrc::CopyOf("b"),
			frame_of: "x",
			kids: &[Tmpl::Var("y"), Tmpl::Var("x")],
		}),
		..SWAP.clone()
	};
	Box::leak(Box::new(decl))
}

fn binary(op: BinaryOperator) -> OpKind {
	OpKind::Expr(ExprOp::Binary {
		op,
	})
}

/// Limits no fixture of a few hundred nodes reaches.
fn roomy() -> EgraphLimits {
	EgraphLimits {
		nodes: 10_000,
		iterations: 8,
		wall: Duration::from_secs(3600),
	}
}

/// A binary op node of payload `payload` over `kids`, in frame 0, with the class numbered
/// `ordinal`. The payload is interned by the caller, so two graphs share its id.
fn put_binary(
	eg: &mut EGraph<ENode, IrAnalysis>,
	payload: PayloadId,
	kids: &[Id],
	ordinal: u32,
) -> Id {
	let mut node = op_node(payload.0, 0, 0);
	node.id.shape = NodeShape::Op {
		tag: BINARY_TAG,
		payload,
	};
	node.kids = kids.iter().copied().collect();
	add(
		eg,
		node,
		with(|facts| {
			facts.ordinal = ordinal;
		}),
	)
}

/// A rebuilt graph of `count` classes `l_i - r` over opaque leaves, added in the order `order`.
///
/// The ordinals do not depend on the order: `r` is 0, `l_i` is `1 + i` and `l_i - r` is
/// `1 + count + i`, so two orders give the same terms under different ids.
fn sub_fixture(cx: &EffectCx<'_>, count: usize, order: &[usize]) -> EGraph<ENode, IrAnalysis> {
	assert_eq!(order.len(), count);
	let mut eg = graph();
	let sub = eg.analysis.intern_payload(Arc::new(binary(BinaryOperator::Subtract)), cx);
	let r = add(&mut eg, leaf_node(0, 0), with(|facts| facts.ordinal = 0));
	let mut leaves: Vec<Option<Id>> = vec![None; count];
	for &i in order {
		let ordinal = 1 + i as u32;
		leaves[i] =
			Some(add(&mut eg, leaf_node(ordinal, 0), with(|facts| facts.ordinal = ordinal)));
	}
	for &i in order {
		let left = leaves[i].expect("every leaf is added");
		put_binary(&mut eg, sub, &[left, r], (1 + count + i) as u32);
	}
	eg.rebuild();
	eg
}

/// The identity, the reverse and a stride of `0..count`. The stride is coprime to `count`, so the
/// third order is a permutation.
fn orders(count: usize, stride: usize) -> [Vec<usize>; 3] {
	let identity: Vec<usize> = (0..count).collect();
	let reversed: Vec<usize> = identity.iter().rev().copied().collect();
	let strided: Vec<usize> = (0..count).map(|i| (i * stride) % count).collect();
	let mut seen = strided.clone();
	seen.sort_unstable();
	assert_eq!(seen, identity, "the stride is not coprime to the count");
	[identity, reversed, strided]
}

/// The classes of `eg` as their ordinal and the sorted content keys of their nodes, sorted: what
/// two graphs hold when they hold the same terms, whatever their ids.
fn contents(eg: &EGraph<ENode, IrAnalysis>) -> Vec<(u32, Vec<NodeKey>)> {
	let mut out: Vec<(u32, Vec<NodeKey>)> = eg
		.classes()
		.map(|class| {
			let mut keys: Vec<NodeKey> = class
				.nodes
				.iter()
				.map(|node| {
					let mut node = node.clone();
					for kid in node.kids.iter_mut() {
						*kid = eg.find(*kid);
					}
					node_key(eg, &node)
				})
				.collect();
			keys.sort();
			keys.dedup();
			(class.data.ordinal, keys)
		})
		.collect();
	out.sort();
	out
}

fn run_logical(
	eg: &mut EGraph<ENode, IrAnalysis>,
	set: &RuleSet,
	max_iterations: usize,
	budget: &mut Budget,
	cx: &EffectCx<'_>,
) -> RoundReport {
	saturate(eg, set, RuleDialect::LogicalRewrite, TierFilter::Normal, max_iterations, budget, cx)
}

/// A payload is shared only between ops with equal attributes. `1`, `1f` and `1dec` intern apart,
/// and under one forced digest, as a collision gives, they still do; an equal `1` is shared.
#[test]
fn interning_confirms_a_digest_hit_by_attributes() {
	let env = Env::new();
	let cx = env.cx();
	let number = |n: Number| Arc::new(OpKind::Expr(ExprOp::Literal(Box::new(Value::Number(n)))));
	let kinds = || {
		[
			number(Number::Int(1)),
			number(Number::Float(1.0)),
			number(Number::Decimal(rust_decimal::Decimal::ONE)),
		]
	};
	let mut eg = graph();
	let by_digest: Vec<PayloadId> =
		kinds().into_iter().map(|kind| eg.analysis.intern_payload(kind, &cx)).collect();
	assert_ne!(by_digest[0], by_digest[1]);
	assert_ne!(by_digest[0], by_digest[2]);
	assert_ne!(by_digest[1], by_digest[2]);
	assert_eq!(eg.analysis.intern_payload(number(Number::Int(1)), &cx), by_digest[0]);

	let collided = AttrDigest([1; 16]);
	let mut eg = graph();
	let forced: Vec<PayloadId> = kinds()
		.into_iter()
		.map(|kind| eg.analysis.intern_payload_under(collided, kind, &cx))
		.collect();
	assert_ne!(forced[0], forced[1]);
	assert_ne!(forced[0], forced[2]);
	assert_ne!(forced[1], forced[2]);
	assert_eq!(eg.analysis.intern_payload_under(collided, number(Number::Int(1)), &cx), forced[0]);
}

/// The second match of a rule is applied after the first match's union has changed the class it
/// matched through, and the attributes its `CopyOf` reads are still the ones it matched.
#[test]
fn match_survives_union() {
	let env = Env::new();
	let cx = env.cx();
	let set = RuleSet::new(&[&SWAP]).expect("the swap rule is admitted");
	let rule = set.rule(0).expect("the set holds the rule");

	let mut eg = graph();
	let sub = eg.analysis.intern_payload(Arc::new(binary(BinaryOperator::Subtract)), &cx);
	let mul = eg.analysis.intern_payload(Arc::new(binary(BinaryOperator::Multiply)), &cx);
	let x = add(&mut eg, leaf_node(0, 0), with(|facts| facts.ordinal = 0));
	let y = add(&mut eg, leaf_node(1, 0), with(|facts| facts.ordinal = 1));
	let w = add(&mut eg, leaf_node(2, 0), with(|facts| facts.ordinal = 2));
	let a = put_binary(&mut eg, sub, &[x, y], 3);
	let b = put_binary(&mut eg, mul, &[a, w], 4);
	eg.rebuild();

	let (matches, capped) = rule.search(&eg);
	assert!(!capped);
	assert_eq!(matches.len(), 2);
	assert_eq!(matches[0].root, ClassRef::from(a));
	assert_eq!(matches[1].root, ClassRef::from(b));

	// No rebuild between the applies: the second match holds the class `a` had at search time.
	assert!(rule.apply(&mut eg, &matches[0], &cx).unioned);
	assert!(rule.apply(&mut eg, &matches[1], &cx).unioned);
	eg.rebuild();
	assert_eq!(eg.analysis.union_refused, 0);

	let (a, w) = (eg.find(a), eg.find(w));
	assert_eq!(eg[a].nodes.len(), 2);
	let swapped = eg[b]
		.nodes
		.iter()
		.find(|node| node.kids.iter().map(|kid| eg.find(*kid)).collect::<Vec<_>>() == [w, a])
		.expect("the second match built its swapped node");
	assert!(
		matches!(
			swapped.id.shape,
			NodeShape::Op {
				tag: BINARY_TAG,
				payload,
			} if payload == mul
		),
		"{:?}",
		swapped.id.shape
	);
}

/// A named applier that adds a physical node in the root's frame and answers its class.
fn adds_a_physical_node(view: &mut dyn EgraphView, m: &Match) -> SmallVec<[ClassRef; 1]> {
	let added = view.add(NodeSpec {
		shape: NodeSpecShape::Op(OpKind::Phys(PhysOp::Vacant {
			table: TableName::from("t"),
		})),
		frame_of: m.root,
		kids: SmallVec::new(),
	});
	smallvec![added]
}

/// A logical rewrite rule whose applier adds a physical node adds nothing in any build: the add
/// is refused and counted, the applier's answer is discarded, and the root keeps its nodes.
#[test]
fn a_logical_rule_cannot_add_a_physical_node() {
	let env = Env::new();
	let cx = env.cx();
	let decl: &'static RuleDecl = Box::leak(Box::new(RuleDecl {
		name: "adds_physical",
		rhs: Rhs::Named {
			name: "adds_physical",
			apply: adds_a_physical_node,
		},
		..SWAP.clone()
	}));
	let set = RuleSet::new(&[decl]).expect("the named rule is admitted");
	let rule = set.rule(0).expect("the set holds the rule");

	let mut eg = graph();
	let sub = eg.analysis.intern_payload(Arc::new(binary(BinaryOperator::Subtract)), &cx);
	let x = add(&mut eg, leaf_node(0, 0), with(|facts| facts.ordinal = 0));
	let y = add(&mut eg, leaf_node(1, 0), with(|facts| facts.ordinal = 1));
	let a = put_binary(&mut eg, sub, &[x, y], 2);
	eg.rebuild();

	let (matches, capped) = rule.search(&eg);
	assert!(!capped);
	assert_eq!(matches.len(), 1);
	let size = eg.total_size();
	let applied = rule.apply(&mut eg, &matches[0], &cx);
	assert!(!applied.unioned && !applied.added);
	eg.rebuild();
	assert_eq!(eg.analysis.union_refused, 1);
	assert_eq!(eg.total_size(), size);
	assert_eq!(eg[eg.find(a)].nodes.len(), 1);
}

/// The graph a round ends with, and the report, depend on the terms the graph holds and never on
/// the order they were added in; a rule that reaches its match cap stops the round the same way
/// under every order.
#[test]
fn plan_independent_of_order() {
	let env = Env::new();
	let cx = env.cx();
	let set = RuleSet::new(&[&SWAP]).expect("the swap rule is admitted");
	let run = |count: usize, order: &[usize]| {
		let mut eg = sub_fixture(&cx, count, order);
		let mut budget = Budget::new(roomy());
		let report = run_logical(&mut eg, &set, ROUND_L_ITERATIONS, &mut budget, &cx);
		(report, contents(&eg))
	};

	let small = orders(7, 3);
	let (report, held) = run(7, &small[0]);
	assert!(report.saturated);
	assert_eq!(report.stopped_by, None);
	assert!(!report.cap_hit);
	assert!(held.iter().any(|(_, keys)| keys.len() == 2));
	for order in &small[1..] {
		let (again, again_held) = run(7, order);
		assert_eq!(again, report);
		assert_eq!(again_held, held);
	}

	let count = MATCH_CAP_PER_RULE + 44;
	let wide = orders(count, 7);
	let (report, held) = run(count, &wide[0]);
	assert_eq!(
		report,
		RoundReport {
			iterations: 1,
			stopped_by: Some(Limit::Iterations),
			saturated: false,
			cap_hit: true,
		}
	);
	// The truncated search applied nothing.
	assert!(held.iter().all(|(_, keys)| keys.len() == 1));
	for order in &wide[1..] {
		let (again, again_held) = run(count, order);
		assert_eq!(again, report);
		assert_eq!(again_held, held);
	}
}

#[test]
fn budget_and_counters() {
	let env = Env::new();
	let cx = env.cx();
	let set = RuleSet::new(&[&SWAP]).expect("the swap rule is admitted");
	let order: Vec<usize> = (0..3).collect();

	// A round that saturates: the second iteration finds nothing to add.
	let mut eg = sub_fixture(&cx, 3, &order);
	let mut budget = Budget::new(roomy());
	let report = run_logical(&mut eg, &set, ROUND_L_ITERATIONS, &mut budget, &cx);
	assert_eq!(
		report,
		RoundReport {
			iterations: 2,
			stopped_by: None,
			saturated: true,
			cap_hit: false,
		}
	);
	assert_eq!((budget.iterations, budget.rounds), (2, 1));
	assert_eq!(eg.analysis.union_refused, 0);
	assert!(eg.clean);

	// The round's own cap.
	let mut eg = sub_fixture(&cx, 3, &order);
	let mut budget = Budget::new(roomy());
	let report = run_logical(&mut eg, &set, 1, &mut budget, &cx);
	assert_eq!(
		report,
		RoundReport {
			iterations: 1,
			stopped_by: Some(Limit::Iterations),
			saturated: false,
			cap_hit: false,
		}
	);

	// The statement's iteration limit is shared by its rounds.
	let mut eg = sub_fixture(&cx, 3, &order);
	let mut budget = Budget::new(EgraphLimits {
		iterations: 1,
		..roomy()
	});
	let first = run_logical(&mut eg, &set, ROUND_L_ITERATIONS, &mut budget, &cx);
	assert_eq!((first.iterations, first.stopped_by), (1, Some(Limit::Iterations)));
	let second = run_logical(&mut eg, &set, ROUND_L_ITERATIONS, &mut budget, &cx);
	assert_eq!((second.iterations, second.stopped_by), (0, Some(Limit::Iterations)));
	assert_eq!((budget.iterations, budget.rounds), (1, 2));
	assert!(budget.iterations <= budget.limits.iterations);

	// A rule that reaches its match cap.
	let count = MATCH_CAP_PER_RULE + 1;
	let mut eg = sub_fixture(&cx, count, &(0..count).collect::<Vec<_>>());
	let mut budget = Budget::new(roomy());
	let report = run_logical(&mut eg, &set, ROUND_L_ITERATIONS, &mut budget, &cx);
	assert!(report.cap_hit);
	assert!(!report.saturated);
	assert_eq!(report.stopped_by, Some(Limit::Iterations));

	// A union a guard refuses is counted and leaves the root class as it was.
	let across = RuleSet::new(&[swap_across_frames()]).expect("the rule is admitted");
	let mut eg = graph();
	let sub = eg.analysis.intern_payload(Arc::new(binary(BinaryOperator::Subtract)), &cx);
	let x = add(&mut eg, leaf_node(0, 1), {
		let mut facts = blank(1);
		facts.ordinal = 0;
		facts
	});
	let y = add(&mut eg, leaf_node(1, 0), with(|facts| facts.ordinal = 1));
	let root = put_binary(&mut eg, sub, &[x, y], 2);
	eg.rebuild();
	let mut budget = Budget::new(roomy());
	let _ = run_logical(&mut eg, &across, ROUND_L_ITERATIONS, &mut budget, &cx);
	assert!(eg.analysis.union_refused >= 1);
	assert_eq!(eg[root].nodes.len(), 1);
}

/// A limit stops a round before it touches the graph, whatever the order of the checks, and the
/// stop is the same on every run.
#[test]
fn wall_stops_answer_the_skip_path() {
	let env = Env::new();
	let cx = env.cx();
	let set = RuleSet::new(&[&SWAP]).expect("the swap rule is admitted");
	let order: Vec<usize> = (0..3).collect();
	let stops_untouched = |limits: EgraphLimits, stop: Limit| {
		let mut reports = Vec::new();
		for _ in 0..10 {
			let mut eg = sub_fixture(&cx, 3, &order);
			let before = contents(&eg);
			let mut budget = Budget::new(limits);
			let report = run_logical(&mut eg, &set, ROUND_L_ITERATIONS, &mut budget, &cx);
			assert_eq!(contents(&eg), before, "{limits:?}");
			assert_eq!(report.stopped_by, Some(stop), "{limits:?}");
			assert_eq!(report.iterations, 0, "{limits:?}");
			assert!(!report.saturated && !report.cap_hit, "{limits:?}");
			reports.push(report);
		}
		assert!(reports.windows(2).all(|pair| pair[0] == pair[1]));
	};
	stops_untouched(
		EgraphLimits {
			wall: Duration::ZERO,
			..roomy()
		},
		Limit::Wall,
	);
	stops_untouched(
		EgraphLimits {
			nodes: 1,
			iterations: 0,
			..roomy()
		},
		Limit::Nodes,
	);
	stops_untouched(
		EgraphLimits {
			iterations: 0,
			..roomy()
		},
		Limit::Iterations,
	);
}

/// A round with no rule still honours the wall limit.
#[test]
fn a_round_without_rules_checks_the_limits() {
	let env = Env::new();
	let cx = env.cx();
	let empty = RuleSet::new(&[]).expect("the empty set is admitted");
	let mut eg = sub_fixture(&cx, 3, &[0, 1, 2]);
	let mut budget = Budget::new(roomy());
	let report = run_logical(&mut eg, &empty, ROUND_L_ITERATIONS, &mut budget, &cx);
	assert_eq!(
		report,
		RoundReport {
			iterations: 1,
			stopped_by: None,
			saturated: true,
			cap_hit: false,
		}
	);
	let mut budget = Budget::new(EgraphLimits {
		wall: Duration::ZERO,
		..roomy()
	});
	let report = run_logical(&mut eg, &empty, ROUND_L_ITERATIONS, &mut budget, &cx);
	assert_eq!(report.stopped_by, Some(Limit::Wall));
}

#[test]
fn budget_clock_leaves_out_paused_time() {
	let limits = EgraphLimits {
		wall: Duration::from_secs(1),
		..roomy()
	};
	let mut budget = Budget::new(limits);
	assert!(!budget.wall_exceeded());
	budget.start =
		Instant::now().checked_sub(Duration::from_secs(10)).expect("the clock is past ten seconds");
	assert!(budget.wall_exceeded());
	budget.paused = Duration::from_secs(10);
	assert!(!budget.wall_exceeded());

	// A resume with no pause in force counts nothing, and a pause in force is not nested.
	let held = budget.paused;
	budget.resume();
	assert_eq!(budget.paused, held);
	budget.pause();
	budget.pause();
	budget.resume();
	assert!(budget.paused >= held);

	let zero = Budget::new(EgraphLimits {
		wall: Duration::ZERO,
		..roomy()
	});
	assert!(zero.wall_exceeded());
	assert_eq!(zero.iterations_left(), roomy().iterations);
}

#[test]
fn resaturation_is_never_asked_for() {
	let env = Env::new();
	let eg = sub_fixture(&env.cx(), 2, &[0, 1]);
	assert!(!resaturate_needed(&eg, 1));
}

/// The text of the Rust sources under `exec/ir`, with each line comment cut off, and the path
/// relative to `exec/ir`. The files that hold the needles are left out.
fn ir_sources() -> Vec<(String, String)> {
	fn visit(dir: &Path, root: &Path, out: &mut Vec<(String, String)>) {
		for entry in fs::read_dir(dir).expect("the directory reads") {
			let path = entry.expect("the entry reads").path();
			if path.is_dir() {
				visit(&path, root, out);
			} else if path.extension().is_some_and(|ext| ext == "rs") {
				let relative = path.strip_prefix(root).expect("a path under the root");
				let relative = relative.to_string_lossy().replace('\\', "/");
				if relative == "egraph/tests.rs" || relative == "tests_iface.rs" {
					continue;
				}
				let text = fs::read_to_string(&path).expect("the file reads");
				let code = text
					.lines()
					.map(|line| line.find("//").map_or(line, |at| &line[..at]))
					.collect::<Vec<_>>()
					.join("\n");
				out.push((relative, code));
			}
		}
	}
	let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/exec/ir");
	let mut out = Vec::new();
	visit(&root, &root, &mut out);
	assert!(out.len() > 100, "a scan of exec/ir read {} files", out.len());
	out
}

/// Whether `word` occurs in `text` bounded by non-identifier characters.
fn names_word(text: &str, word: &str) -> bool {
	let is_ident = |c: char| c.is_alphanumeric() || c == '_';
	text.match_indices(word).any(|(at, _)| {
		let before = text[..at].chars().next_back();
		let after = text[at + word.len()..].chars().next();
		!before.is_some_and(is_ident) && !after.is_some_and(is_ident)
	})
}

/// The module drives its own runner: egg's `Runner` reads a clock the limits cannot silence and
/// its `Rewrite` searches in an order that depends on class ids.
#[test]
fn no_quanta_runner() {
	for (path, code) in ir_sources() {
		for word in ["Runner", "Rewrite", "quanta"] {
			assert!(!names_word(&code, word), "{path} names `{word}`");
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Extraction
// ---------------------------------------------------------------------------------------------

/// A hook that prices every node at one plus its kids, so a smaller term is cheaper and nothing
/// else separates two terms of one size.
struct ExtractWeights;

impl CostHook for ExtractWeights {
	fn node(&self, n: &CostNode<'_>, kids: &[PlanCost]) -> PlanCost {
		PlanCost {
			illegal: false,
			work: 1 + kids.iter().map(|kid| kid.work).sum::<u64>(),
			novelty: 0,
			rank: 0,
			prov: n.view.facts(n.class).ordinal,
		}
	}
}

/// An op node of `tag` with payload `payload` over `kids`, in frame 0 and snapshot 0.
fn tagged_node(tag: OpTag, payload: u32, kids: &[Id]) -> ENode {
	ENode {
		id: NodeIdentity {
			shape: NodeShape::Op {
				tag,
				payload: PayloadId(payload),
			},
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			epoch: EpochKey::None,
		},
		kids: kids.iter().copied().collect(),
	}
}

/// Facts of a pure class with import ordinal `ordinal`.
fn ordinal_facts(ordinal: u32) -> ClassFacts {
	let mut facts = blank(0);
	facts.ordinal = ordinal;
	facts
}

/// The island root whose class is `class`.
fn island_root(class: Id) -> IslandRoot {
	IslandRoot {
		value: ValueId::default(),
		class,
		uses: SmallVec::new(),
	}
}

/// The shape of the node `chosen` holds for the class of `class`.
fn chosen_shape(
	eg: &EGraph<ENode, IrAnalysis>,
	chosen: &super::extract::Term,
	class: Id,
) -> NodeShape {
	chosen.chosen[&ClassRef::from(eg.find(class))].identity.shape
}

/// A class holding only a logical node has no plan made of physical nodes, and a physical node
/// over such a class is illegal with it.
#[test]
fn extraction_all_phys_or_fallback() {
	let mut eg = graph();
	let logical =
		add(&mut eg, tagged_node(OpTag::Logical(LogTag::Filter), 0, &[]), ordinal_facts(0));
	let above =
		add(&mut eg, tagged_node(OpTag::Phys(PhysTag::Filter), 1, &[logical]), ordinal_facts(1));
	eg.rebuild();
	assert!(matches!(
		extract_phys(&eg, &[island_root(logical)], &ExtractWeights),
		Err(FallbackReason::NoPhysRoot)
	));
	assert!(matches!(
		extract_phys(&eg, &[island_root(above)], &ExtractWeights),
		Err(FallbackReason::NoPhysRoot)
	));

	// A physical node joined to the logical one is the plan, whatever the hook prices it at.
	let scan = add(&mut eg, tagged_node(OpTag::Phys(PhysTag::TableScan), 2, &[]), ordinal_facts(2));
	assert!(eg.union(logical, scan));
	eg.rebuild();
	let choice =
		extract_phys(&eg, &[island_root(above)], &ExtractWeights).expect("a physical plan");
	assert!(matches!(
		chosen_shape(&eg, &choice.term, logical),
		NodeShape::Op {
			tag: OpTag::Phys(PhysTag::TableScan),
			..
		}
	));
	assert!(matches!(
		chosen_shape(&eg, &choice.term, above),
		NodeShape::Op {
			tag: OpTag::Phys(PhysTag::Filter),
			..
		}
	));
	assert_eq!(choice.term.order.len(), 2);
}

/// A class that holds itself as a kid still has a plan: the node that needs nothing wins.
#[test]
fn extraction_survives_a_class_that_holds_itself() {
	let mut eg = graph();
	let leaf = add(&mut eg, tagged_node(OpTag::Phys(PhysTag::TableScan), 0, &[]), ordinal_facts(0));
	let above =
		add(&mut eg, tagged_node(OpTag::Phys(PhysTag::Filter), 1, &[leaf]), ordinal_facts(1));
	// The filter node has its own class as its kid.
	assert!(eg.union(leaf, above));
	eg.rebuild();
	let choice = extract_phys(&eg, &[island_root(above)], &ExtractWeights).expect("the leaf wins");
	assert!(matches!(
		chosen_shape(&eg, &choice.term, above),
		NodeShape::Op {
			tag: OpTag::Phys(PhysTag::TableScan),
			..
		}
	));
}

/// With no rule applied there is one candidate, the incumbent, and nothing is to be written.
#[test]
fn logical_candidates_incumbent_only_without_rules() {
	let mut eg = graph();
	let leaf = add(&mut eg, op_node(0, 0, 0), ordinal_facts(0));
	let parent =
		add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Truthy), 1, &[leaf]), ordinal_facts(1));
	eg.rebuild();
	let got = logical_candidates(&eg, &[island_root(parent)]);
	assert_eq!(got.len(), 1);
	assert_eq!(got[0].index, 0);
	let term = &got[0].term;
	assert_eq!(term.order, vec![ClassRef::from(eg.find(parent)), ClassRef::from(eg.find(leaf))]);
	assert!(matches!(
		chosen_shape(&eg, term, parent),
		NodeShape::Op {
			tag: OpTag::Expr(ExprTag::Truthy),
			..
		}
	));
}

/// A class joined with a smaller form gives a second candidate that chooses the smaller form, also
/// when the form is the class's own kid and the union leaves the class holding itself.
#[test]
fn logical_candidates_choose_the_smaller_union() {
	let mut eg = graph();
	let leaf = add(&mut eg, op_node(0, 0, 0), ordinal_facts(0));
	let parent =
		add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Truthy), 1, &[leaf]), ordinal_facts(1));
	assert!(eg.union(parent, leaf));
	eg.rebuild();
	let got = logical_candidates(&eg, &[island_root(parent)]);
	assert_eq!(got.len(), 2);
	assert_eq!((got[0].index, got[1].index), (0, 1));
	assert!(matches!(
		chosen_shape(&eg, &got[0].term, parent),
		NodeShape::Op {
			tag: OpTag::Expr(ExprTag::Truthy),
			..
		}
	));
	assert!(matches!(
		chosen_shape(&eg, &got[1].term, parent),
		NodeShape::Op {
			tag: OpTag::Expr(ExprTag::Literal),
			..
		}
	));
	assert_eq!(got[1].term.order.len(), 1);
}

/// A rewrite below the root is a rewrite: the root node is the incumbent's and the candidate still
/// differs from it.
#[test]
fn logical_candidates_see_a_rewrite_below_the_root() {
	let mut eg = graph();
	let big = add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Truthy), 1, &[]), ordinal_facts(0));
	let wrapped =
		add(&mut eg, tagged_node(OpTag::Expr(ExprTag::ShortCircuit), 2, &[big]), ordinal_facts(1));
	let small = add(&mut eg, op_node(9, 0, 0), ordinal_facts(2));
	let top =
		add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Truthy), 3, &[wrapped]), ordinal_facts(3));
	assert!(eg.union(wrapped, small));
	eg.rebuild();
	let got = logical_candidates(&eg, &[island_root(top)]);
	assert_eq!(got.len(), 2);
	let term = &got[1].term;
	assert!(matches!(
		chosen_shape(&eg, term, wrapped),
		NodeShape::Op {
			tag: OpTag::Expr(ExprTag::Literal),
			..
		}
	));
	assert_eq!(term.order.len(), 2);
}

/// Two graphs holding the same terms, built in opposite orders, choose the same node and give the
/// same structural key.
#[test]
fn extraction_ties_do_not_depend_on_insertion_order() {
	let build = |first: u32, second: u32| {
		let mut eg = graph();
		let a = add(&mut eg, op_node(first, 0, 0), ordinal_facts(0));
		let b = add(&mut eg, op_node(second, 0, 0), ordinal_facts(0));
		assert!(eg.union(a, b));
		eg.rebuild();
		(eg, a)
	};
	let mut keys = Vec::new();
	for (first, second) in [(3, 7), (7, 3)] {
		let (eg, class) = build(first, second);
		let best = best_costs(&eg, &ExtractWeights, |_, _| true);
		let class = ClassRef::from(eg.find(class));
		let (_, node) = &best[&class];
		assert!(matches!(
			node.identity.shape,
			NodeShape::Op {
				payload: PayloadId(3),
				..
			}
		));
		keys.push(structural_key(&eg, class, &best));
	}
	assert_eq!(keys[0], keys[1]);
	assert!(!keys[0].is_empty());
}

/// A node that would change the order a row producer delivers is refused unless the class proves
/// the change harmless, and the seed that delivers the seed order is admissible.
#[test]
fn phys_extraction_refuses_an_unproven_order_change() {
	let mut eg = graph();
	let input = add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Literal), 0, &[]), ordinal_facts(0));
	let mut seed_facts = ordinal_facts(1);
	seed_facts.seed_order = Some(OrderSig::IdAsc);
	let seed_node = tagged_node(OpTag::Phys(PhysTag::TableScan), 1, &[input]);
	let seed = add(&mut eg, seed_node.clone(), seed_facts);
	// The alternative is cheaper: it has no kid.
	let alternative =
		add(&mut eg, tagged_node(OpTag::Phys(PhysTag::IndexScan), 2, &[]), ordinal_facts(2));
	assert!(eg.union(seed, alternative));
	eg.rebuild();
	eg.analysis.seeds.insert(
		seed_node,
		smallvec![SeedInfo {
			candidate: 0,
			op: OpId::default(),
			order: OrderSig::IdAsc,
		}],
	);
	let roots = [island_root(seed)];
	let kept = extract_phys(&eg, &roots, &ExtractWeights).expect("the seed is a plan");
	assert!(matches!(
		chosen_shape(&eg, &kept.term, seed),
		NodeShape::Op {
			tag: OpTag::Phys(PhysTag::TableScan),
			..
		}
	));

	let class = eg.find(seed);
	eg[class].data.total_order_ok = true;
	let moved = extract_phys(&eg, &roots, &ExtractWeights).expect("the alternative is a plan");
	assert!(matches!(
		chosen_shape(&eg, &moved.term, seed),
		NodeShape::Op {
			tag: OpTag::Phys(PhysTag::IndexScan),
			..
		}
	));
}

/// A seed that delivers an index order over a field its table withholds is never admissible, while
/// the same seed over an index of an observable field is.
#[test]
fn phys_extraction_refuses_an_order_over_a_withheld_field() {
	use crate::exec::ir::egraph::decl::{IndexCandidate, IndexKindClass};
	use crate::exec::ir::iface::choose::IndexRef;
	use crate::exec::ir::iface::effects::FieldSet;

	let table = TableName::new(Strand::new("t"));
	let index = |name: &str| IndexRef {
		table: table.clone(),
		index: Strand::new(name),
		position: 0,
		scalar: Box::default(),
	};
	let candidate = |name: &str, column: &str| IndexCandidate {
		index: index(name),
		columns: smallvec![Strand::new(column)],
		unique: false,
		kind: IndexKindClass::BTree,
	};
	let plan = |withheld: FieldSet, by: &str| {
		let mut eg = graph();
		let order = OrderSig::Index {
			index: index(by),
			reverse: false,
		};
		let mut seed_facts = ordinal_facts(0);
		seed_facts.seed_order = Some(order.clone());
		let seed_node = tagged_node(OpTag::Phys(PhysTag::IndexScan), 0, &[]);
		let seed = add(&mut eg, seed_node.clone(), seed_facts);
		eg.rebuild();
		eg.analysis.seeds.insert(
			seed_node,
			smallvec![SeedInfo {
				candidate: 0,
				op: OpId::default(),
				order,
			}],
		);
		eg.analysis.tables.insert(
			table.clone(),
			PhysTableFacts {
				table: table.clone(),
				indexes: smallvec![
					candidate("by_secret", "secret.inner"),
					candidate("by_open", "open")
				],
				withheld_order: withheld,
				hint: None,
				current_snapshot: true,
				estimate: None,
			},
		);
		extract_phys(&eg, &[island_root(seed)], &ExtractWeights).is_ok()
	};
	let secret = || FieldSet::Some(smallvec![Strand::new("secret")]);
	assert!(!plan(secret(), "by_secret"), "an order under a withheld field");
	assert!(plan(secret(), "by_open"), "an order over an observable field");
	assert!(!plan(FieldSet::All, "by_open"), "every field withheld");
	assert!(plan(FieldSet::Some(SmallVec::new()), "by_secret"), "nothing withheld");
}

/// A seed joined to a row producer's class does not clear the licence to reorder the producer: the
/// join alone drops it, and marking the canonical class restores it, so the alternative is
/// extractable.
#[test]
fn order_licence_survives_a_seed_union() {
	let mut eg = graph();
	let input = add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Literal), 0, &[]), ordinal_facts(0));
	let mut source_facts = ordinal_facts(1);
	source_facts.total_order_ok = true;
	let source =
		add(&mut eg, tagged_node(OpTag::Logical(LogTag::Filter), 1, &[input]), source_facts);
	let mut seed_facts = ordinal_facts(2);
	seed_facts.seed_order = Some(OrderSig::IdAsc);
	let seed_node = tagged_node(OpTag::Phys(PhysTag::TableScan), 2, &[input]);
	let seed = add(&mut eg, seed_node.clone(), seed_facts);
	// The alternative is cheaper: it has no kid.
	let alternative =
		add(&mut eg, tagged_node(OpTag::Phys(PhysTag::IndexScan), 3, &[]), ordinal_facts(3));
	assert!(eg.union(source, seed));
	assert!(eg.union(seed, alternative));
	eg.rebuild();
	eg.analysis.seeds.insert(
		seed_node,
		smallvec![SeedInfo {
			candidate: 0,
			op: OpId::default(),
			order: OrderSig::IdAsc,
		}],
	);
	let roots = [island_root(source)];
	assert!(!eg[eg.find(source)].data.total_order_ok, "the join keeps the licence");
	let kept = extract_phys(&eg, &roots, &ExtractWeights).expect("the seed is a plan");
	assert!(matches!(
		chosen_shape(&eg, &kept.term, source),
		NodeShape::Op {
			tag: OpTag::Phys(PhysTag::TableScan),
			..
		}
	));

	OrderLicence {
		free: vec![source],
		held: Vec::new(),
	}
	.mark(&mut eg);
	assert!(eg[eg.find(source)].data.total_order_ok);
	let moved = extract_phys(&eg, &roots, &ExtractWeights).expect("the alternative is a plan");
	assert!(matches!(
		chosen_shape(&eg, &moved.term, source),
		NodeShape::Op {
			tag: OpTag::Phys(PhysTag::IndexScan),
			..
		}
	));
}

/// A class that an order-sensitive producer shares with an order-free one keeps no licence: the
/// licence belongs to a producer's consumer, and the shared class serves both.
#[test]
fn order_licence_is_withheld_from_a_shared_class() {
	let mut eg = graph();
	let input = add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Literal), 0, &[]), ordinal_facts(0));
	let shared =
		add(&mut eg, tagged_node(OpTag::Logical(LogTag::Filter), 1, &[input]), ordinal_facts(1));
	let alone =
		add(&mut eg, tagged_node(OpTag::Logical(LogTag::Limit), 2, &[input]), ordinal_facts(2));
	OrderLicence {
		free: vec![shared, alone],
		held: vec![shared],
	}
	.mark(&mut eg);
	assert!(!eg[eg.find(shared)].data.total_order_ok, "the shared class has no licence");
	assert!(eg[eg.find(alone)].data.total_order_ok, "a class no sensitive producer joined has");
}

// ---------------------------------------------------------------------------------------------
// Write-back
// ---------------------------------------------------------------------------------------------

/// A container whose statement copy is a clone of the whole container: each id of the copy is the
/// id it copies, and adopting the copy replaces the container.
struct CloneSurgery;

/// The map of a clone: every op, value and region maps to itself.
fn identity_map(ir: &Ir) -> StmtMap {
	let mut map = StmtMap::default();
	let note_value = |map: &mut StmtMap, value: ValueId| {
		map.values_to_copy.insert(value, value);
		map.values_to_orig.insert(value, value);
	};
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		map.ops_to_copy.insert(op, op);
		map.ops_to_orig.insert(op, op);
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		for value in &data.results {
			note_value(&mut map, *value);
		}
		for region in &data.regions {
			map.regions_to_copy.insert(*region, *region);
			map.regions_to_orig.insert(*region, *region);
			let block = ir.region(*region).and_then(|held| held.blocks.first());
			for arg in block.and_then(|block| ir.block(*block)).iter().flat_map(|b| b.args.iter()) {
				note_value(&mut map, *arg);
			}
		}
		Walk::Continue
	});
	map
}

impl StatementSurgery for CloneSurgery {
	fn copy_statement(&self, ir: &Ir, _stmt: OpId) -> Result<(Ir, StmtMap), IrError> {
		Ok((ir.clone(), identity_map(ir)))
	}

	fn adopt_statement(
		&self,
		ir: &mut Ir,
		_facts: &mut FactCache,
		_stmt: OpId,
		from: Ir,
		_map: &StmtMap,
	) -> Result<(), IrError> {
		*ir = from;
		Ok(())
	}

	fn retake_legality(
		&self,
		_ir: &Ir,
		_facts: &mut FactCache,
		_registry: &FunctionRegistry,
		_stmt: OpId,
	) -> Result<(), IrError> {
		Ok(())
	}
}

/// The statement of `fx` as a candidate's copy: a clone, a created op standing for the island
/// root, and a physical op that replaces the created op's value. Answers the copy, its facts, its
/// id map, the created value and the physical value.
fn candidate_copy(fx: &Fixture) -> (Ir, FactCache, StmtMap, ValueId, ValueId) {
	let map = identity_map(&fx.ir);
	let mut copy = fx.ir.clone();
	let mut copy_facts = FactCache::new();
	let (created, physical) = {
		let mut rw = Rewriter::new(&mut copy, &mut copy_facts);
		let made = rw
			.create(
				InsertPoint::End(fx.block),
				spec(literal(Value::Number(Number::Int(7))), &[], 1, FrameId::new(0)),
			)
			.expect("the created op is added");
		let scan = rw
			.create(
				InsertPoint::End(fx.block),
				spec(
					OpKind::Phys(PhysOp::Vacant {
						table: TableName::from("t"),
					}),
					&[],
					1,
					FrameId::new(0),
				),
			)
			.expect("the physical op is added");
		let result = |op: OpId| rw.ir().op(op).expect("the op exists").results[0];
		(result(made), result(scan))
	};
	(copy, copy_facts, map, created, physical)
}

/// A value the candidate's elaboration created stands for the class its node was built for, so
/// the physical value that replaces it joins that class as a seed; with no record of the class the
/// seeding is refused and joins nothing.
#[test]
fn a_seed_replacing_a_created_value_joins_its_class() {
	let env = Env::new();
	let host = StubHost {
		order_ok: false,
	};
	let mut fx = Fixture::new();
	let root = add_only(&mut fx);
	let root_value = fx.result(root);
	fx.add(OpKind::Logical(LogOp::Eval), &[root_value], 1);
	fx.close();
	let phys_in = |imp: &Imported, value: ValueId| {
		imp.eg[imp.eg.find(class_of(imp, value))].nodes.iter().any(|node| {
			matches!(
				node.id.shape,
				NodeShape::Op {
					tag: OpTag::Phys(PhysTag::Vacant),
					..
				}
			)
		})
	};

	let mut imp = fx.import(&env);
	let (copy, mut copy_facts, map, created, physical) = candidate_copy(&fx);
	let built = HashMap::from([(created, ClassRef::from(class_of(&imp, root_value)))]);
	import_seed(
		&mut imp,
		&SeedCopy {
			ir: &copy,
			map: &map,
			created: &built,
			log: &[(created, physical)],
			candidate: 1,
		},
		&mut copy_facts,
		&host,
		&env.cx(),
	)
	.expect("the seed imports");
	assert!(phys_in(&imp, root_value));
	assert_eq!(imp.eg.analysis.union_refused, 0);

	let mut imp = fx.import(&env);
	let (copy, mut copy_facts, map, created, physical) = candidate_copy(&fx);
	let refused = import_seed(
		&mut imp,
		&SeedCopy {
			ir: &copy,
			map: &map,
			created: &HashMap::new(),
			log: &[(created, physical)],
			candidate: 1,
		},
		&mut copy_facts,
		&host,
		&env.cx(),
	);
	assert!(matches!(refused, Err(ImportStop::Seed(_))), "{refused:?}");
	assert!(!phys_in(&imp, root_value));
}

/// `1 * ...` fixtures of the four catalogue rules: a statement whose one island root, the op the
/// rule matches, is read by an op outside the island. Each answers the root op.
fn lr5(fx: &mut Fixture) -> OpId {
	truthy_of_short_circuit(fx, ShortCircuitOp::And, false)
}

fn lr6(fx: &mut Fixture) -> OpId {
	truthy_of_short_circuit(fx, ShortCircuitOp::Or, true)
}

fn lr7r(fx: &mut Fixture) -> OpId {
	binary_of(fx, BinaryOperator::Multiply, 7, 0)
}

fn lr7l(fx: &mut Fixture) -> OpId {
	binary_of(fx, BinaryOperator::Multiply, 0, 7)
}

/// What a rewrite of one fixture gives: the op the rule matched, the ops the statement held before,
/// the graph, the plan over the container itself, and the candidate the plan was made from.
struct Rewritten {
	root: OpId,
	before: Vec<OpId>,
	plan: ElabPlan,
	candidate: Candidate,
	imported: Imported,
}

fn ops_of(ir: &Ir) -> Vec<OpId> {
	let mut ops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		ops.push(op);
		Walk::Continue
	});
	ops
}

/// Builds `build`'s fixture and the op that reads its root, imports it, runs the catalogue and
/// plans the rewritten form into the container itself over the identity map.
fn rewritten(env: &Env, build: fn(&mut Fixture) -> OpId) -> (Fixture, Rewritten) {
	let mut fx = Fixture::new();
	let root = build(&mut fx);
	let value = fx.result(root);
	let eval = fx.add(OpKind::Logical(LogOp::Eval), &[value], 1);
	let evaluated = fx.result(eval);
	fx.add(OpKind::Logical(LogOp::Yield), &[evaluated], 0);
	let before = ops_of(&fx.ir);
	let mut imported = fx.import(env);
	let set = RuleSet::new(CATALOGUE).expect("the catalogue is admitted");
	let mut budget = Budget::new(roomy());
	run_logical(&mut imported.eg, &set, ROUND_L_ITERATIONS, &mut budget, &env.cx());
	let found = logical_candidates(&imported.eg, &imported.roots);
	assert_eq!(found.len(), 2, "the rule rewrites the root");
	let candidate = found.into_iter().nth(1).expect("a rewritten candidate");
	let built = plan(&imported.eg, &candidate.term, &identity_map(&fx.ir), &fx.ir, &Basis::LOGICAL)
		.expect("the rewrite plans");
	(
		fx,
		Rewritten {
			root,
			before,
			plan: built,
			candidate,
			imported,
		},
	)
}

/// Whether the logical id `id` is still an op, is eliminated, or is held by the provenance of a
/// live op.
fn covered(ir: &Ir, id: OpId) -> bool {
	if ir.op(id).is_some() || ir.eliminated().iter().any(|(op, _)| *op == id) {
		return true;
	}
	ops_of(ir).iter().any(|op| match ir.op(*op).map(|data| &data.provenance) {
		Some(Provenance::From(ids)) => ids.contains(&id),
		_ => false,
	})
}

/// With a container that offers no statement copy, the write-back aborts before it touches the
/// container, which is as it was.
#[test]
fn writeback_without_surgery_falls_back() {
	let env = Env::new();
	let mut eg = graph();
	let leaf = add(&mut eg, op_node(0, 0, 0), ordinal_facts(0));
	let parent =
		add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Truthy), 1, &[leaf]), ordinal_facts(1));
	assert!(eg.union(parent, leaf));
	eg.rebuild();
	let found = logical_candidates(&eg, &[island_root(parent)]);
	assert_eq!(found.len(), 2);
	let mut ir = Ir::new(
		IrScope::Request {
			fragment: None,
		},
		RegionKind::Request,
	);
	let mut facts = FactCache::new();
	let held = format!("{ir:?}");
	let outcome =
		write_logical(&mut ir, &mut facts, OpId::default(), &eg, &found[1], &NoSurgery, &env.cx());
	assert_eq!(outcome, Err(FallbackReason::WritebackAborted));
	assert_eq!(format!("{ir:?}"), held);
}

/// Each rule's rewrite builds one literal in place of the root op, or reuses the equal literal the
/// statement holds, merges the root into it, and eliminates the ops it dropped as dead pure ops;
/// after the write-back every logical id the statement held is covered or eliminated.
#[test]
fn writeback_provenance_complete() {
	let env = Env::new();
	for build in [lr5, lr6, lr7r, lr7l] {
		let (mut fx, done) = rewritten(&env, build);
		let built = &done.plan;
		// The root's uses move to a literal: a new op standing for the root, or, where the built
		// literal is the one the statement already holds, that op's value.
		assert_eq!(built.swaps.len(), 1);
		assert_eq!(built.swaps[0].from, fx.result(done.root));
		let merged = match built.swaps[0].to {
			Operand::Result(slot) => {
				assert_eq!(built.steps.len(), 1);
				let Step::Op(new) = &built.steps[slot as usize] else {
					panic!("the plan creates an op");
				};
				assert!(matches!(new.kind, OpKind::Expr(ExprOp::Literal(_))));
				assert!(new.provenance.contains(&done.root));
				Elimination::MergedInto(slot)
			}
			Operand::Value(value) => {
				assert!(built.steps.is_empty());
				let Some(ValueDef::OpResult {
					op,
					..
				}) = fx.ir.value(value).map(|data| data.def)
				else {
					panic!("the literal is an op result");
				};
				Elimination::Reason(EliminationReason::MergedInto(op))
			}
			Operand::Arg(..) => panic!("no region is built"),
		};
		// The root is merged into the literal; every other eliminated op is dead and pure.
		assert!(matches!(built.erase.first(), Some(erase) if erase.op == done.root
			&& erase.reason == merged));
		assert!(
			built.erase[1..]
				.iter()
				.all(|erase| { erase.reason == Elimination::Reason(EliminationReason::DeadPure) })
		);
		assert!(built.fuse.is_empty());

		apply(built, &mut Rewriter::new(&mut fx.ir, &mut fx.facts)).expect("the plan applies");
		assert!(done.before.iter().all(|id| covered(&fx.ir, *id)));
		// A root merged into a new op is held by that op's provenance and is not eliminated; a
		// root merged into a literal the statement already holds is eliminated with the plan's
		// reason. Every other eliminated op is dead and pure.
		let reused = matches!(built.swaps[0].to, Operand::Value(_));
		for (op, reason) in fx.ir.eliminated() {
			if let Elimination::Reason(planned) = merged
				&& reused && *op == done.root
			{
				assert_eq!(*reason, planned);
			} else {
				assert_eq!(*reason, EliminationReason::DeadPure);
			}
		}
		assert_eq!(fx.ir.eliminated().iter().any(|(op, _)| *op == done.root), reused);
		assert!(fx.ir.op(done.root).is_none());
		assert!(done.candidate.index == 1 && done.imported.roots.len() == 1);
	}
}

/// A plan that reads one leaf from two places of a new tree aborts with E5, and one that reads it
/// once is not refused for it.
#[test]
fn no_duplicated_opaque_leaf() {
	let mut eg = graph();
	let leaf = add(&mut eg, leaf_node(0, 0), ordinal_facts(0));
	let twice =
		add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Binary), 1, &[leaf, leaf]), ordinal_facts(1));
	let once =
		add(&mut eg, tagged_node(OpTag::Expr(ExprTag::Truthy), 2, &[leaf]), ordinal_facts(2));
	eg.rebuild();
	let term = |root: Id| {
		let chosen: HashMap<ClassRef, ENodeSnapshot> = [leaf, root]
			.into_iter()
			.map(|class| (ClassRef::from(eg.find(class)), snapshot_of(eg.id_to_node(class))))
			.collect();
		Term {
			roots: smallvec![ClassRef::from(eg.find(root))],
			order: vec![ClassRef::from(eg.find(root)), ClassRef::from(eg.find(leaf))],
			chosen,
		}
	};
	let target = Ir::new(
		IrScope::Request {
			fragment: None,
		},
		RegionKind::Request,
	);
	let map = StmtMap::default();
	let refused = plan(&eg, &term(twice), &map, &target, &Basis::LOGICAL);
	assert!(matches!(
		refused,
		Err(Abort {
			rule: ElabRule::E5,
			..
		})
	));
	let allowed = plan(&eg, &term(once), &map, &target, &Basis::LOGICAL).expect("one read plans");
	assert!(allowed.is_empty());
}

/// Writing the LR5 form back, then printing, parsing and printing again, gives the same text, and
/// the written-back container verifies. A container whose statement copy is a clone of the whole
/// container stands in for the container's own copy and adoption.
#[test]
fn rewrite_round_trip() {
	let env = Env::new();
	let (mut fx, done) = rewritten(&env, lr5);
	let mut facts = FactCache::new();
	write_logical(
		&mut fx.ir,
		&mut facts,
		fx.stmt,
		&done.imported.eg,
		&done.candidate,
		&CloneSurgery,
		&env.cx(),
	)
	.expect("the rewrite is written back and verifies");
	assert!(fx.ir.op(done.root).is_none());
	let text = print(&fx.ir, fx.ir.top(), PrintOpts::default());
	let parsed = parse(
		&text,
		IrScope::Request {
			fragment: None,
		},
		registry::attr_syntaxes(),
	)
	.unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	assert_eq!(print(&parsed, parsed.top(), PrintOpts::default()), text);
}

// ---------------------------------------------------------------------------------------------
// The container's statement surgery
// ---------------------------------------------------------------------------------------------

/// A `FILTER` loop over an int literal, its body testing the element for truth. Answers the loop
/// op, which stands in the statement's block.
fn element_filter(fx: &mut Fixture) -> OpId {
	let source = fx.add(literal(Value::Number(Number::Int(3))), &[], 1);
	let source = fx.result(source);
	let sink = LoopSink::Filter;
	let looped = fx.add(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::ElementFilter,
			sink,
			order: SourceOrder::Positional,
			step: None,
		}),
		&[source],
		1,
	);
	let (_, body) = fx.region(looped, RegionKind::Loop, 2);
	let element = fx.ir.block(body).expect("the body exists").args[0];
	let test = fx.add_in(body, truthy(), &[element], 1, FrameId::new(0));
	let test = fx.result(test);
	fx.add_in(body, OpKind::Logical(LogOp::Yield), &[test], 0, FrameId::new(0));
	looped
}

/// A statement that reads two values defined before it, a literal and the truth of that literal,
/// and holds a lazy region and a loop body. Answers the two outside values.
fn with_outside_reads(fx: &mut Fixture) -> (ValueId, ValueId) {
	let (literal_value, truth_value) = {
		let mut rw = Rewriter::new(&mut fx.ir, &mut fx.facts);
		let defined = rw
			.create(
				InsertPoint::Before(fx.stmt),
				spec(literal(Value::Number(Number::Int(5))), &[], 1, FrameId::new(0)),
			)
			.expect("the outside literal is created");
		let defined = rw.ir().op(defined).expect("the op exists").results[0];
		let tested = rw
			.create(InsertPoint::Before(fx.stmt), spec(truthy(), &[defined], 1, FrameId::new(0)))
			.expect("the outside truth is created");
		(defined, rw.ir().op(tested).expect("the op exists").results[0])
	};
	let root = lr5(fx);
	let root = fx.result(root);
	fx.add(OpKind::Logical(LogOp::Eval), &[root], 1);
	fx.close();
	fx.add(binary(BinaryOperator::Add), &[literal_value, truth_value], 1);
	element_filter(fx);
	(literal_value, truth_value)
}

/// Every op, result, region and block argument of a statement copy maps to its original and back;
/// the copy's top region maps to the statement's region, its outside arguments to the values the
/// statement read, and the copy of an outside definition to that definition.
#[test]
fn container_copy_maps_every_id() {
	let mut fx = Fixture::new();
	let (outside_literal, outside_truth) = with_outside_reads(&mut fx);
	let (copy, map) =
		CONTAINER_SURGERY.copy_statement(&fx.ir, fx.stmt).expect("the statement is copied");
	let container = map.container.as_ref().expect("a container copy carries the container's map");

	let statement_region = fx.ir.op(fx.stmt).expect("the statement exists").regions[0];
	assert_eq!(map.regions_to_orig.get(&copy.top()), Some(&statement_region));
	assert_eq!(map.regions_to_copy.get(&statement_region), Some(&copy.top()));

	let copied = ops_of(&copy);
	assert_eq!(map.ops_to_orig.len(), copied.len());
	assert_eq!(map.ops_to_copy.len(), copied.len());
	for op in &copied {
		let original = *map.ops_to_orig.get(op).expect("the copied op has an original");
		assert_eq!(map.ops_to_copy.get(&original), Some(op));
		assert_eq!(container.original_op(*op), Some(original));
		assert!(fx.ir.op(original).is_some(), "the original is a live op");
		let data = copy.op(*op).expect("the copy exists");
		let held = fx.ir.op(original).expect("the original exists");
		assert_eq!(data.results.len(), held.results.len());
		for (new, old) in data.results.iter().zip(&held.results) {
			assert_eq!(map.values_to_orig.get(new), Some(old));
			assert_eq!(map.values_to_copy.get(old), Some(new));
		}
		assert_eq!(data.regions.len(), held.regions.len());
		for (new, old) in data.regions.iter().zip(&held.regions) {
			assert_eq!(map.regions_to_orig.get(new), Some(old));
			assert_eq!(map.regions_to_copy.get(old), Some(new));
			let blocks = |ir: &Ir, region: RegionId| -> Vec<ValueId> {
				let region = ir.region(region).expect("the region exists");
				region
					.blocks
					.iter()
					.flat_map(|block| ir.block(*block).expect("the block exists").args.to_vec())
					.collect()
			};
			let (new_args, old_args) = (blocks(&copy, *new), blocks(&fx.ir, *old));
			assert_eq!(new_args.len(), old_args.len());
			for (new, old) in new_args.iter().zip(&old_args) {
				assert_eq!(map.values_to_orig.get(new), Some(old));
				assert_eq!(map.values_to_copy.get(old), Some(new));
			}
		}
	}

	// The top block's arguments: the statement timestamp, then the outside value that is no
	// definition the copy could make by itself.
	let top_block = copy.region(copy.top()).expect("the top region exists").blocks[0];
	let args = copy.block(top_block).expect("the top block exists").args.clone();
	assert_eq!(args.len(), 2);
	assert_eq!(map.values_to_orig.get(&args[0]), Some(&fx.timestamp()));
	assert_eq!(map.values_to_orig.get(&args[1]), Some(&outside_truth));
	assert_eq!(map.values_to_copy.get(&outside_truth), Some(&args[1]));

	// The outside literal is copied with its definition, and the copy resolves to it.
	let copy_of_literal = *map.values_to_copy.get(&outside_literal).expect("the literal is mapped");
	let Some(ValueDef::OpResult {
		op: defining,
		..
	}) = copy.value(copy_of_literal).map(|data| data.def)
	else {
		panic!("the literal's copy is an op result");
	};
	assert!(container.is_outside_copy(defining));
	assert_eq!(
		map.ops_to_orig.get(&defining).copied(),
		fx.ir.value(outside_literal).and_then(|data| match data.def {
			ValueDef::OpResult {
				op,
				..
			} => Some(op),
			ValueDef::BlockArg {
				..
			} => None,
		})
	);
}

/// A copy adopted unchanged leaves the statement as it was: the same text, the statement's own
/// `OpId`, and every logical id of before still covered.
#[test]
fn container_adopt_of_an_unchanged_copy_round_trips() {
	let mut fx = Fixture::new();
	with_outside_reads(&mut fx);
	let before = ops_of(&fx.ir);
	let text = print(&fx.ir, fx.ir.top(), PrintOpts::default());
	let (copy, map) =
		CONTAINER_SURGERY.copy_statement(&fx.ir, fx.stmt).expect("the statement is copied");
	CONTAINER_SURGERY
		.adopt_statement(&mut fx.ir, &mut fx.facts, fx.stmt, copy, &map)
		.expect("the copy is adopted");
	assert_eq!(print(&fx.ir, fx.ir.top(), PrintOpts::default()), text);
	assert!(
		matches!(
			fx.ir.op(fx.stmt).map(|data| &data.kind),
			Some(OpKind::Logical(LogOp::Stmt { .. }))
		),
		"the statement keeps its id"
	);
	assert!(before.iter().all(|id| covered(&fx.ir, *id)));
}

/// A map that carries no container map cannot adopt, and the container is untouched.
#[test]
fn container_adopt_without_an_id_map_changes_nothing() {
	let mut fx = Fixture::new();
	with_outside_reads(&mut fx);
	let (copy, _) =
		CONTAINER_SURGERY.copy_statement(&fx.ir, fx.stmt).expect("the statement is copied");
	let map = identity_map(&fx.ir);
	assert!(map.container.is_none());
	let held = format!("{:?}", fx.ir);
	let text = print(&fx.ir, fx.ir.top(), PrintOpts::default());
	let adopted = CONTAINER_SURGERY.adopt_statement(&mut fx.ir, &mut fx.facts, fx.stmt, copy, &map);
	assert!(matches!(adopted, Err(IrError::Invariant { .. })));
	assert_eq!(format!("{:?}", fx.ir), held);
	assert_eq!(print(&fx.ir, fx.ir.top(), PrintOpts::default()), text);
}

/// A copy of one statement does not adopt in place of another, and the container is untouched.
#[test]
fn container_adopt_of_a_copy_of_another_statement_changes_nothing() {
	let mut fx = Fixture::new();
	let (outside_literal, _) = with_outside_reads(&mut fx);
	let ValueDef::OpResult {
		op: other,
		..
	} = fx.ir.value(outside_literal).expect("the value exists").def
	else {
		panic!("the outside literal is an op result");
	};
	let (copy, map) =
		CONTAINER_SURGERY.copy_statement(&fx.ir, fx.stmt).expect("the statement is copied");
	let held = format!("{:?}", fx.ir);
	let adopted = CONTAINER_SURGERY.adopt_statement(&mut fx.ir, &mut fx.facts, other, copy, &map);
	assert!(matches!(adopted, Err(IrError::Invariant { .. })), "{adopted:?}");
	assert_eq!(format!("{:?}", fx.ir), held);
}

/// The container's adoption drops what the fact cache held for the replaced code: its effects
/// and its pending folds.
#[test]
fn container_adopt_drops_the_facts_of_the_replaced_code() {
	let mut fx = Fixture::new();
	with_outside_reads(&mut fx);
	let replaced = ops_of(&fx.ir)
		.into_iter()
		.find(|op| {
			let Some(data) = fx.ir.op(*op) else {
				return false;
			};
			let region = fx.ir.block(data.parent).map(|block| block.parent);
			!data.results.is_empty()
				&& region.and_then(|held| fx.ir.parent_op(held)) == Some(fx.stmt)
		})
		.expect("the statement holds an op with a result");
	let value = fx.ir.op(replaced).expect("the op exists").results[0];
	{
		let mut rw = Rewriter::new(&mut fx.ir, &mut fx.facts)
			.with_registry(crate::exec::ir::analysis::cx::builtin_registry());
		rw.request_fold(FoldRequest {
			value,
			consumer: FoldConsumer::AlgebraicRule,
		});
		let _ = rw.facts(replaced);
	}
	let generation = fx.ir.generation(replaced);
	assert!(fx.facts.get(replaced, generation, &default_key(&fx.ir)).is_some());
	assert_eq!(fx.facts.pending_folds().count(), 1);
	let (copy, map) =
		CONTAINER_SURGERY.copy_statement(&fx.ir, fx.stmt).expect("the statement is copied");
	CONTAINER_SURGERY
		.adopt_statement(&mut fx.ir, &mut fx.facts, fx.stmt, copy, &map)
		.expect("the copy is adopted");
	assert!(fx.facts.get(replaced, generation, &default_key(&fx.ir)).is_none());
	assert_eq!(fx.facts.pending_folds().count(), 0);
}

/// With no legality snapshot taken, a retake answers `Ok` and takes none.
#[test]
fn retake_leaves_an_absent_snapshot_absent() {
	let mut fx = Fixture::new();
	element_filter(&mut fx);
	assert!(fx.facts.legality_snapshot().is_none());
	CONTAINER_SURGERY
		.retake_legality(
			&fx.ir,
			&mut fx.facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			fx.stmt,
		)
		.expect("an absent snapshot needs no retake");
	assert!(fx.facts.legality_snapshot().is_none());
}

/// After a copy and an adopt, the loops of the statement are keyed by the logical ids they stand
/// for and hold the analysis's answer for the adopted op, and an entry for another id is kept.
///
/// The snapshot is seeded with an answer that differs from the analysis's, so the test sees the
/// stored value change. The verifier's recompute check admits any loop its snapshot still covers,
/// so it cannot tell a retaken entry from a stale one.
#[test]
fn retake_rekeys_the_statement_loops() {
	let mut fx = Fixture::new();
	let (outside_literal, _) = with_outside_reads(&mut fx);
	let ValueDef::OpResult {
		op: unrelated,
		..
	} = fx.ir.value(outside_literal).expect("the value exists").def
	else {
		panic!("the outside literal is an op result");
	};
	let looped = ops_of(&fx.ir)
		.into_iter()
		.find(|op| {
			matches!(
				fx.ir.op(*op).map(|data| &data.kind),
				Some(OpKind::Logical(LogOp::Loop { .. }))
			)
		})
		.expect("the statement holds a loop");
	let real = loop_legality::of(&fx.ir, &mut fx.facts, looped).expect("the loop is analysed");
	let stale = LoopFacts {
		mode: if matches!(real.mode, LoopMode::Independent) {
			LoopMode::Serial(LoopRefusal::Unsplittable)
		} else {
			LoopMode::Independent
		},
		..real
	};
	fx.facts.set_legality_snapshot([(looped, stale.clone()), (unrelated, stale.clone())].into());

	let (copy, map) =
		CONTAINER_SURGERY.copy_statement(&fx.ir, fx.stmt).expect("the statement is copied");
	CONTAINER_SURGERY
		.adopt_statement(&mut fx.ir, &mut fx.facts, fx.stmt, copy, &map)
		.expect("the copy is adopted");
	assert!(fx.ir.op(looped).is_none(), "adoption gave the loop a fresh id");
	CONTAINER_SURGERY
		.retake_legality(
			&fx.ir,
			&mut fx.facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			fx.stmt,
		)
		.expect("the legality is retaken");

	let adopted = ops_of(&fx.ir)
		.into_iter()
		.find(|op| {
			matches!(
				fx.ir.op(*op).map(|data| &data.kind),
				Some(OpKind::Logical(LogOp::Loop { .. }))
			)
		})
		.expect("the adopted statement holds a loop");
	let expected = loop_legality::of(&fx.ir, &mut fx.facts, adopted).expect("the loop is analysed");
	let slot = fx.facts.legality_snapshot().expect("the snapshot is still taken");
	assert_eq!(slot.len(), 2);
	assert_eq!(format!("{:?}", slot[&looped]), format!("{expected:?}"));
	assert_ne!(format!("{:?}", slot[&looped]), format!("{stale:?}"));
	assert_eq!(format!("{:?}", slot[&unrelated]), format!("{stale:?}"));
}

/// Each catalogue rule's rewrite is written back through the container's own copy and adopt: the
/// write-back answers `Ok`, the statement verifies at the logical boundary and keeps its `OpId`,
/// and every logical id the statement held is covered.
#[test]
fn writeback_through_container_surgery() {
	let env = Env::new();
	for build in [lr5, lr6, lr7r, lr7l] {
		let (mut fx, done) = rewritten(&env, build);
		let mut facts = FactCache::new();
		write_logical(
			&mut fx.ir,
			&mut facts,
			fx.stmt,
			&done.imported.eg,
			&done.candidate,
			&CONTAINER_SURGERY,
			&env.cx(),
		)
		.expect("the rewrite is written back");
		verify(
			&fx.ir,
			&mut facts,
			VerifyAt::PhaseBoundary(Phase::Logical),
			&LOGICAL_PHASE,
			&env.cx(),
		)
		.expect("the adopted statement verifies");
		assert!(
			matches!(
				fx.ir.op(fx.stmt).map(|data| &data.kind),
				Some(OpKind::Logical(LogOp::Stmt { .. }))
			),
			"the statement keeps its id"
		);
		assert!(fx.ir.op(done.root).is_none(), "the matched root is gone");
		assert!(done.before.iter().all(|id| covered(&fx.ir, *id)));
	}
}

/// The context a request plans with holds the registered rules, the default limits and no
/// physical layer.
#[test]
fn for_request_plans_with_the_registered_rules() {
	let env = Env::new();
	let cx = PlanStmtCx::for_request(env.cx());
	assert!(std::ptr::eq(cx.rules, registered()));
	assert!(cx.seed.is_none());
	assert_eq!(cx.limits, EGRAPH_DEFAULT);
}

// ---------------------------------------------------------------------------------------------
// Gate and orchestration
// ---------------------------------------------------------------------------------------------

/// A rule set that lives as long as the process, for the hook that takes a `&'static RuleSet`.
fn rule_set(decls: &[&'static RuleDecl]) -> &'static RuleSet {
	Box::leak(Box::new(RuleSet::new(decls).expect("the rules are admitted")))
}

/// Adds the choice point of a subquery to a fixture's statement.
fn with_subquery(fx: &mut Fixture) -> ValueId {
	let subquery = fx.add(OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)), &[], 1);
	let (_, body) = fx.region(subquery, RegionKind::Subquery, 0);
	let inner = fx.add_in(body, literal(Value::None), &[], 1, FrameId::new(0));
	let inner = fx.result(inner);
	fx.add_in(body, OpKind::Expr(ExprOp::Yield), &[inner], 0, FrameId::new(0));
	fx.result(subquery)
}

/// The statement of `build`'s fixture: its root op read by an op outside the island, and a
/// subquery so the gate finds a choice point. The statement yields the value its root reads.
fn statement_of(build: fn(&mut Fixture) -> OpId) -> Fixture {
	let mut fx = Fixture::new();
	let root = build(&mut fx);
	let value = fx.result(root);
	let eval = fx.add(OpKind::Logical(LogOp::Eval), &[value], 1);
	let evaluated = fx.result(eval);
	with_subquery(&mut fx);
	fx.add(OpKind::Logical(LogOp::Yield), &[evaluated], 0);
	fx
}

fn add_only(fx: &mut Fixture) -> OpId {
	binary_of(fx, BinaryOperator::Add, 7, 0)
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
	tokio::runtime::Builder::new_current_thread()
		.build()
		.expect("a current-thread runtime")
		.block_on(future)
}

/// Plans the statement of `fx` through `plan_statement` under the context the arguments make.
fn plan_with(
	fx: &mut Fixture,
	env: &Env,
	rules: &RuleSet,
	limits: EgraphLimits,
	seed: Option<&dyn SeedHost>,
	surgery: &dyn StatementSurgery,
) -> EgraphOutcome {
	let cx = PlanStmtCx {
		effects: env.cx(),
		limits,
		rules,
		seed,
		surgery,
	};
	block_on(plan_statement(&mut fx.ir, &mut fx.facts, fx.stmt, &cx))
}

/// What a rule's rewrite changed in a statement, by op tag. Adoption gives every op a fresh id,
/// so the diff holds no id.
#[derive(Debug, Default, PartialEq, Eq)]
struct RewriteDiff {
	/// Per tag, how many ops the statement lost.
	removed: BTreeMap<String, usize>,
	/// Per tag, how many ops the statement gained.
	added: BTreeMap<String, usize>,
	/// The (tag, reason variant) of every op the run eliminated, sorted. The tag is the op's tag
	/// before the run.
	eliminated: Vec<(String, String)>,
}

/// The name of `tag` within its dialect: the `Debug` form of the dialect's own tag.
fn tag_name(tag: OpTag) -> String {
	match tag {
		OpTag::Expr(tag) => format!("{tag:?}"),
		OpTag::Logical(tag) => format!("{tag:?}"),
		OpTag::Phys(tag) => format!("{tag:?}"),
	}
}

/// The name of the variant of `reason`, without the op a variant may carry.
fn reason_name(reason: EliminationReason) -> String {
	let text = format!("{reason:?}");
	match text.split_once('(') {
		Some((name, _)) => name.to_owned(),
		None => text,
	}
}

/// How many ops of each tag the statement under `ir.top()` holds.
fn tag_counts(ir: &Ir) -> BTreeMap<String, usize> {
	let mut counts = BTreeMap::new();
	for op in ops_of(ir) {
		let data = ir.op(op).expect("a walked op exists");
		*counts.entry(tag_name(data.kind.tag())).or_insert(0) += 1;
	}
	counts
}

/// Tag counts as a map, for a literal diff.
fn tagged(counts: &[(&str, usize)]) -> BTreeMap<String, usize> {
	counts.iter().map(|(tag, count)| ((*tag).to_owned(), *count)).collect()
}

/// Plans the statement `build` makes, its root read by a `logical.eval`, with `rule` alone under
/// the container's statement surgery and roomy limits, logical-only. Answers what changed and the
/// planner's outcome.
///
/// The printed statement is equal before and after exactly when nothing was removed, added or
/// eliminated.
fn golden_run(
	rule: &'static RuleDecl,
	build: fn(&mut Fixture) -> OpId,
) -> (RewriteDiff, EgraphOutcome) {
	let env = Env::new();
	let set = rule_set(&[rule]);
	let mut fx = Fixture::new();
	let root = build(&mut fx);
	let value = fx.result(root);
	fx.add(OpKind::Logical(LogOp::Eval), &[value], 1);
	fx.close();

	let tags: HashMap<OpId, String> = ops_of(&fx.ir)
		.into_iter()
		.map(|op| (op, tag_name(fx.ir.op(op).expect("a walked op exists").kind.tag())))
		.collect();
	let eliminated_before = fx.ir.eliminated().to_vec();
	let counts_before = tag_counts(&fx.ir);
	let text_before = print(&fx.ir, fx.ir.top(), PrintOpts::default());

	let outcome = {
		let _forced = ForcedPlanning::new(EgraphMode::ForceOnLogicalOnly, Some(set));
		plan_with(&mut fx, &env, set, roomy(), None, &CONTAINER_SURGERY)
	};

	let counts_after = tag_counts(&fx.ir);
	let text_after = print(&fx.ir, fx.ir.top(), PrintOpts::default());
	let mut diff = RewriteDiff::default();
	for tag in counts_before.keys().chain(counts_after.keys()) {
		let before = counts_before.get(tag).copied().unwrap_or(0);
		let after = counts_after.get(tag).copied().unwrap_or(0);
		if after < before {
			diff.removed.insert(tag.clone(), before - after);
		} else if after > before {
			diff.added.insert(tag.clone(), after - before);
		}
	}
	for (op, reason) in fx.ir.eliminated() {
		if eliminated_before.contains(&(*op, *reason)) {
			continue;
		}
		let tag = tags.get(op).expect("an eliminated op stood in the statement before the run");
		diff.eliminated.push((tag.clone(), reason_name(*reason)));
	}
	diff.eliminated.sort();
	assert_eq!(text_before == text_after, diff == RewriteDiff::default(), "{text_before}");
	(diff, outcome)
}

/// What `rule` changes in the statement `build` makes; see [`golden_run`].
fn golden_diff(rule: &'static RuleDecl, build: fn(&mut Fixture) -> OpId) -> RewriteDiff {
	golden_run(rule, build).0
}

/// `truthy(1 AND false)` loses the literal operand, the short circuit with its lazy region, and
/// the truthy: the two literals go and one fresh `false` stands for the truthy, so the net literal
/// count falls by one and nothing is added. The fresh literal's provenance holds the truthy and one
/// of the literals, which are therefore not recorded as eliminated; every other dropped op is dead
/// and pure.
#[test]
fn golden_diff_and_false_truthy() {
	let (diff, outcome) = golden_run(&AND_FALSE_TRUTHY, lr5);
	assert!(outcome.chose_alternative);
	assert_eq!(outcome.fallback, None);
	assert_eq!(
		diff.removed,
		tagged(&[("Literal", 1), ("ShortCircuit", 1), ("Yield", 1), ("Truthy", 1)])
	);
	assert!(diff.added.is_empty());
	let dead = |tag: &str| (tag.to_owned(), "DeadPure".to_owned());
	assert_eq!(diff.eliminated, vec![dead("Literal"), dead("ShortCircuit"), dead("Yield")]);
}

/// `truthy(1 OR true)` loses the literal operand, the short circuit with its lazy region, and the
/// truthy: the two literals go and one fresh `true` stands for the truthy, so the net literal
/// count falls by one and nothing is added. The fresh literal's provenance holds the truthy and one
/// of the literals, which are therefore not recorded as eliminated; every other dropped op is dead
/// and pure.
#[test]
fn golden_diff_or_true_truthy() {
	let (diff, outcome) = golden_run(&OR_TRUE_TRUTHY, lr6);
	assert!(outcome.chose_alternative);
	assert_eq!(outcome.fallback, None);
	assert_eq!(
		diff.removed,
		tagged(&[("Literal", 1), ("ShortCircuit", 1), ("Yield", 1), ("Truthy", 1)])
	);
	assert!(diff.added.is_empty());
	let dead = |tag: &str| (tag.to_owned(), "DeadPure".to_owned());
	assert_eq!(diff.eliminated, vec![dead("Literal"), dead("ShortCircuit"), dead("Yield")]);
}

/// `7 * 0` loses the other operand and the product: the built `int 0` is the literal the statement
/// already holds, which dominates the use and is reused, so nothing is added and the `0` literal
/// stays. The `7` is dead and pure; the product is merged into the reused literal.
#[test]
fn golden_diff_mul_zero_int_r() {
	let (diff, outcome) = golden_run(&MUL_ZERO_INT_R, lr7r);
	assert!(outcome.chose_alternative);
	assert_eq!(outcome.fallback, None);
	assert_eq!(diff.removed, tagged(&[("Literal", 1), ("Binary", 1)]));
	assert!(diff.added.is_empty());
	assert_eq!(
		diff.eliminated,
		vec![
			("Binary".to_owned(), "MergedInto".to_owned()),
			("Literal".to_owned(), "DeadPure".to_owned()),
		]
	);
}

/// `0 * 7` loses the other operand and the product: the built `int 0` is the literal the statement
/// already holds, which dominates the use and is reused, so nothing is added and the `0` literal
/// stays. The `7` is dead and pure; the product is merged into the reused literal.
#[test]
fn golden_diff_mul_zero_int_l() {
	let (diff, outcome) = golden_run(&MUL_ZERO_INT_L, lr7l);
	assert!(outcome.chose_alternative);
	assert_eq!(outcome.fallback, None);
	assert_eq!(diff.removed, tagged(&[("Literal", 1), ("Binary", 1)]));
	assert!(diff.added.is_empty());
	assert_eq!(
		diff.eliminated,
		vec![
			("Binary".to_owned(), "MergedInto".to_owned()),
			("Literal".to_owned(), "DeadPure".to_owned()),
		]
	);
}

/// `REGISTERED` and `CATALOGUE` hold the same declarations in the same order.
#[test]
fn registered_equals_catalogue() {
	assert_eq!(REGISTERED.len(), CATALOGUE.len());
	for (registered, written) in REGISTERED.iter().zip(CATALOGUE) {
		assert!(std::ptr::eq(*registered, *written), "{} differs", written.name);
	}
}

/// `<block argument> * 0`: the left operand is a block argument with no type fact, as a field read
/// is.
fn product_over_unknown(fx: &mut Fixture) -> OpId {
	let left = fx.timestamp();
	let right = typed_literal(fx, Value::Number(Number::Int(0)));
	fx.add(binary(BinaryOperator::Multiply), &[left, right], 1)
}

/// `1.5 * 0`: the left operand is a float by type fact.
fn float_times_zero(fx: &mut Fixture) -> OpId {
	let left = typed_literal(fx, Value::Number(Number::Float(1.5)));
	let right = typed_literal(fx, Value::Number(Number::Int(0)));
	fx.add(binary(BinaryOperator::Multiply), &[left, right], 1)
}

/// The zero product is rewritten only where the other operand is an int by type fact.
#[test]
fn lr7_needs_an_int_fact() {
	let builds: [fn(&mut Fixture) -> OpId; 2] = [product_over_unknown, float_times_zero];
	for build in builds {
		let (diff, outcome) = golden_run(&MUL_ZERO_INT_R, build);
		assert_eq!(outcome.ran, Ok(()));
		assert!(!outcome.chose_alternative);
		assert_eq!(diff, RewriteDiff::default());
	}
}

/// `truthy((1 + 'a') AND false)`: the left operand may raise, so the conjunction cannot be dropped.
fn lr5_over_a_fallible_operand(fx: &mut Fixture) -> OpId {
	let one = fx.add(literal(Value::Number(Number::Int(1))), &[], 1);
	let text = fx.add(literal(Value::String(Strand::from("a"))), &[], 1);
	let (one, text) = (fx.result(one), fx.result(text));
	let sum = fx.add(binary(BinaryOperator::Add), &[one, text], 1);
	let left = fx.result(sum);
	truthy_over(fx, left, ShortCircuitOp::And, false)
}

#[test]
fn lr5_does_not_drop_a_fallible_operand() {
	let (diff, outcome) = golden_run(&AND_FALSE_TRUTHY, lr5_over_a_fallible_operand);
	assert_eq!(outcome.ran, Ok(()));
	assert!(!outcome.chose_alternative);
	assert_eq!(diff, RewriteDiff::default());
}

/// A physical layer that answers no table facts and converts nothing, so a seeded run over it
/// seeds no physical node.
struct StubHost {
	order_ok: bool,
}

impl SeedHost for StubHost {
	fn table_facts<'a>(
		&'a self,
		_sources: &'a [(TableName, Option<With>, SnapshotId)],
	) -> SeedFuture<'a, Vec<PhysTableFacts>> {
		Box::pin(async { Ok(Vec::new()) })
	}

	fn convert_copy<'a>(
		&'a self,
		_copy: &'a mut Ir,
		_facts: &'a mut FactCache,
	) -> SeedFuture<'a, Vec<(ValueId, ValueId)>> {
		Box::pin(async { Ok(Vec::new()) })
	}

	fn order_sig(&self, _op: &PhysOp, input: OrderSig) -> OrderSig {
		input
	}

	fn order_change_admissible(&self, _ir: &Ir, _facts: &mut FactCache, _source: OpId) -> bool {
		self.order_ok
	}

	fn cost(&self) -> &dyn CostHook {
		&ExtractWeights
	}
}

/// The future `plan_statement` returns crosses threads, so the planner may hold the e-graph
/// across the awaits of the physical layer.
#[allow(dead_code)]
fn planner_future_is_send(ir: &mut Ir, facts: &mut FactCache, stmt: OpId, cx: &PlanStmtCx<'_>) {
	fn assert_send<T: Send>(_: T) {}
	assert_send(plan_statement(ir, facts, stmt, cx));
}

/// Whether the statement holds logical ops, physical ops, or both.
fn dialects_of(ir: &Ir, stmt: OpId) -> (bool, bool) {
	let (mut logical, mut physical) = (false, false);
	let regions = ir.op(stmt).map(|data| data.regions.clone()).unwrap_or_default();
	for region in regions {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			match ir.op(op).map(|data| data.kind.tag()) {
				Some(OpTag::Logical(_)) => logical = true,
				Some(OpTag::Phys(_)) => physical = true,
				_ => {}
			}
			Walk::Continue
		});
	}
	(logical, physical)
}

/// The gate runs the e-graph on a statement with a choice point and a pre-matching rule, and
/// skips every other for its own reason.
#[test]
fn gate_verdicts() {
	let set = RuleSet::new(CATALOGUE).expect("the catalogue is admitted");
	for build in [lr5, lr6, lr7r, lr7l] {
		let fx = statement_of(build);
		assert_eq!(gate(&fx.ir, fx.stmt, &set), Gate::Run(Mode::LogicalOnly));
	}
	// Only `expr.binary{add}`: a choice point and no rule that pre-matches.
	let fx = statement_of(add_only);
	assert_eq!(gate(&fx.ir, fx.stmt, &set), Gate::Skip(SkipReason::NoApplicableRule));
	// A matching op and no choice point.
	let mut fx = Fixture::new();
	let root = lr5(&mut fx);
	let value = fx.result(root);
	fx.add(OpKind::Logical(LogOp::Eval), &[value], 1);
	fx.close();
	assert_eq!(gate(&fx.ir, fx.stmt, &set), Gate::Skip(SkipReason::NoChoicePoint));
	// No rule at all.
	let none = RuleSet::default();
	let fx = statement_of(lr5);
	assert_eq!(gate(&fx.ir, fx.stmt, &none), Gate::Skip(SkipReason::NoApplicableRule));
}

/// A join, a graph-step loop, and a subquery in either dialect are choice points; other loops and
/// ops are not.
#[test]
fn choice_points_are_the_four_ops() {
	let loop_of = |kind| {
		OpKind::Logical(LogOp::Loop {
			kind,
			sink: LoopSink::Output,
			order: SourceOrder::Positional,
			step: None,
		})
	};
	assert!(is_choice_point(&OpKind::Logical(LogOp::Join {
		kind: JoinKind::Inner,
		alias: RelationAlias::new(0),
	})));
	assert!(is_choice_point(&loop_of(LoopKind::GraphStep)));
	assert!(is_choice_point(&OpKind::Logical(LogOp::Subquery)));
	assert!(is_choice_point(&OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply))));
	for kind in [
		LoopKind::Select,
		LoopKind::Write,
		LoopKind::For,
		LoopKind::Closure,
		LoopKind::ElementFilter,
		LoopKind::Recurse,
		LoopKind::Broadcast,
	] {
		assert!(!is_choice_point(&loop_of(kind)), "{kind:?}");
	}
	assert!(!is_choice_point(&OpKind::Logical(LogOp::Eval)));
	assert!(!is_choice_point(&truthy()));
}

/// A run is seeded exactly when an implementation rule pre-matches.
#[test]
fn gate_picks_seeded_by_implementation_rules() {
	let found = |choice_point, mask| Scan {
		choice_point,
		mask,
	};
	assert_eq!(decide(found(false, 0b11), 0b10), Gate::Skip(SkipReason::NoChoicePoint));
	assert_eq!(decide(found(true, 0), 0b10), Gate::Skip(SkipReason::NoApplicableRule));
	assert_eq!(decide(found(true, 0b01), 0b10), Gate::Run(Mode::LogicalOnly));
	assert_eq!(decide(found(true, 0b11), 0b10), Gate::Run(Mode::Seeded));
	assert_eq!(decide(found(true, 0b10), 0b10), Gate::Run(Mode::Seeded));
	assert_eq!(decide(found(true, 0b01), 0), Gate::Run(Mode::LogicalOnly));
}

/// Every skip is reported with its reason and leaves the container as it was, and each call is
/// counted.
#[test]
fn plan_statement_skips() {
	let env = Env::new();
	let set = rule_set(CATALOGUE);
	let calls = counts().planned;
	let skipped = |build: fn(&mut Fixture) -> OpId, mode, seed: Option<&dyn SeedHost>| {
		let mut fx = statement_of(build);
		let held = format!("{:?}", fx.ir);
		let _forced = ForcedPlanning::new(mode, Some(set));
		let outcome = plan_with(&mut fx, &env, set, roomy(), seed, &NoSurgery);
		assert_eq!(format!("{:?}", fx.ir), held);
		outcome
	};
	let outcome = skipped(lr5, EgraphMode::ForceOff, None);
	assert_eq!(outcome, EgraphOutcome::skipped(SkipReason::Forced));
	let outcome = skipped(add_only, EgraphMode::Default, None);
	assert_eq!(outcome.ran, Err(SkipReason::NoApplicableRule));
	assert_eq!(outcome.mode, None);
	let outcome = skipped(lr5, EgraphMode::ForceOnSeeded, None);
	assert_eq!(outcome, EgraphOutcome::skipped(SkipReason::NotYet));
	// A seeded run with no row producer that may reorder, and no implementation rule, is skipped
	// by the gate and not by a force.
	let host = StubHost {
		order_ok: false,
	};
	let outcome = skipped(lr5, EgraphMode::ForceOnSeeded, Some(&host));
	assert_eq!(outcome.ran, Ok(()));
	assert_eq!(outcome.mode, Some(Mode::Seeded));
	assert!(counts().planned >= calls + 4);
}

/// A logical-only run reports its counters, writes the rewritten form back through the
/// container's surgery, and leaves a statement that is wholly logical.
#[test]
fn plan_statement_counters() {
	let env = Env::new();
	let set = rule_set(CATALOGUE);
	for build in [lr5, lr6, lr7r, lr7l] {
		let mut fx = statement_of(build);
		let _forced = ForcedPlanning::new(EgraphMode::Default, Some(set));
		let outcome = plan_with(&mut fx, &env, set, roomy(), None, &CloneSurgery);
		assert_eq!(outcome.ran, Ok(()));
		assert_eq!(outcome.mode, Some(Mode::LogicalOnly));
		assert_eq!(outcome.rounds, 1);
		assert!((1..=roomy().iterations).contains(&outcome.iterations));
		assert!(outcome.nodes > 0);
		assert_eq!(outcome.union_refused, 0);
		assert_eq!(outcome.fallback, None);
		assert!(outcome.chose_alternative);
		assert_eq!(dialects_of(&fx.ir, fx.stmt).1, false);
	}

	// The statement's iteration limit stops the round.
	let mut fx = statement_of(lr5);
	let limits = EgraphLimits {
		iterations: 1,
		..roomy()
	};
	let outcome = plan_with(&mut fx, &env, set, limits, None, &CloneSurgery);
	assert_eq!(outcome.iterations, 1);
	assert_eq!(outcome.stopped_by, Some(Limit::Iterations));

	// A container with no statement copy keeps the statement as it was.
	let mut fx = statement_of(lr5);
	let held = format!("{:?}", fx.ir);
	let outcome = plan_with(&mut fx, &env, set, roomy(), None, &NoSurgery);
	assert_eq!(outcome.fallback, Some(FallbackReason::WritebackAborted));
	assert!(!outcome.chose_alternative);
	assert_eq!(format!("{:?}", fx.ir), held);
}

/// The work bounds decide the plan; the wall is a safety stop. With no wall each statement
/// saturates and adopts its rewrite. Under the default limits a run either plans and writes back
/// exactly that, or meets the wall first (a debug build under load may) and keeps the statement as
/// it was: the wall never picks a different rewrite.
#[test]
fn the_default_limits_plan_as_an_unbounded_wall_does() {
	let env = Env::new();
	let set = rule_set(CATALOGUE);
	let unbounded = EgraphLimits {
		wall: Duration::MAX,
		..EGRAPH_DEFAULT
	};
	assert!(EGRAPH_DEFAULT.wall >= Duration::from_millis(5), "the wall is a safety stop");
	let kinds_of = |fx: &Fixture| -> Vec<String> {
		let region = fx.ir.op(fx.stmt).expect("the statement exists").regions[0];
		crate::exec::ir::core::walk::ops_in(&fx.ir, region)
			.map(|op| format!("{:?}", fx.ir.op(op).expect("the op exists").kind))
			.collect()
	};
	for build in [lr5, lr6, lr7r, lr7l] {
		let _forced = ForcedPlanning::new(EgraphMode::Default, Some(set));
		let mut fx = statement_of(build);
		let saturated = plan_with(&mut fx, &env, set, unbounded, None, &CloneSurgery);
		assert_eq!(saturated.stopped_by, None, "the work bounds end the run");
		assert!(saturated.chose_alternative);
		let rewritten = kinds_of(&fx);

		let mut fx = statement_of(build);
		let as_built = kinds_of(&fx);
		let bounded = plan_with(&mut fx, &env, set, EGRAPH_DEFAULT, None, &CloneSurgery);
		match bounded.stopped_by {
			None => assert_eq!((bounded, kinds_of(&fx)), (saturated, rewritten)),
			Some(Limit::Wall) => {
				assert!(!bounded.chose_alternative, "a run the wall stops adopts nothing");
				assert_eq!(kinds_of(&fx), as_built, "a run the wall stops keeps the statement");
			}
			Some(other) => panic!("the default limits stopped the run at {other:?}"),
		}
	}
}

/// A limit answers what the skip path answers: nothing is written, whatever the mode, and the
/// answer is the same on every run.
#[test]
fn plan_statement_stops_leave_the_statement() {
	let env = Env::new();
	let set = rule_set(CATALOGUE);
	let host = StubHost {
		order_ok: true,
	};

	for mode in [EgraphMode::ForceOnLogicalOnly, EgraphMode::ForceOnSeeded] {
		let _forced = ForcedPlanning::new(mode, Some(set));
		let mut fx = statement_of(lr5);
		let held = format!("{:?}", fx.ir);
		let limits = EgraphLimits {
			wall: Duration::ZERO,
			..roomy()
		};
		let outcome = plan_with(&mut fx, &env, set, limits, Some(&host), &CloneSurgery);
		assert_eq!(outcome.fallback, Some(FallbackReason::WallBeforeSeeding), "{mode:?}");
		assert_eq!(outcome.stopped_by, Some(Limit::Wall), "{mode:?}");
		assert!(!outcome.chose_alternative, "{mode:?}");
		assert_eq!(format!("{:?}", fx.ir), held, "{mode:?}");
	}

	let _forced = ForcedPlanning::new(EgraphMode::ForceOnLogicalOnly, Some(set));
	for limits in [
		EgraphLimits {
			nodes: 1,
			iterations: 0,
			..roomy()
		},
		EgraphLimits {
			iterations: 0,
			..roomy()
		},
	] {
		let mut outcomes: Vec<EgraphOutcome> = Vec::new();
		for _ in 0..10 {
			let mut fx = statement_of(lr5);
			let held = format!("{:?}", fx.ir);
			outcomes.push(plan_with(&mut fx, &env, set, limits, None, &CloneSurgery));
			assert_eq!(format!("{:?}", fx.ir), held, "{limits:?}");
		}
		assert!(outcomes.windows(2).all(|pair| pair[0] == pair[1]), "{limits:?}");
		assert!(outcomes[0].stopped_by.is_some(), "{limits:?}");
		assert!(!outcomes[0].chose_alternative, "{limits:?}");
	}
}

/// `count` statements `truthy(<n> and false)`, each read outside the island, built in the order
/// `order` names `n`s. Every one is a match of the `and`-false rule.
fn many_and_false(order: &[usize]) -> Fixture {
	let mut fx = Fixture::new();
	for &n in order {
		let lit = fx.add(literal(Value::Number(Number::Int(n as i64))), &[], 1);
		let left = fx.result(lit);
		let short = fx.add(
			OpKind::Expr(ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			}),
			&[left],
			1,
		);
		let (_, block) = fx.region(short, RegionKind::LazyOperand, 0);
		let right = fx.add_in(block, literal(Value::Bool(false)), &[], 1, FrameId::new(0));
		let right = fx.result(right);
		fx.add_in(block, closing_yield(), &[right], 0, FrameId::new(0));
		let short = fx.result(short);
		let held = fx.add(truthy(), &[short], 1);
		let held = fx.result(held);
		fx.add(OpKind::Logical(LogOp::Eval), &[held], 1);
	}
	fx.close();
	fx
}

/// A rule over more matches than the cap answers the skip path with `MatchCapHit`, and the answer
/// does not depend on the order the islands were imported in.
#[test]
fn plan_independent_of_import_order() {
	let env = Env::new();
	let set = rule_set(&[&AND_FALSE_TRUTHY]);
	let _forced = ForcedPlanning::new(EgraphMode::ForceOnLogicalOnly, Some(set));
	let count = MATCH_CAP_PER_RULE + 44;
	let forward: Vec<usize> = (0..count).collect();
	let backward: Vec<usize> = forward.iter().rev().copied().collect();
	let mut printed = Vec::new();
	for order in [&forward, &backward] {
		let mut fx = many_and_false(order);
		let held = format!("{:?}", fx.ir);
		let outcome = plan_with(&mut fx, &env, set, roomy(), None, &CloneSurgery);
		assert_eq!(outcome.fallback, Some(FallbackReason::MatchCapHit));
		assert!(!outcome.chose_alternative);
		assert_eq!(format!("{:?}", fx.ir), held);
		printed.push(format!("{outcome:?}"));
	}
	assert_eq!(printed[0], printed[1]);
}

/// After planning, a statement holds logical ops or physical ops, never both, in every mode and
/// whether or not the e-graph changed it.
#[test]
fn no_mixed_statement_reaches_convert() {
	let env = Env::new();
	let set = rule_set(CATALOGUE);
	let host = StubHost {
		order_ok: true,
	};
	for mode in [
		EgraphMode::Default,
		EgraphMode::ForceOff,
		EgraphMode::ForceOnLogicalOnly,
		EgraphMode::ForceOnSeeded,
	] {
		for build in [lr5, lr6, lr7r, lr7l, add_only] {
			for surgery in [&CloneSurgery as &dyn StatementSurgery, &NoSurgery] {
				let mut fx = statement_of(build);
				let _forced = ForcedPlanning::new(mode, Some(set));
				plan_with(&mut fx, &env, set, roomy(), Some(&host), surgery);
				let (logical, physical) = dialects_of(&fx.ir, fx.stmt);
				assert!(!(logical && physical), "{mode:?}");
			}
		}
	}
}

// ---------------------------------------------------------------------------------------------
// The request path on a datastore
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "kv-mem")]
mod kv {
	use super::*;
	use crate::corpus_slices::{assert_slices_cover, slice, sliced_tests};
	use crate::exec::ir::{RequestInput, RequestStatement};
	use crate::kvs::Datastore;

	/// A datastore with the test namespace and database in place, built as the language-test
	/// runner builds one: with no maintenance tasks, so nothing drains a queued event while a
	/// request runs.
	async fn datastore() -> Arc<Datastore> {
		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::TransactionType;

		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.expect("a memory datastore");
		let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
		tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
		tx.commit().await.expect("committing the namespace");
		ds
	}

	/// Each statement's outcome as SurrealQL text, or its error message; or the error of the
	/// request itself.
	async fn run(sql: &str) -> Result<Vec<Result<String, String>>, String> {
		run_on(&*datastore().await, sql).await
	}

	/// [`run`] of `query` in a request of its own, over a datastore that [`PRELUDE`] has already
	/// built. A statement after a catalog change in its own request is planned late-bound, when the
	/// request reaches it, so the prelude runs first for the query to plan with the request.
	async fn run_over_prelude(query: &str) -> Result<Vec<Result<String, String>>, String> {
		let ds = datastore().await;
		let built = run_on(&ds, PRELUDE).await.expect("the prelude runs");
		assert!(built.iter().all(Result::is_ok), "the prelude runs: {built:?}");
		run_on(&ds, query).await
	}

	/// [`run`] over the datastore `ds`.
	async fn run_on(ds: &Datastore, sql: &str) -> Result<Vec<Result<String, String>>, String> {
		use surrealdb_types::ToSql;

		let session = crate::dbs::Session::owner().with_ns("test").with_db("test");
		ds.execute(sql, &session, None)
			.await
			.map(|results| {
				results
					.into_iter()
					.map(|result| {
						result.result.map(|value| value.to_sql()).map_err(|error| error.to_string())
					})
					.collect()
			})
			.map_err(|error| error.to_string())
	}

	/// The limits the rewrite runs plan under: the default node and iteration bounds with a wall
	/// no debug build reaches, so a run measures the rewrite and not the clock.
	const UNHURRIED: EgraphLimits = EgraphLimits {
		wall: Duration::from_secs(3600),
		..EGRAPH_DEFAULT
	};

	const PRELUDE: &str = "DEFINE TABLE t SCHEMALESS; DEFINE TABLE u SCHEMALESS; \
		DEFINE FIELD y ON u TYPE int; CREATE t:1; CREATE t:2; \
		CREATE u:1 SET y = 1, n = 5, tags = [1, 2]; CREATE u:2 SET y = 0, n = 7, tags = [1, 2]; \
		CREATE u:3 SET y = 2, n = NONE, tags = [1, 2];";

	/// Statements each of which a rule of the catalogue matches and rewrites. A loop is an opaque
	/// leaf of the import, so each product stands outside every loop, beside a table read that
	/// gives the gate a choice point.
	const MATCHING: &[&str] =
		&["RETURN [(SELECT VALUE id FROM t), 7 * 0];", "RETURN [(SELECT VALUE id FROM t), 0 * 7];"];

	/// The statement runs to the same results, errors and order with `rule` registered and with
	/// no rule; the request reaches the planner in both runs, and the rule rewrites a statement
	/// in the first and none in the second.
	async fn rewrite_then_run_equals_run(query: &str, rule: &'static RuleDecl) {
		let before = counts();
		let with = {
			let _forced = ForcedPlanning::new(EgraphMode::Default, Some(rule_set(&[rule])))
				.with_limits(UNHURRIED);
			run_over_prelude(query).await
		};
		let between = counts();
		let without = {
			let _forced = ForcedPlanning::new(EgraphMode::Default, Some(rule_set(&[])));
			run_over_prelude(query).await
		};
		let after = counts();
		assert_eq!(with, without);
		assert!(
			between.planned > before.planned && after.planned > between.planned,
			"the request did not reach the e-graph planner"
		);
		assert!(
			between.adopted > before.adopted,
			"the rule did not rewrite a statement of the request"
		);
		assert_eq!(after.adopted, between.adopted, "a statement was rewritten with no rule");
	}

	/// The statement runs to the same results, errors and order with `rule` registered and with
	/// no rule, the request reaches the planner in both runs, and no statement is rewritten in
	/// either.
	async fn reached_and_left_alone(query: &str, rule: &'static RuleDecl) {
		let before = counts();
		let with = {
			let _forced = ForcedPlanning::new(EgraphMode::Default, Some(rule_set(&[rule])))
				.with_limits(UNHURRIED);
			run_over_prelude(query).await
		};
		let between = counts();
		let without = {
			let _forced = ForcedPlanning::new(EgraphMode::Default, Some(rule_set(&[])));
			run_over_prelude(query).await
		};
		let after = counts();
		assert_eq!(with, without);
		assert!(
			between.planned > before.planned && after.planned > between.planned,
			"the request did not reach the e-graph planner"
		);
		assert_eq!(after.adopted, before.adopted, "a statement was rewritten");
	}

	/// `truthy(x AND false)` stands only in a loop body, an element filter's predicate or a
	/// closure's, and a loop is an opaque leaf of the import (`import::islands`): the request
	/// reaches the planner and the rule rewrites nothing.
	#[tokio::test]
	async fn rewrite_then_run_equals_run_lr5() {
		reached_and_left_alone(
			"SELECT VALUE (SELECT VALUE tags[WHERE $this > 0 AND false] FROM u) FROM t;",
			&AND_FALSE_TRUTHY,
		)
		.await;
	}

	/// `truthy(x OR true)` stands only in a loop body, as [`rewrite_then_run_equals_run_lr5`]
	/// states for its conjunction.
	#[tokio::test]
	async fn rewrite_then_run_equals_run_lr6() {
		reached_and_left_alone(
			"SELECT VALUE (SELECT VALUE tags[WHERE $this > 0 OR true] FROM u) FROM t;",
			&OR_TRUE_TRUTHY,
		)
		.await;
	}

	#[tokio::test]
	async fn rewrite_then_run_equals_run_lr7r() {
		rewrite_then_run_equals_run("RETURN [(SELECT VALUE id FROM t), 7 * 0];", &MUL_ZERO_INT_R)
			.await;
	}

	/// A product over a field read carries no type fact, so neither zero-product rule rewrites it:
	/// the request reaches the planner, the answers are equal and no statement is adopted.
	#[tokio::test]
	async fn rewrite_leaves_a_field_product_alone() {
		let query = "SELECT VALUE (SELECT VALUE y * 0 FROM u) FROM t;";
		let before = counts();
		let with = {
			let _forced = ForcedPlanning::new(
				EgraphMode::Default,
				Some(rule_set(&[&MUL_ZERO_INT_R, &MUL_ZERO_INT_L])),
			)
			.with_limits(UNHURRIED);
			run_over_prelude(query).await
		};
		let between = counts();
		let without = {
			let _forced = ForcedPlanning::new(EgraphMode::Default, Some(rule_set(&[])));
			run_over_prelude(query).await
		};
		assert_eq!(with, without);
		assert!(between.planned > before.planned, "the request did not reach the e-graph planner");
		assert_eq!(between.adopted, before.adopted, "a field product was rewritten");
	}

	#[tokio::test]
	async fn rewrite_then_run_equals_run_lr7l() {
		rewrite_then_run_equals_run("RETURN [(SELECT VALUE id FROM t), 0 * 7];", &MUL_ZERO_INT_L)
			.await;
	}

	/// Whether the language suite runs the file `text` as a test: its header sets no `run = false`
	/// in its `[test]` table. A file that does is a fixture another file imports (a schema, a
	/// dataset a benchmark loads), which the suite never runs on its own.
	fn the_suite_runs(text: &str) -> bool {
		let Some(header) = text
			.trim_start()
			.strip_prefix("/**")
			.and_then(|rest| rest.split_once("*/").map(|(header, _)| header))
		else {
			return true;
		};
		let mut in_test = false;
		for line in header.lines().map(str::trim) {
			if line.starts_with('[') {
				in_test = line == "[test]";
			} else if in_test
				&& let Some((key, value)) = line.split_once('=')
				&& key.trim() == "run"
				&& value.trim() == "false"
			{
				return false;
			}
		}
		true
	}

	/// The language tests' files the suite runs ([`the_suite_runs`]), as the path and the text, in
	/// path order. The text is used as written: the header comment is a SurrealQL comment, and no
	/// expectation is read.
	fn corpus() -> Vec<(String, String)> {
		fn visit(dir: &Path, out: &mut Vec<PathBuf>) {
			for entry in fs::read_dir(dir).expect("the directory reads") {
				let path = entry.expect("the entry reads").path();
				if path.is_dir() {
					visit(&path, out);
				} else if path.extension().is_some_and(|ext| ext == "surql") {
					out.push(path);
				}
			}
		}
		let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../language-tests/tests");
		let mut paths = Vec::new();
		visit(&root, &mut paths);
		paths.sort();
		assert!(paths.len() > 100, "the language tests number {}", paths.len());
		paths
			.into_iter()
			.filter_map(|path| {
				let text = fs::read_to_string(&path).ok()?;
				the_suite_runs(&text).then(|| (path.to_string_lossy().into_owned(), text))
			})
			.collect()
	}

	/// A statement whose answer differs between two runs of one binary says nothing about the
	/// e-graph.
	const NONDETERMINISTIC: &[&str] = &[
		"rand::",
		// `ORDER BY rand()` draws a fresh order on every run.
		"rand()",
		"RAND()",
		// `array::shuffle` and its method form draw a fresh order on every call.
		"shuffle",
		"uuid",
		"ulid",
		"time::now",
		"sleep(",
		"SLEEP",
		"sequence::",
		"<future>",
		// A salted password hash differs on every run.
		"argon2",
		"bcrypt",
		"pbkdf2",
		"scrypt",
		// A bearer grant draws a fresh key and stamps the time it was made.
		"GRANT FOR",
	];

	fn deterministic_corpus() -> Vec<(String, String)> {
		corpus()
			.into_iter()
			.filter(|(_, text)| !NONDETERMINISTIC.iter().any(|needle| text.contains(needle)))
			.collect()
	}

	/// The slices of each corpus hold every file once, so the sliced corpus tests together walk
	/// the whole corpus, and no slice is empty.
	#[test]
	fn slices_cover_each_corpus() {
		for (name, files) in
			[("corpus", corpus()), ("deterministic corpus", deterministic_corpus())]
		{
			let paths: Vec<String> = files.into_iter().map(|(path, _)| path).collect();
			assert_slices_cover(name, &paths);
		}
	}

	/// The printed physical IR a request plans to, or why it does not plan.
	async fn planned(text: &str) -> Option<Result<String, String>> {
		let parsed = crate::syn::parse(text).ok()?.expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(
			&request_statements,
			IrScope::Request {
				fragment: None,
			},
		);
		Some(match crate::exec::ir::tests_plan_request::plan("", &input).await {
			Ok(planned) => Ok(print(&planned.ir, planned.ir.top(), PrintOpts::default())),
			Err(error) => Err(format!("{error:?}")),
		})
	}

	/// With no rule, the seeded run plans what the skip path plans, and the logical-only run
	/// leaves the container unchanged. Each forced run reaches the planner, and the seeded run
	/// reaches all-physical extraction: a request path with no physical layer behind it skips
	/// Worker threads a corpus slice divides its files across. Each worker runs its own
	/// single-threaded runtime on [`RUNNER_STACK`] so thread-local planning overrides and counters
	/// stay isolated per worker.
	const CORPUS_WORKERS: usize = 8;

	/// Runs `step` across the items of `items` on up to [`CORPUS_WORKERS`] threads, each with a
	/// [`RUNNER_STACK`] stack and its own single-threaded runtime, and returns each worker's
	/// accumulated state.
	fn par_corpus_slice<'a, T, Acc, F, Fut>(
		items: &'a [T],
		init: impl Fn() -> Acc + Send + Sync,
		step: F,
	) -> Vec<Acc>
	where
		T: Sync,
		Acc: Send,
		F: Fn(Acc, &'a T) -> Fut + Send + Sync,
		Fut: Future<Output = Acc>,
	{
		use std::sync::atomic::{AtomicUsize, Ordering};

		let next = AtomicUsize::new(0);
		let workers = CORPUS_WORKERS.min(items.len()).max(1);
		std::thread::scope(|scope| {
			let mut handles = Vec::with_capacity(workers);
			for _ in 0..workers {
				let next = &next;
				let init = &init;
				let step = &step;
				handles.push(
					std::thread::Builder::new()
						.stack_size(RUNNER_STACK)
						.spawn_scoped(scope, move || {
							tokio::runtime::Builder::new_current_thread()
								.enable_all()
								.build()
								.expect("a runtime")
								.block_on(async {
									let mut acc = init();
									loop {
										let idx = next.fetch_add(1, Ordering::Relaxed);
										let Some(item) = items.get(idx) else {
											break;
										};
										acc = step(acc, item).await;
									}
									acc
								})
						})
						.expect("a worker thread"),
				);
			}
			handles
				.into_iter()
				.map(|h| h.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
				.collect()
		})
	}

	/// seeded mode and fails here. Runs the slice `k` of the corpus.
	fn seed_equals_skip_path(k: usize) {
		let none = rule_set(&[]);
		let items = slice(corpus(), k);
		let totals = par_corpus_slice(
			&items,
			|| (0usize, 0usize, 0usize),
			|(mut planned_seeded, mut planned_logical, mut extracted), (path, text)| async move {
				let off = {
					let _forced = ForcedPlanning::new(EgraphMode::ForceOff, Some(none));
					planned(text).await
				};
				for mode in [EgraphMode::ForceOnSeeded, EgraphMode::ForceOnLogicalOnly] {
					let before = counts();
					let on = {
						let _forced = ForcedPlanning::new(mode, Some(none)).with_limits(UNHURRIED);
						planned(text).await
					};
					let after = counts();
					if mode == EgraphMode::ForceOnSeeded {
						planned_seeded += after.planned - before.planned;
						extracted += after.extracted - before.extracted;
					} else {
						planned_logical += after.planned - before.planned;
					}
					assert_eq!(on, off, "{path} under {mode:?}");
				}
				(planned_seeded, planned_logical, extracted)
			},
		);
		let (planned_seeded, planned_logical, extracted) =
			totals.into_iter().fold((0usize, 0usize, 0usize), |(ps, pl, ex), (dps, dpl, dex)| {
				(ps + dps, pl + dpl, ex + dex)
			});
		assert!(planned_seeded > 0, "no request reached the e-graph planner in seeded mode");
		assert!(planned_logical > 0, "no request reached the e-graph planner in logical mode");
		assert!(extracted > 0, "no seeded run reached all-physical extraction");
	}

	sliced_tests! {
		seed_equals_skip_path, SEED_SLICES;
		seed_equals_skip_path_00 => 0,
		seed_equals_skip_path_01 => 1,
		seed_equals_skip_path_02 => 2,
		seed_equals_skip_path_03 => 3,
		seed_equals_skip_path_04 => 4,
		seed_equals_skip_path_05 => 5,
		seed_equals_skip_path_06 => 6,
		seed_equals_skip_path_07 => 7,
		seed_equals_skip_path_08 => 8,
		seed_equals_skip_path_09 => 9,
		seed_equals_skip_path_10 => 10,
		seed_equals_skip_path_11 => 11,
	}

	/// `answers` with each value the engine generates for a run replaced, so two runs that
	/// generate them compare equal:
	/// - a record id key (twenty lowercase letters and digits) and a UUID (a request id) by one
	///   marker;
	/// - a wall-clock time an `EXPLAIN ANALYZE` reports (`elapsed: 1.2ms` in text, `elapsed_ns:
	///   1200000` in a structured plan) by one marker;
	/// - a change feed's versionstamp, which the process-wide oracle numbers across datastores, by
	///   its rank among the versionstamps of `answers`, so their order is kept.
	fn without_generated_ids(answers: &Result<Vec<Result<String, String>>, String>) -> String {
		use std::sync::LazyLock;

		static GENERATED: LazyLock<regex::Regex> = LazyLock::new(|| {
			regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*):[0-9a-z]{20}\b")
				.expect("the pattern compiles")
		});
		static UUID: LazyLock<regex::Regex> = LazyLock::new(|| {
			regex::Regex::new(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
				.expect("the pattern compiles")
		});
		static ELAPSED: LazyLock<regex::Regex> = LazyLock::new(|| {
			regex::Regex::new(r"elapsed: [0-9.]+(?:ns|µs|ms|s)\b").expect("the pattern compiles")
		});
		static ELAPSED_NS: LazyLock<regex::Regex> = LazyLock::new(|| {
			regex::Regex::new(r"\belapsed_ns: [0-9]+\b").expect("the pattern compiles")
		});
		static VERSIONSTAMP: LazyLock<regex::Regex> = LazyLock::new(|| {
			regex::Regex::new(r"\bversionstamp: ([0-9]+)\b").expect("the pattern compiles")
		});

		let answers = format!("{answers:?}");
		let answers = GENERATED.replace_all(&answers, "$1:<generated>");
		let answers = UUID.replace_all(&answers, "<uuid>");
		let answers = ELAPSED.replace_all(&answers, "elapsed: <elapsed>");
		let answers = ELAPSED_NS.replace_all(&answers, "elapsed_ns: <elapsed>");
		let stamps: std::collections::BTreeSet<u128> = VERSIONSTAMP
			.captures_iter(&answers)
			.filter_map(|stamp| stamp[1].parse().ok())
			.collect();
		let rank: BTreeMap<u128, usize> =
			stamps.into_iter().enumerate().map(|(rank, stamp)| (stamp, rank)).collect();
		VERSIONSTAMP
			.replace_all(&answers, |stamp: &regex::Captures<'_>| {
				let at = stamp[1].parse::<u128>().ok().and_then(|value| rank.get(&value));
				format!(
					"versionstamp: <{}>",
					at.map_or_else(|| stamp[1].to_owned(), usize::to_string)
				)
			})
			.into_owned()
	}

	/// The stack the language-test runner gives a worker in an unoptimised build.
	const RUNNER_STACK: usize = 20 << 20;

	/// Runs the future `body` makes to completion on a thread of [`RUNNER_STACK`] bytes, on a
	/// runtime of its own. The corpus runs the language tests' statements, which a debug build
	/// runs on that stack, so a statement that spends its computation-depth budget meets it before
	/// the stack ends. The e-graph's mode, rules, limits and counts are per thread, so `body` sets
	/// and reads them itself.
	fn on_the_runner_stack<F: Future<Output = ()>>(body: impl FnOnce() -> F + Send + 'static) {
		std::thread::Builder::new()
			.stack_size(RUNNER_STACK)
			.spawn(move || {
				tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("a runtime")
					.block_on(body())
			})
			.expect("a thread")
			.join()
			.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
	}

	/// With the registered rules, the logical-only run answers every statement of the slice `k` of
	/// the deterministic corpus as the forced-off run does. A value the engine generates for a run
	/// is not part of the answer ([`without_generated_ids`]).
	fn forced_on_answers_equal(k: usize) {
		let rules = registered();
		let items = slice(deterministic_corpus(), k);
		let forced_calls: usize = par_corpus_slice(
			&items,
			|| 0usize,
			|mut forced_calls, (path, text)| async move {
				let off = {
					let _forced = ForcedPlanning::new(EgraphMode::ForceOff, Some(rules));
					run(text).await
				};
				let before = counts();
				let on = {
					let _forced = ForcedPlanning::new(EgraphMode::ForceOnLogicalOnly, Some(rules));
					run(text).await
				};
				forced_calls += counts().planned - before.planned;
				assert_eq!(without_generated_ids(&on), without_generated_ids(&off), "{path}");
				forced_calls
			},
		)
		.into_iter()
		.sum();
		assert!(forced_calls > 0, "no request reached the e-graph planner");
	}

	sliced_tests! {
		forced_on_answers_equal, FORCED_SLICES;
		forced_on_answers_equal_00 => 0,
		forced_on_answers_equal_01 => 1,
		forced_on_answers_equal_02 => 2,
		forced_on_answers_equal_03 => 3,
		forced_on_answers_equal_04 => 4,
		forced_on_answers_equal_05 => 5,
		forced_on_answers_equal_06 => 6,
		forced_on_answers_equal_07 => 7,
		forced_on_answers_equal_08 => 8,
		forced_on_answers_equal_09 => 9,
		forced_on_answers_equal_10 => 10,
		forced_on_answers_equal_11 => 11,
	}

	/// With the registered rules, the logical-only run answers each statement they match as the
	/// forced-off run does, and it rewrites and adopts that statement.
	#[test]
	fn forced_on_rewrites_matching_statements() {
		on_the_runner_stack(forced_on_rewrites_matching_statements_body);
	}

	async fn forced_on_rewrites_matching_statements_body() {
		let rules = registered();
		for query in MATCHING {
			let off = {
				let _forced = ForcedPlanning::new(EgraphMode::ForceOff, Some(rules));
				run_over_prelude(query).await
			};
			let before = counts();
			let on = {
				let _forced = ForcedPlanning::new(EgraphMode::ForceOnLogicalOnly, Some(rules))
					.with_limits(UNHURRIED);
				run_over_prelude(query).await
			};
			assert_eq!(on, off, "{query}");
			assert!(
				counts().adopted > before.adopted,
				"the registered rules did not rewrite a statement they match: {query}"
			);
		}
	}
}

/// The key of the default effect context of `ir`, which the rewriter's facts are filed under.
fn default_key(ir: &Ir) -> crate::exec::ir::iface::catalog::CxKey {
	crate::exec::ir::analysis::cx::default_cx(ir).memo_key()
}
