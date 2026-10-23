//! Cross-analysis tests: each binds SurrealQL into a container and asks the analyses about the
//! ops it holds.

#![deny(clippy::wildcard_enum_match_arm)]

#[cfg(feature = "kv-mem")]
use surrealdb_strand::Strand;
use surrealdb_strand::TableName;

use super::certainty::ReadCertainty;
use super::cx::NoDefinitions;
#[cfg(feature = "kv-mem")]
use super::frames::RowView;
use super::row_dep::{DepCause, Invariance};
#[cfg(feature = "kv-mem")]
use super::slice::OnFailure;
use super::slice::SliceExclusion;
use super::{
	certainty, effects, fallibility, footprint, frames, identity, liveness, loop_legality,
	multiplicity, row_dep, slice, tokens, types, write_epoch,
};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::build::logical::bind_request;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope, Walk, WalkOrder};
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{ElemPath, FieldSet, KeyOf, Multiplicity};
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::frame::FrameTable;
use crate::exec::ir::iface::identity::AttrDigest;
use crate::exec::ir::iface::ids::{FrameId, OpId, SnapshotId, ValueId, WriteEpoch};
use crate::exec::ir::iface::region_kind::{FieldRole, RegionKind, RegionProperties};
use crate::exec::ir::iface::types::{FactSource, TokenClass, TypeKnowledge};
use crate::exec::ir::logical::ops::{
	FieldsShape, LogOp, LogTag, LoopKind, ProjectAttrs, WriteStageKind,
};
use crate::exec::ir::op_kind::{OpKind, OpTag};
#[cfg(feature = "kv-mem")]
use crate::exec::ir::tests_plan_request::Bound;
use crate::exec::ir::{RequestInput, RequestStatement};
#[cfg(feature = "kv-mem")]
use crate::exec::logical::pipelines::ErrorOrder;
use crate::exec::logical::pipelines::{
	Arrival, ChainLegality, ChainRefusal, SinkCombine, SourceSplit,
};
use crate::expr::logical::EffectSet;
use crate::expr::{Function, Kind};
use crate::val::Value;

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
			catalog: &NoDefinitions,
			frames: &self.frames,
		}
	}
}

fn request_scope() -> IrScope {
	IrScope::Request {
		fragment: None,
	}
}

/// The statements of `sql`, parsed with the planner's own parser.
fn statements(sql: &str) -> Vec<surrealdb_expr::parser::TopLevelExpr> {
	crate::syn::parse(sql).expect("the request parses").expressions
}

/// Binds `sql` into a container in the logical dialect.
async fn bind(sql: &str) -> Ir {
	let env = Env::new();
	let parsed = statements(sql);
	let request_statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&request_statements, request_scope());
	bind_request(&input, &env.cx()).await.expect("the request binds")
}

/// Binds `sql` into a container in the logical dialect over a database `setup` builds, as the
/// owner, each statement resolved against the catalog as a unit's statement is. The binder reads
/// the definitions `setup` made: a statement after a schema statement of its own request binds
/// none, so a case that reads a definition makes it here, and judges the container under the
/// catalog the binder read ([`Bound::cx`]).
#[cfg(feature = "kv-mem")]
async fn bind_over(setup: &str, sql: &str) -> Bound {
	crate::exec::ir::tests_plan_request::bound(setup, sql).await
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

/// The ops of `ir` whose tag is `tag`, in pre-order.
fn ops_tagged(ir: &Ir, tag: OpTag) -> Vec<OpId> {
	all_ops(ir).into_iter().filter(|op| kind_of(ir, *op).tag() == tag).collect()
}

/// Every op beneath `op`, in pre-order.
fn ops_below(ir: &Ir, op: OpId) -> Vec<OpId> {
	let mut out = Vec::new();
	let regions = ir.op(op).map(|data| data.regions.to_vec()).unwrap_or_default();
	for region in regions {
		ir.walk(region, WalkOrder::PreOrder, &mut |inner| {
			out.push(inner);
			Walk::Continue
		});
	}
	out
}

/// The first op of `ir` whose tag is `tag`.
fn first_tagged(ir: &Ir, tag: OpTag) -> OpId {
	*ops_tagged(ir, tag).first().unwrap_or_else(|| panic!("the container holds no {tag:?}"))
}

/// The ops of `ir` that run the closure handed to them: closure loops and method calls.
fn invokers(ir: &Ir) -> Vec<OpId> {
	all_ops(ir)
		.into_iter()
		.filter(|op| match kind_of(ir, *op) {
			OpKind::Logical(LogOp::Loop {
				kind,
				..
			}) => *kind == LoopKind::Closure,
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
				kind_of(ir, *op).tag() == OpTag::Expr(ExprTag::Method)
			}
		})
		.collect()
}

#[tokio::test]
async fn stored_clause_contributes_nothing() {
	let env = Env::new();
	let ir = bind("DEFINE FIELD f ON t VALUE rand()").await;
	let stored = all_ops(&ir).into_iter().any(|op| {
		let region = ir.enclosing_region(op);
		matches!(ir.region(region).map(|r| r.kind), Some(RegionKind::StoredClause(_)))
	});
	assert!(stored, "the field's value is a stored clause");
	let define = first_tagged(&ir, OpTag::Logical(LogTag::Define));
	let answer = effects::of_with(&ir, &mut FactCache::new(), &env.cx(), define);
	assert!(
		!answer.set.contains(EffectSet::ENTROPY),
		"a clause that never runs draws no entropy: {:?}",
		answer.set
	);
}

#[tokio::test]
async fn closure_body_counts_at_invocation() {
	let env = Env::new();
	let ir = bind("RETURN [1].map(|$v| rand())").await;
	let mut facts = FactCache::new();
	let invoking = invokers(&ir);
	assert!(!invoking.is_empty(), "the call that runs the closure is in the container");
	for op in invoking {
		let answer = effects::of_with(&ir, &mut facts, &env.cx(), op);
		assert!(answer.set.contains(EffectSet::ENTROPY), "the invocation draws entropy");
	}
	let creation = first_tagged(&ir, OpTag::Expr(ExprTag::ClosureCreate));
	let answer = effects::of_with(&ir, &mut facts, &env.cx(), creation);
	assert!(
		!answer.set.contains(EffectSet::ENTROPY),
		"creating a closure runs none of its body: {:?}",
		answer.set
	);

	let ir = bind("LET $f = |$v| rand(); RETURN 1").await;
	let mut facts = FactCache::new();
	for tag in [OpTag::Logical(LogTag::Let), OpTag::Expr(ExprTag::ClosureCreate)] {
		let op = first_tagged(&ir, tag);
		let answer = effects::of_with(&ir, &mut facts, &env.cx(), op);
		assert!(
			!answer.set.contains(EffectSet::ENTROPY),
			"a closure bound and never invoked draws no entropy at {tag:?}"
		);
	}
}

#[tokio::test]
async fn fallibility_of_a_raising_closure() {
	let env = Env::new();
	let ir = bind("RETURN [3, 'a', 2].map(|$v| $v + 1)").await;
	let stmt = first_tagged(&ir, OpTag::Logical(LogTag::Stmt));
	let answer = fallibility::of_with(&ir, &mut FactCache::new(), &env.cx(), stmt);
	assert!(answer.contains(Fallibility::VALUE), "the closure body raises on 'a': {answer:?}");
	let frozen = fallibility::of(&ir, &mut FactCache::new(), stmt);
	assert_eq!(answer, frozen, "the frozen entry point answers under the default context");
}

#[tokio::test]
async fn reads_separated_by_writes() {
	let env = Env::new();
	let ir = bind(
		"CREATE t:1 SET n = 1; \
		 CREATE person:1, person:2; \
		 RETURN [t:1.n, (UPDATE person SET a = t:1.n, b = (UPDATE t:1 SET n += 1 RETURN NONE) \
		 RETURN VALUE a)];",
	)
	.await;
	let mut facts = FactCache::new();
	let statements = ops_tagged(&ir, OpTag::Logical(LogTag::Stmt));
	let last = *statements.last().expect("the request holds statements");
	let reads: Vec<OpId> = ops_below(&ir, last)
		.into_iter()
		.filter(|op| {
			let tag = kind_of(&ir, *op).tag();
			let record_read =
				tag == OpTag::Expr(ExprTag::Access) || tag == OpTag::Expr(ExprTag::Deref);
			record_read
				&& ir.op(*op).is_some_and(|data| data.regions.is_empty())
				&& effects::local(&ir, &mut facts, &env.cx(), *op)
					.set
					.contains(EffectSet::READS_DATA)
		})
		.collect();
	assert!(reads.len() >= 2, "the outer and the per-row read of t:1.n are in the statement");
	for read in reads {
		assert_eq!(
			write_epoch::assign_with(&ir, &mut facts, &env.cx(), read),
			WriteEpoch::Unique(read),
			"a write of the statement reaches the read of t:1.n"
		);
	}
	let writes = write_epoch::statement_writes(&ir, &mut facts, &env.cx(), last);
	assert!(!writes.is_empty(), "the statement writes");
}

/// The record reads of the last statement of `ir`, which read `t:1.n`.
fn record_reads_of_last_statement(ir: &Ir, facts: &mut FactCache, env: &Env) -> (OpId, Vec<OpId>) {
	let statements = ops_tagged(ir, OpTag::Logical(LogTag::Stmt));
	let last = *statements.last().expect("the request holds statements");
	let reads: Vec<OpId> = ops_below(ir, last)
		.into_iter()
		.filter(|op| {
			let tag = kind_of(ir, *op).tag();
			let record_read =
				tag == OpTag::Expr(ExprTag::Access) || tag == OpTag::Expr(ExprTag::Deref);
			record_read
				&& ir.op(*op).is_some_and(|data| data.regions.is_empty())
				&& effects::local(ir, facts, &env.cx(), *op).set.contains(EffectSet::READS_DATA)
		})
		.collect();
	(last, reads)
}

/// A statement whose ops only read, draw entropy or read the clock writes nothing, so its reads
/// of stored data share one epoch; a statement holding a body the plan does not show writes
/// anything, so each of its reads has an epoch of its own.
#[tokio::test]
async fn reads_share_an_epoch_unless_the_statement_may_write() {
	let env = Env::new();
	for sql in [
		"RETURN [t:1.n, t:1.n]",
		"RETURN [t:1.n, rand::float(), time::now(), t:1.n]",
		"RETURN [t:1.n, (SELECT VALUE n FROM t WHERE n > 0), t:1.n]",
	] {
		let ir = bind(sql).await;
		let mut facts = FactCache::new();
		let (last, reads) = record_reads_of_last_statement(&ir, &mut facts, &env);
		assert!(!reads.is_empty(), "`{sql}` holds a read of t:1.n");
		let writes = write_epoch::statement_writes(&ir, &mut facts, &env.cx(), last);
		assert!(writes.is_empty(), "`{sql}` writes {writes:?}");
		for read in reads {
			assert_eq!(
				write_epoch::assign_with(&ir, &mut facts, &env.cx(), read),
				WriteEpoch::Stable(0),
				"`{sql}`: no write reaches the read"
			);
		}
	}
	for sql in [
		"RETURN [t:1.n, fn::unseen(), t:1.n]",
		"RETURN [t:1.n, (UPDATE t:1 SET n += 1 RETURN NONE)]",
	] {
		let ir = bind(sql).await;
		let mut facts = FactCache::new();
		let (last, reads) = record_reads_of_last_statement(&ir, &mut facts, &env);
		assert!(!reads.is_empty(), "`{sql}` holds a read of t:1.n");
		let writes = write_epoch::statement_writes(&ir, &mut facts, &env.cx(), last);
		assert!(!writes.is_empty(), "`{sql}` may write");
		for read in reads {
			assert_eq!(
				write_epoch::assign_with(&ir, &mut facts, &env.cx(), read),
				WriteEpoch::Unique(read),
				"`{sql}`: a write of the statement reaches the read"
			);
		}
	}
}

#[tokio::test]
async fn nondeterministic_calls_are_distinct() {
	let env = Env::new();
	let ir = bind("RETURN [sequence::nextval('s'), sequence::nextval('s')]").await;
	let mut facts = FactCache::new();
	let draws: Vec<OpId> = ops_tagged(&ir, OpTag::Expr(ExprTag::Call))
		.into_iter()
		.filter(|op| {
			effects::of_with(&ir, &mut facts, &env.cx(), *op).determinism
				== crate::exec::ir::iface::effects::Determinism::NonDeterministic
		})
		.collect();
	assert_eq!(draws.len(), 2, "both calls draw from the sequence");
	let first = identity::key_with(&ir, &mut facts, &env.cx(), draws[0]);
	let second = identity::key_with(&ir, &mut facts, &env.cx(), draws[1]);
	assert_ne!(first, second, "two draws are two values");
	assert_eq!(first.opaque, Some(draws[0]));
	assert_eq!(second.opaque, Some(draws[1]));
	for op in all_ops(&ir) {
		if kind_of(&ir, op).attr_digest() == AttrDigest::OPAQUE {
			let key = identity::key_with(&ir, &mut facts, &env.cx(), op);
			assert_eq!(key.opaque, Some(op), "an undigested op is equal to no other");
		}
	}
	assert_eq!(
		identity::key(&ir, &mut facts, draws[0]),
		first,
		"the frozen entry point answers under the default context"
	);
}

/// The calls of the builtin `name` in `ir`, in pre-order.
fn calls_named(ir: &Ir, name: &str) -> Vec<OpId> {
	ops_tagged(ir, OpTag::Expr(ExprTag::Call))
		.into_iter()
		.filter(|op| {
			matches!(kind_of(ir, *op), OpKind::Expr(ExprOp::Call(call))
				if call.function == Function::Normal(name.to_owned()))
		})
		.collect()
}

/// `time::now()` reads the statement's instant when the statement runs: the binder keeps each
/// read a call, so the capability check runs, and the two calls are one value of the statement.
#[tokio::test]
async fn clock_reads_are_one_value() {
	let env = Env::new();
	let ir = bind("RETURN [time::now(), time::now()]").await;
	let mut facts = FactCache::new();
	assert!(ops_tagged(&ir, OpTag::Expr(ExprTag::Literal)).is_empty(), "no read is a literal");
	let clocks = calls_named(&ir, "time::now");
	assert_eq!(clocks.len(), 2, "both reads of the clock are in the statement");
	for clock in &clocks {
		let effects = effects::of_with(&ir, &mut facts, &env.cx(), *clock);
		assert!(effects.set.contains(EffectSet::CLOCK), "the call reads the clock");
	}
	let first = identity::key_with(&ir, &mut facts, &env.cx(), clocks[0]);
	let second = identity::key_with(&ir, &mut facts, &env.cx(), clocks[1]);
	assert_eq!(first, second, "the clock is one value within a statement");
	assert_eq!(first.opaque, None);
	assert_eq!(first.epoch, None);
}

/// The first loop of `ir` whose kind is `kind`.
fn loop_of(ir: &Ir, kind: LoopKind) -> OpId {
	all_ops(ir)
		.into_iter()
		.find(|op| multiplicity::loop_kind(ir, *op) == Some(kind))
		.unwrap_or_else(|| panic!("the container holds no {kind:?} loop"))
}

/// The ops of `ir` that read the field `name` off a row or a value: `expr.field` and
/// `expr.access`.
fn field_reads(ir: &Ir, name: &str) -> Vec<OpId> {
	all_ops(ir)
		.into_iter()
		.filter(|op| {
			if let OpKind::Expr(ExprOp::Field {
				name: read,
			}) = kind_of(ir, *op)
			{
				*read == name
			} else if let OpKind::Expr(ExprOp::Access {
				field,
			}) = kind_of(ir, *op)
			{
				*field == name
			} else {
				false
			}
		})
		.collect()
}

/// What analysis knows of the first result of `op`.
fn knowledge_of(ir: &Ir, op: OpId) -> TypeKnowledge {
	let value =
		ir.op(op).and_then(|data| data.results.first().copied()).expect("the op has a result");
	types::classify(ir, &mut FactCache::new(), value)
}

/// A `Fact` of `kind` that analysis derived.
fn derived(kind: Kind) -> TypeKnowledge {
	TypeKnowledge::Fact {
		kind,
		source: FactSource::Derived,
	}
}

/// The kind a `Fact` holds, or `None` for any other knowledge.
fn fact_kind(knowledge: &TypeKnowledge) -> Option<&Kind> {
	if let TypeKnowledge::Fact {
		kind,
		..
	} = knowledge
	{
		Some(kind)
	} else {
		None
	}
}

#[tokio::test]
async fn types_classify_sources() {
	// A literal holds the knowledge its own value carries.
	let ir = bind("RETURN 1").await;
	let literal = first_tagged(&ir, OpTag::Expr(ExprTag::Literal));
	assert_eq!(fact_kind(&knowledge_of(&ir, literal)), Some(&Kind::Int));

	// A conversion guarantees its kind.
	let ir = bind("RETURN <int> $p").await;
	let convert = first_tagged(&ir, OpTag::Expr(ExprTag::Convert));
	assert_eq!(knowledge_of(&ir, convert), derived(Kind::Int));

	// Comparisons, containment, truthiness and negation are booleans.
	for sql in ["RETURN $a = $b", "RETURN $a < 3", "RETURN $a != $b", "RETURN $a IN $b"] {
		let ir = bind(sql).await;
		let compare = *ops_tagged(&ir, OpTag::Expr(ExprTag::Binary))
			.last()
			.unwrap_or_else(|| panic!("`{sql}` binds a binary op"));
		assert_eq!(knowledge_of(&ir, compare), derived(Kind::Bool), "`{sql}`");
	}
	let ir = bind("RETURN !$a").await;
	let negate = first_tagged(&ir, OpTag::Expr(ExprTag::Prefix));
	assert_eq!(knowledge_of(&ir, negate), derived(Kind::Bool));
	let ir = bind("IF $a { RETURN 1 }").await;
	for op in ops_tagged(&ir, OpTag::Expr(ExprTag::Truthy)) {
		assert_eq!(knowledge_of(&ir, op), derived(Kind::Bool));
	}

	// Integer arithmetic closes over the integers; division widens to a number.
	let arithmetic: &[(&str, TypeKnowledge)] = &[
		("RETURN <int> $a + <int> $b", derived(Kind::Int)),
		("RETURN <int> $a - <int> $b", derived(Kind::Int)),
		("RETURN <int> $a * <int> $b", derived(Kind::Int)),
		("RETURN <int> $a / <int> $b", derived(Kind::Number)),
		("RETURN <int> $a + $b", TypeKnowledge::Unknown),
		("RETURN <int> $a % <int> $b", TypeKnowledge::Unknown),
	];
	for (sql, expected) in arithmetic {
		let ir = bind(sql).await;
		let op = *ops_tagged(&ir, OpTag::Expr(ExprTag::Binary))
			.last()
			.unwrap_or_else(|| panic!("`{sql}` binds a binary op"));
		assert_eq!(&knowledge_of(&ir, op), expected, "`{sql}`");
	}

	// The `id` of a row of a table scan is a record id of the table.
	let ir = bind("SELECT * FROM t WHERE id != NONE").await;
	let reads = field_reads(&ir, "id");
	assert!(!reads.is_empty(), "the filter reads `id`");
	for read in reads {
		assert_eq!(knowledge_of(&ir, read), derived(Kind::Record(vec![TableName::new("t")])));
	}

	// The `id` of a loop element is a record id by provenance when the source is a distinct scan of
	// one table that keeps its `id`.
	let pinned = TypeKnowledge::Fact {
		kind: Kind::Record(vec![TableName::new("user")]),
		source: FactSource::Provenance {
			table: TableName::new("user"),
		},
	};
	let ir = bind("FOR $u IN (SELECT * FROM user) { RETURN $u.id }").await;
	let read = *field_reads(&ir, "id").last().expect("the body reads `id`");
	assert_eq!(knowledge_of(&ir, read), pinned);
	let ir = bind("FOR $u IN (SELECT id, a FROM user) { RETURN $u.id }").await;
	let read = *field_reads(&ir, "id").last().expect("the body reads `id`");
	assert_eq!(knowledge_of(&ir, read), pinned, "a projection that names `id` keeps it");
	let unpinned = [
		"FOR $u IN (SELECT * OMIT id FROM user) { RETURN $u.id }",
		"FOR $u IN (SELECT a, count() AS c FROM user GROUP BY a) { RETURN $u.id }",
		"FOR $u IN (SELECT VALUE x FROM user) { RETURN $u.id }",
		"FOR $u IN (SELECT 1 AS id FROM user) { RETURN $u.id }",
		"FOR $u IN (SELECT VALUE author.id FROM user) { RETURN $u.id }",
		"FOR $u IN (SELECT *, author.id AS id FROM user) { RETURN $u.id }",
		"FOR $u IN (SELECT author.id FROM user) { RETURN $u.id }",
		"LET $x = (SELECT * FROM user); FOR $u IN $x { RETURN $u.id }",
		"FOR $u IN $users { RETURN $u.id }",
	];
	for sql in unpinned {
		let ir = bind(sql).await;
		let read = *field_reads(&ir, "id").last().unwrap_or_else(|| panic!("`{sql}` reads `id`"));
		assert_eq!(knowledge_of(&ir, read), TypeKnowledge::Unknown, "`{sql}`");
	}

	// A compound literal has no kind, so the element of a loop over one is unknown.
	let ir = bind("FOR $i IN [1, 2] { RETURN $i }").await;
	let each = loop_of(&ir, LoopKind::For);
	let element = multiplicity::loop_element(&ir, each).expect("the loop has an element");
	assert_eq!(types::classify(&ir, &mut FactCache::new(), element), TypeKnowledge::Unknown);

	// The element of a loop over an array of known element kind has that kind.
	let ir = bind("FOR $i IN <array<int>> $p { RETURN $i }").await;
	let each = loop_of(&ir, LoopKind::For);
	let element = multiplicity::loop_element(&ir, each).expect("the loop has an element");
	assert_eq!(fact_kind(&types::classify(&ir, &mut FactCache::new(), element)), Some(&Kind::Int));
	let position = ir
		.block(
			*ir.region(multiplicity::loop_body(&ir, each).expect("the loop has a body"))
				.expect("the body exists")
				.blocks
				.first()
				.expect("the body has a block"),
		)
		.and_then(|block| block.args.get(1).copied());
	if let Some(position) = position {
		assert_eq!(
			types::classify(&ir, &mut FactCache::new(), position),
			TypeKnowledge::Unknown,
			"the position is not classified"
		);
	}

	// Nothing declared in the catalog is known: a field of a strict table, parameters and calls.
	let ir = bind("DEFINE TABLE t SCHEMAFULL; DEFINE FIELD a ON t TYPE int; SELECT a FROM t").await;
	for read in field_reads(&ir, "a") {
		assert_eq!(knowledge_of(&ir, read), TypeKnowledge::Unknown, "a declared field");
	}
	let ir = bind("RETURN $p").await;
	let param = first_tagged(&ir, OpTag::Expr(ExprTag::FreeParam));
	assert_eq!(knowledge_of(&ir, param), TypeKnowledge::Unknown);
	let ir = bind("RETURN math::abs($p)").await;
	let call = first_tagged(&ir, OpTag::Expr(ExprTag::Call));
	assert_eq!(knowledge_of(&ir, call), TypeKnowledge::Unknown);
}

#[tokio::test]
async fn write_target_pins_by_provenance() {
	let env = Env::new();
	let ir = bind("FOR $u IN (SELECT * FROM user) { UPDATE $u SET seen = true }").await;
	let mut facts = FactCache::new();
	let write = first_tagged(&ir, OpTag::Logical(LogTag::Write));
	assert_eq!(
		types::write_target_table(&ir, &mut facts, &env.cx(), write),
		Some(TableName::new("user")),
		"the subject is the element of a distinct scan of `user`"
	);
	assert_eq!(
		footprint::key_of(&ir, write),
		KeyOf::Elem(ElemPath::Id),
		"a pinned subject is keyed along the stream's key path"
	);

	let unpinned = [
		"LET $us = (SELECT * FROM user); FOR $u IN $us { UPDATE $u SET seen = true }",
		"FOR $u IN $users { UPDATE $u SET seen = true }",
		"FOR $u IN (SELECT 1 AS id FROM user) { UPDATE $u SET seen = true }",
	];
	for sql in unpinned {
		let ir = bind(sql).await;
		let write = first_tagged(&ir, OpTag::Logical(LogTag::Write));
		assert_eq!(types::write_target_table(&ir, &mut facts, &env.cx(), write), None, "`{sql}`");
		assert_eq!(footprint::key_of(&ir, write), KeyOf::Any, "`{sql}`");
	}
}

/// The ids a write's one range mock names are each named once, along the element itself; a count of
/// ids drawn at random, two mocks and a mock beside another target may name one twice.
#[tokio::test]
async fn the_ids_of_a_range_mock_are_distinct() {
	for (sql, expected) in [
		("CREATE |t:1..4| SET n = 1", (Multiplicity::Distinct, ElemPath::Elem)),
		("CREATE |t:1..=4| SET n = 1", (Multiplicity::Distinct, ElemPath::Elem)),
		("CREATE |t:3| SET n = 1", (Multiplicity::MayRepeat, ElemPath::Elem)),
		("CREATE |t:1..4|, |t:2..6| SET n = 1", (Multiplicity::MayRepeat, ElemPath::Elem)),
		("CREATE |t:1..4|, t:9 SET n = 1", (Multiplicity::MayRepeat, ElemPath::Elem)),
		("CREATE t:1, t:1 SET n = 1", (Multiplicity::MayRepeat, ElemPath::Elem)),
	] {
		let ir = bind(sql).await;
		let write = loop_of(&ir, LoopKind::Write);
		assert_eq!(multiplicity::of_stream(&ir, write), expected, "`{sql}`");
	}
}

#[tokio::test]
async fn multiplicity_facts_mirror() {
	let select_cases: &[(&str, Multiplicity, ElemPath)] = &[
		("SELECT * FROM t", Multiplicity::Distinct, ElemPath::Id),
		("SELECT * FROM t:1..3", Multiplicity::Distinct, ElemPath::Id),
		(
			"DEFINE INDEX ix ON t FIELDS a; SELECT * FROM t WHERE a = 1",
			Multiplicity::Distinct,
			ElemPath::Id,
		),
		// A set literal collapses to one literal under its conversion, which is not read as
		// distinct.
		("SELECT * FROM <set> [1, 2]", Multiplicity::MayRepeat, ElemPath::Elem),
		("SELECT 1 AS id FROM w", Multiplicity::MayRepeat, ElemPath::Elem),
		("SELECT * FROM t SPLIT a", Multiplicity::MayRepeat, ElemPath::Elem),
		("SELECT * FROM t, t", Multiplicity::MayRepeat, ElemPath::Elem),
		("SELECT id, a FROM t", Multiplicity::Distinct, ElemPath::Id),
		("SELECT VALUE id FROM t", Multiplicity::Distinct, ElemPath::Elem),
		("SELECT * OMIT id FROM t", Multiplicity::MayRepeat, ElemPath::Elem),
		("SELECT a FROM t", Multiplicity::MayRepeat, ElemPath::Elem),
		("SELECT VALUE author.id FROM post", Multiplicity::MayRepeat, ElemPath::Elem),
		("SELECT *, author.id AS id FROM post", Multiplicity::MayRepeat, ElemPath::Elem),
		("SELECT author.id FROM post", Multiplicity::MayRepeat, ElemPath::Elem),
	];
	for (sql, multiplicity_expected, path_expected) in select_cases {
		let ir = bind(sql).await;
		let select = loop_of(&ir, LoopKind::Select);
		assert_eq!(multiplicity::of(&ir, select), *multiplicity_expected, "`{sql}`");
		assert_eq!(multiplicity::key_path(&ir, select), *path_expected, "`{sql}`");
		assert_eq!(
			multiplicity::of_stream(&ir, select),
			(*multiplicity_expected, *path_expected),
			"`{sql}`"
		);
	}

	let for_cases: &[(&str, Multiplicity, ElemPath)] = &[
		("FOR $i IN (SELECT * FROM user) { RETURN $i }", Multiplicity::Distinct, ElemPath::Id),
		(
			"FOR $i IN (SELECT * FROM t ORDER BY a LIMIT 3) { RETURN $i }",
			Multiplicity::Distinct,
			ElemPath::Id,
		),
		(
			"FOR $i IN (SELECT VALUE id FROM t) { RETURN $i }",
			Multiplicity::Distinct,
			ElemPath::Elem,
		),
		("FOR $i IN 1..3 { RETURN $i }", Multiplicity::Distinct, ElemPath::Elem),
		("FOR $i IN [1, 2] { RETURN $i }", Multiplicity::MayRepeat, ElemPath::Elem),
		("FOR $i IN $arr { RETURN $i }", Multiplicity::MayRepeat, ElemPath::Elem),
		(
			"LET $x = (SELECT * FROM t); FOR $i IN $x { RETURN $i }",
			Multiplicity::MayRepeat,
			ElemPath::Elem,
		),
		(
			"FOR $i IN array::distinct([1, 1, 2]) { RETURN $i }",
			Multiplicity::Distinct,
			ElemPath::Elem,
		),
		("FOR $i IN (SELECT id, a FROM t) { RETURN $i }", Multiplicity::Distinct, ElemPath::Id),
		(
			"FOR $i IN (SELECT 1 AS id FROM w) { RETURN $i }",
			Multiplicity::MayRepeat,
			ElemPath::Elem,
		),
		("FOR $i IN p:1->knows->person { RETURN $i }", Multiplicity::MayRepeat, ElemPath::Elem),
	];
	for (sql, multiplicity_expected, path_expected) in for_cases {
		let ir = bind(sql).await;
		let each = loop_of(&ir, LoopKind::For);
		assert_eq!(
			multiplicity::of_stream(&ir, each),
			(*multiplicity_expected, *path_expected),
			"`{sql}`"
		);
	}

	let ir = bind("FOR $u IN (SELECT * FROM user) { RETURN $u }").await;
	let each = loop_of(&ir, LoopKind::For);
	let facts = loop_legality::of(&ir, &mut FactCache::new(), each).expect("the loop is judged");
	assert_eq!(facts.multiplicity, Multiplicity::Distinct);
	assert_eq!(facts.key_path, ElemPath::Id);
}

/// The first result of `op`.
fn result_of(ir: &Ir, op: OpId) -> ValueId {
	ir.op(op).and_then(|data| data.results.first().copied()).expect("the op has a result")
}

/// The ops beneath `scope` that read stored data by their own effects and write none.
fn data_reads_below(ir: &Ir, env: &Env, scope: OpId) -> Vec<OpId> {
	let mut facts = FactCache::new();
	ops_below(ir, scope)
		.into_iter()
		.filter(|op| {
			let set = effects::local(ir, &mut facts, &env.cx(), *op).set;
			set.contains(EffectSet::READS_DATA) && !set.writes_data()
		})
		.collect()
}

/// The class of the token that makes an answer dependent, when it is one.
fn carried(ir: &Ir, answer: &Invariance) -> Option<TokenClass> {
	match answer {
		Invariance::Dependent(DepCause::BlockArg(arg)) => ir.token_class(*arg),
		Invariance::Dependent(_) | Invariance::Invariant => None,
	}
}

/// The reads of a table beneath `scope`: its `logical.source` ops.
fn sources_below(ir: &Ir, scope: OpId) -> Vec<OpId> {
	ops_below(ir, scope)
		.into_iter()
		.filter(|op| kind_of(ir, *op).tag() == OpTag::Logical(LogTag::Source))
		.collect()
}

/// A read of the table a write's statement writes, in a subquery of the write, depends on the
/// store token the loop of the write's subjects carries.
#[tokio::test]
async fn a_read_in_a_write_depends_on_the_store_the_write_carries() {
	for sql in [
		"UPDATE t SET n = (SELECT VALUE count() FROM t GROUP ALL)[0]",
		"DELETE t WHERE n > (SELECT VALUE count() FROM t GROUP ALL)[0]",
	] {
		let ir = bind(sql).await;
		let scope = loop_of(&ir, LoopKind::Write);
		let reads = sources_below(&ir, scope);
		assert!(!reads.is_empty(), "{sql}: the loop holds a read of `t`");
		for read in reads {
			let answer = row_dep::invariant_in(&ir, result_of(&ir, read), scope);
			assert_eq!(carried(&ir, &answer), Some(TokenClass::Store), "{sql}: {answer:?}");
		}
	}
}

/// A write in the body of a `FOR` makes the loop carry the store, and a read of the written table
/// in the body is not invariant.
#[tokio::test]
async fn a_write_in_a_for_body_is_carried_by_the_loop() {
	let ir = bind(
		"FOR $i IN [1, 2] { CREATE t SET n = $i; RETURN (SELECT VALUE count() FROM t GROUP ALL) }",
	)
	.await;
	let scope = loop_of(&ir, LoopKind::For);
	let body = multiplicity::loop_body(&ir, scope).expect("the loop has a body");
	let block = ir.region(body).and_then(|info| info.blocks.first().copied()).expect("a block");
	let carries: Vec<_> =
		ir.token_args(block).iter().filter_map(|arg| ir.token_class(*arg)).collect();
	assert!(carries.contains(&TokenClass::Store), "the body carries {carries:?}");
	let reads = sources_below(&ir, scope);
	assert!(!reads.is_empty(), "the loop holds a read of `t`");
	for read in reads {
		let answer = row_dep::invariant_in(&ir, result_of(&ir, read), scope);
		assert!(matches!(answer, Invariance::Dependent(_)), "{answer:?}");
	}
}

/// An op a pass creates in a threaded container holds no token until the container is threaded
/// again, and no answer holds it invariant before then.
#[tokio::test]
async fn an_op_created_after_the_threading_is_not_invariant_until_it_is_threaded() {
	let env = Env::new();
	let mut ir = bind("SELECT *, string::uppercase($auth) FROM user").await;
	let select = loop_of(&ir, LoopKind::Select);
	let call = first_tagged(&ir, OpTag::Expr(ExprTag::Call));
	let mut facts = FactCache::new();
	let (frame, snapshot) = {
		let data = ir.op(call).expect("the call exists");
		(data.frame, data.snapshot)
	};
	let created = Rewriter::new(&mut ir, &mut facts)
		.create(
			InsertPoint::After(call),
			OpSpec {
				kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1)))),
				operands: Default::default(),
				results: vec![TypeKnowledge::Unknown].into(),
				frame,
				snapshot,
				loc: Location {
					stmt: 0,
					span: None,
					origin: LocOrigin::Synthesized,
				},
			},
		)
		.expect("the literal is created");
	assert_eq!(
		row_dep::invariant_in(&ir, result_of(&ir, created), select),
		Invariance::Dependent(DepCause::Opaque(created)),
		"the literal holds no token"
	);
	assert_eq!(
		row_dep::invariant_in(&ir, result_of(&ir, call), select),
		Invariance::Invariant,
		"an op that is threaded is judged as before"
	);
	tokens::rethread(&mut ir, &mut facts, &env.cx()).expect("the container is threaded again");
	assert_eq!(
		row_dep::invariant_in(&ir, result_of(&ir, created), select),
		Invariance::Invariant,
		"a literal depends on nothing the loop defines"
	);
}

#[tokio::test]
async fn row_dep_cases() {
	let env = Env::new();

	// A call of a pure builtin over a value that is fixed for the statement is invariant in the
	// select loop.
	let ir = bind("SELECT *, string::uppercase($auth) FROM user").await;
	let select = loop_of(&ir, LoopKind::Select);
	let call = first_tagged(&ir, OpTag::Expr(ExprTag::Call));
	assert_eq!(
		row_dep::invariant_in(&ir, result_of(&ir, call), select),
		Invariance::Invariant,
		"`$auth` is the same for every row"
	);

	// A call of a function the container holds no body for may do anything, so it is not
	// invariant even over a fixed value.
	let ir = bind("SELECT *, fn::f($auth) FROM user").await;
	let select = loop_of(&ir, LoopKind::Select);
	let call = first_tagged(&ir, OpTag::Expr(ExprTag::Call));
	let answer = row_dep::invariant_in(&ir, result_of(&ir, call), select);
	assert!(matches!(answer, Invariance::Dependent(_)), "{answer:?}");

	// A call that takes the row varies with it.
	let ir = bind("SELECT *, fn::f($this) FROM user").await;
	let select = loop_of(&ir, LoopKind::Select);
	let call = first_tagged(&ir, OpTag::Expr(ExprTag::Call));
	let answer = row_dep::invariant_in(&ir, result_of(&ir, call), select);
	assert!(matches!(answer, Invariance::Dependent(DepCause::BlockArg(_))), "{answer:?}");

	// A projection function and an index function read the row the loop visits, though neither
	// takes it as an argument, so neither is one value for every row.
	for sql in [
		"SELECT *, type::field('a') FROM user",
		"SELECT *, type::fields(['a']) FROM user",
		"SELECT *, search::score(1) FROM user WHERE a @1@ 'x'",
		"SELECT *, search::highlight('<b>', '</b>', 1) FROM user WHERE a @1@ 'x'",
		"SELECT *, search::offsets(1) FROM user WHERE a @1@ 'x'",
	] {
		let ir = bind(sql).await;
		let select = loop_of(&ir, LoopKind::Select);
		let call = first_tagged(&ir, OpTag::Expr(ExprTag::Call));
		let answer = row_dep::invariant_in(&ir, result_of(&ir, call), select);
		assert!(
			matches!(answer, Invariance::Dependent(DepCause::BlockArg(_))),
			"{sql}: {answer:?}"
		);
	}

	// A draw of entropy is never one value for every row: it takes the random token the loop
	// carries.
	let ir = bind("SELECT *, rand() FROM user").await;
	let select = loop_of(&ir, LoopKind::Select);
	let call = first_tagged(&ir, OpTag::Expr(ExprTag::Call));
	let answer = row_dep::invariant_in(&ir, result_of(&ir, call), select);
	assert_eq!(carried(&ir, &answer), Some(TokenClass::Rng), "{answer:?}");

	// A read of stored data in a loop whose body writes is not one value: it takes the store
	// token the loop carries.
	let ir = bind(
		"CREATE t:1 SET n = 1; \
		 SELECT VALUE [t:1.n, (UPDATE t:1 SET n += 1 RETURN NONE)] FROM [1, 2];",
	)
	.await;
	let select = loop_of(&ir, LoopKind::Select);
	let reads = data_reads_below(&ir, &env, select);
	assert!(!reads.is_empty(), "the projection reads `t:1.n`");
	let answer = row_dep::invariant_in(&ir, result_of(&ir, reads[0]), select);
	assert_eq!(carried(&ir, &answer), Some(TokenClass::Store), "{answer:?}");

	// A read of stored data in a loop whose body writes nothing is one value, whatever an earlier
	// statement of the request wrote.
	let ir = bind("CREATE t:1 SET n = 1; SELECT VALUE (SELECT VALUE n FROM t) FROM [1, 2];").await;
	let select = loop_of(&ir, LoopKind::Select);
	let reads = data_reads_below(&ir, &env, select);
	assert!(!reads.is_empty(), "the projection reads `t`");
	assert_eq!(
		row_dep::invariant_in(&ir, result_of(&ir, reads[0]), select),
		Invariance::Invariant,
		"the read takes the token the statement starts from"
	);

	// A catalog read in a loop whose body changes the catalog is never one value.
	let ir = bind(
		"DEFINE PARAM $q VALUE 1; \
		 SELECT VALUE [$q + 1, (DEFINE PARAM OVERWRITE $q VALUE $q + 5), $q + 1] FROM [1, 2];",
	)
	.await;
	let select = loop_of(&ir, LoopKind::Select);
	let reads: Vec<OpId> = ops_below(&ir, select)
		.into_iter()
		.filter(|op| {
			let tag = kind_of(&ir, *op).tag();
			tag == OpTag::Expr(ExprTag::CatalogParam) || tag == OpTag::Expr(ExprTag::FreeParam)
		})
		.collect();
	assert!(!reads.is_empty(), "the projection reads `$q`");
	let answer = row_dep::invariant_in(&ir, result_of(&ir, reads[0]), select);
	assert_eq!(carried(&ir, &answer), Some(TokenClass::Catalog), "{answer:?}");

	// A catalog change before the loop is outside it: the read after it takes the token the
	// change produced, which the loop does not define.
	let ir = bind("{ DEFINE PARAM $q VALUE 1; RETURN (SELECT VALUE $q + 1 FROM [1, 2]); }").await;
	let select = loop_of(&ir, LoopKind::Select);
	let reads: Vec<OpId> = ops_below(&ir, select)
		.into_iter()
		.filter(|op| {
			let tag = kind_of(&ir, *op).tag();
			tag == OpTag::Expr(ExprTag::CatalogParam) || tag == OpTag::Expr(ExprTag::FreeParam)
		})
		.collect();
	assert!(!reads.is_empty(), "the projection reads `$q`");
	assert_eq!(
		row_dep::invariant_in(&ir, result_of(&ir, reads[0]), select),
		Invariance::Invariant,
		"the change precedes the loop"
	);

	// A value is invariant in every loop it does not depend on, and the outermost is reported.
	let ir = bind("FOR $i IN [1, 2] { FOR $j IN [3, 4] { RETURN <int>$p + 1 } }").await;
	let loops = ops_tagged(&ir, OpTag::Logical(LogTag::Loop));
	assert_eq!(loops.len(), 2, "the two FOR loops are in the container");
	let sum = *ops_tagged(&ir, OpTag::Expr(ExprTag::Binary))
		.last()
		.expect("the body adds one to the cast");
	assert_eq!(
		row_dep::outermost_scope(&ir, result_of(&ir, sum)),
		Some(loops[0]),
		"the sum depends on neither element"
	);
	let ir = bind("FOR $i IN [1, 2] { FOR $j IN [3, 4] { RETURN $j + 1 } }").await;
	let sum = *ops_tagged(&ir, OpTag::Expr(ExprTag::Binary))
		.last()
		.expect("the body adds one to the element");
	assert_eq!(
		row_dep::outermost_scope(&ir, result_of(&ir, sum)),
		None,
		"the sum depends on the inner element"
	);
}

#[tokio::test]
async fn reads_that_take_different_tokens_are_two_values() {
	let env = Env::new();
	let ir = bind(
		"{ LET $a = (SELECT * FROM t); LET $b = (SELECT * FROM t); \
		   CREATE t:1 SET n = 1; LET $c = (SELECT * FROM t); }",
	)
	.await;
	let mut facts = FactCache::new();
	let sources = ops_tagged(&ir, OpTag::Logical(LogTag::Source));
	assert_eq!(sources.len(), 3, "the three reads of `t`");
	let keys: Vec<_> =
		sources.iter().map(|op| identity::key_with(&ir, &mut facts, &env.cx(), *op)).collect();
	assert_eq!(keys[0].tokens.len(), 1, "a read takes the store token");
	assert_eq!(keys[0].tokens, keys[1].tokens, "no write stands between the first two reads");
	assert_ne!(keys[1].tokens, keys[2].tokens, "the write stands between the last two");
}

/// The stored fields a demand names, or `None` for the whole row.
fn named(demand: &liveness::FieldDemand) -> Option<Vec<String>> {
	match &demand.stored {
		FieldSet::All => None,
		FieldSet::Some(fields) => Some(fields.iter().map(|field| field.to_string()).collect()),
	}
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn liveness_cases() {
	let ir = bind("SELECT a FROM t").await;
	let select = loop_of(&ir, LoopKind::Select);
	let demand = liveness::of(&ir, &mut FactCache::new(), select);
	assert_eq!(named(&demand), Some(vec!["a".to_owned()]), "a projection reads the field it names");
	assert!(demand.computed.is_empty());
	assert!(demand.guarded.is_empty());

	let ir = bind("SELECT a FROM t WHERE b > 1").await;
	let select = loop_of(&ir, LoopKind::Select);
	let demand = liveness::of(&ir, &mut FactCache::new(), select);
	let mut fields = named(&demand).expect("no whole-row use");
	fields.sort();
	assert_eq!(fields, vec!["a".to_owned(), "b".to_owned()], "the filter reads `b`");

	let ir = bind("SELECT * FROM t").await;
	let select = loop_of(&ir, LoopKind::Select);
	let demand = liveness::of(&ir, &mut FactCache::new(), select);
	assert_eq!(demand.stored, FieldSet::All, "`*` reads the whole row");

	let ir = bind("DEFINE FUNCTION fn::r() { RETURN $this.v }; SELECT fn::r() FROM t").await;
	let select = loop_of(&ir, LoopKind::Select);
	let demand = liveness::of(&ir, &mut FactCache::new(), select);
	assert_eq!(demand.stored, FieldSet::All, "a call that takes the row reads the whole row");

	// A layer derives `c` from the whole row it takes in, so `c` is not derived on demand.
	let ir = bind_over("DEFINE TABLE t; DEFINE FIELD c ON t COMPUTED a + 1;", "SELECT c FROM t")
		.await
		.ir;
	let select = loop_of(&ir, LoopKind::Select);
	let demand = liveness::of(&ir, &mut FactCache::new(), select);
	assert!(demand.computed.is_empty(), "the layer derives `c`");
	assert_eq!(demand.stored, FieldSet::All, "the layer reads the whole row");

	let ir = bind("UPDATE t SET a = 1").await;
	let write = loop_of(&ir, LoopKind::Write);
	let demand = liveness::of(&ir, &mut FactCache::new(), write);
	assert_eq!(demand.stored, FieldSet::All, "a write reads and rewrites the record");

	assert_eq!(
		liveness::FieldDemand::top().computed,
		smallvec::SmallVec::<[Strand; 4]>::new(),
		"the top demand names no computed field"
	);
}

/// The first projection layer of `ir`.
fn layer_of(ir: &Ir) -> OpId {
	all_ops(ir)
		.into_iter()
		.find(|op| {
			if let OpKind::Logical(LogOp::Project(attrs)) = kind_of(ir, *op) {
				attrs.layer
			} else {
				false
			}
		})
		.expect("the container holds a projection layer")
}

/// The op that yields the value of the first `WHERE` clause of `ir`.
fn where_conjunct(ir: &Ir) -> OpId {
	let filter = first_tagged(ir, OpTag::Logical(LogTag::Filter));
	let region = *ir.op(filter).expect("the filter exists").regions.first().expect("a clause");
	let value = multiplicity::region_result(ir, region).expect("the clause yields a value");
	match ir.value(value).expect("the value exists").def {
		crate::exec::ir::core::value::ValueDef::OpResult {
			op,
			..
		} => op,
		crate::exec::ir::core::value::ValueDef::BlockArg {
			..
		} => panic!("the clause yields one of its own arguments"),
	}
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn merge_and_row_view() {
	/// `sql` bound for the record user `user:reader`, whose reads of `vault` the table's select
	/// permission filters.
	async fn bind_vault(sql: &str) -> Ir {
		const VAULT: &str = "DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			DEFINE TABLE user SCHEMALESS PERMISSIONS FULL; \
			DEFINE TABLE vault PERMISSIONS FOR select WHERE owner = $auth.id AND $auth.active = true;";
		let reader = crate::dbs::Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader")),
		);
		crate::exec::ir::tests_plan_request::bound_as(&reader, VAULT, sql).await.ir
	}

	// A write of the statement changes what the permission reads.
	let ir = bind_vault(
		"SELECT VALUE [id, (UPDATE user:reader SET active = false RETURN NONE)] FROM vault;",
	)
	.await;
	let filter = first_tagged(&ir, OpTag::Logical(LogTag::PermissionFilter));
	assert!(
		!frames::merge_ok(&ir, &mut FactCache::new(), filter).condition1,
		"the statement writes the record `$auth` names"
	);

	let ir = bind_vault("SELECT * FROM vault;").await;
	let filter = first_tagged(&ir, OpTag::Logical(LogTag::PermissionFilter));
	assert!(
		frames::merge_ok(&ir, &mut FactCache::new(), filter).condition1,
		"a read-only statement leaves the permission's reads alone"
	);

	// The layer delivers the row its fields' guards and derivations shape, so an op that reads
	// that row is masked: the filter over the layer takes it as its operand. A conjunct reads the
	// row through its clause's own argument, which the caller judges field by field
	// (`PushBelowLayer`), and one that reads no row needs no view.
	const LAYERED: &str = "DEFINE TABLE t; DEFINE FIELD c ON t COMPUTED 1;";
	let ir = bind_over(LAYERED, "SELECT * FROM t WHERE $this = {}").await.ir;
	let filter = first_tagged(&ir, OpTag::Logical(LogTag::Filter));
	assert_eq!(
		frames::row_view(&ir, &mut FactCache::new(), filter, layer_of(&ir)),
		RowView::Masked,
		"the filter reads the row the layer delivers"
	);
	let ir = bind_over(LAYERED, "SELECT * FROM t WHERE $p = 1").await.ir;
	assert_eq!(
		frames::row_view(&ir, &mut FactCache::new(), where_conjunct(&ir), layer_of(&ir)),
		RowView::Real,
		"a conjunct that reads no row needs no view"
	);
	assert!(
		!frames::layer_work_unobservable(&ir, &mut FactCache::new(), layer_of(&ir)),
		"the layer's guards cannot be read"
	);
}

/// A layer over one row whose single column is evaluated in a field frame holding `body`, and
/// the layer op.
fn layer_over_column(body: OpKind) -> (Ir, OpId) {
	fn spec(kind: OpKind, operands: &[ValueId], results: usize) -> OpSpec {
		OpSpec {
			kind,
			operands: operands.iter().copied().collect(),
			results: (0..results).map(|_| TypeKnowledge::Unknown).collect(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Synthesized,
			},
		}
	}
	let mut ir = Ir::new(request_scope(), RegionKind::Request);
	let mut facts = FactCache::new();
	let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let at = InsertPoint::End(block);
	let row = rw
		.create(at, spec(OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1)))), &[], 1))
		.expect("the row is created");
	let row = rw.ir().op(row).expect("the row exists").results[0];
	let attrs = ProjectAttrs {
		fields: FieldsShape::Value {
			alias: None,
		},
		layer: true,
		omit: 0,
		names: Vec::new(),
		computed: Vec::new(),
		sort_keys: Vec::new(),
		judged: None,
	};
	let layer = rw
		.create(at, spec(OpKind::Logical(LogOp::Project(Box::new(attrs))), &[row], 1))
		.expect("the layer is created");
	let (_, frame) = rw
		.create_region(layer, RegionKind::FieldFrame(FieldRole::Computed), &[])
		.expect("the field frame is created");
	let column =
		rw.create(InsertPoint::End(frame), spec(body, &[], 1)).expect("the column body is created");
	let column = rw.ir().op(column).expect("the body exists").results[0];
	rw.create(InsertPoint::End(frame), spec(OpKind::Expr(ExprOp::Yield), &[column], 0))
		.expect("the frame is closed");
	(ir, layer)
}

#[test]
fn a_layer_over_a_hidden_field_records_no_effect_from_the_body() {
	// A field whose select guard denies the caller outright is a layer column set to `NONE`. The
	// layer does not hold its body, so it answers nothing the body would.
	// A column whose body the layer does hold is analysed with the rest of the layer.
	let env = Env::new();
	let none = || OpKind::Expr(ExprOp::Literal(Box::new(Value::None)));
	let drawing = || OpKind::Expr(ExprOp::call(Function::Normal("rand::float".to_owned())));

	let (ir, hidden) = layer_over_column(none());
	let answer = effects::of_with(&ir, &mut FactCache::new(), &env.cx(), hidden);
	assert!(
		!answer.set.contains(EffectSet::ENTROPY),
		"the hidden column holds no body to draw: {:?}",
		answer.set
	);

	let (ir, visible) = layer_over_column(drawing());
	let answer = effects::of_with(&ir, &mut FactCache::new(), &env.cx(), visible);
	assert!(
		answer.set.contains(EffectSet::ENTROPY),
		"a column that holds the drawing body records it: {:?}",
		answer.set
	);
}

#[test]
fn the_effect_memo_answers_only_the_context_it_was_computed_in() {
	// The cache is shared by contexts whose builtin registries differ. A registry without the
	// builtin answers the top, so that answer must not be reused for the full registry.
	let drawing = OpKind::Expr(ExprOp::call(Function::Normal("rand::float".to_owned())));
	let (ir, visible) = layer_over_column(drawing);
	let mut facts = FactCache::new();
	let unknown = Env {
		registry: FunctionRegistry::new(),
		frames: FrameTable::default(),
	};
	let blind = effects::of_with(&ir, &mut facts, &unknown.cx(), visible);
	assert!(blind.set.contains(EffectSet::WRITES_DATA), "an unknown call answers the top");
	let env = Env::new();
	let known = effects::of_with(&ir, &mut facts, &env.cx(), visible);
	assert!(known.set.contains(EffectSet::ENTROPY), "{:?}", known.set);
	assert!(!known.set.contains(EffectSet::WRITES_DATA), "{:?}", known.set);
	let again = effects::of_with(&ir, &mut facts, &unknown.cx(), visible);
	assert_eq!(again, blind, "each context keeps its own answer");
}

#[tokio::test]
async fn frames_check_rejects_a_captured_caller_value() {
	// The body names `$q` as a free parameter, which resolves by name and captures nothing of
	// its caller, so no isolated region reads a value defined outside it.
	let ir = bind("LET $q = 1; DEFINE FUNCTION fn::f() { RETURN $q }").await;
	assert_eq!(frames::check(&ir), Ok(()), "a free name is not a captured value");
	let inside_isolated = ops_tagged(&ir, OpTag::Expr(ExprTag::FreeParam)).into_iter().any(|op| {
		let mut region = ir.enclosing_region(op);
		loop {
			if ir.region(region).is_some_and(|info| info.kind.isolated_from_above()) {
				return true;
			}
			let Some(owner) = ir.parent_op(region) else {
				return false;
			};
			region = ir.enclosing_region(owner);
		}
	});
	assert!(inside_isolated, "the body's free parameter stands in an isolated region");
}

/// Why `op` is outside the compute slice, or `None` when it is inside it.
fn exclusion_of(slice: &slice::Slice, op: OpId) -> Option<SliceExclusion> {
	slice.residue.iter().find(|(held, _)| *held == op).map(|(_, why)| *why)
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn slice_cases() {
	let env = Env::new();
	let cx = env.cx();

	// A draw of entropy is apply work in a loop that applies, and what uses its result follows it.
	let ir = bind("UPDATE t SET x = rand() * n").await;
	let write = loop_of(&ir, LoopKind::Write);
	let cut = loop_legality::apply_cut(&ir, &mut FactCache::new(), &cx, write);
	assert_eq!(cut, crate::exec::ir::iface::r#loop::ApplyCut::Ordered, "the loop writes");
	let held = slice::of(&ir, &mut FactCache::new(), &cx, write);
	let draw = first_tagged(&ir, OpTag::Expr(ExprTag::Call));
	assert_eq!(
		exclusion_of(&held, draw),
		Some(SliceExclusion::Effect(ChainRefusal::Entropy)),
		"`rand()` leaves the slice"
	);
	let product = *ops_tagged(&ir, OpTag::Expr(ExprTag::Binary))
		.last()
		.expect("the assignment multiplies the draw");
	assert_eq!(
		exclusion_of(&held, product),
		Some(SliceExclusion::DependsOnApply),
		"the product uses the draw"
	);
	let assigns = ops_tagged(&ir, OpTag::Logical(LogTag::Assign));
	assert!(
		assigns.iter().all(|assign| exclusion_of(&held, *assign).is_some()),
		"the assignment depends on apply"
	);
	assert!(
		held.compute.iter().all(|op| exclusion_of(&held, *op).is_none()),
		"no op is both compute and residue"
	);

	// A read of the record an earlier write of the iteration changes waits for every iteration's
	// write: the write's stages run in a loop of their own, whose keys widen where they leave it
	// (`footprint::relative_to`), so no key names one record per iteration of the `FOR`.
	let bound = bind_over(
		"DEFINE TABLE user SCHEMALESS;",
		"FOR $u IN (SELECT VALUE id FROM user) { UPDATE $u SET seen = true; RETURN $u.seen }",
	)
	.await;
	let (ir, over) = (&bound.ir, bound.cx(&env.registry));
	let each = loop_of(ir, LoopKind::For);
	let held = slice::of(ir, &mut FactCache::new(), &over, each);
	let later = *field_reads(ir, "seen").last().expect("the loop reads `seen` back");
	let write = *ops_below(ir, each)
		.iter()
		.find(|op| multiplicity::loop_kind(ir, **op) == Some(LoopKind::Write))
		.expect("`UPDATE $u` writes");
	let source = ir.op(write).expect("the write exists").operands[0];
	let crate::exec::ir::core::value::ValueDef::OpResult {
		op: subject,
		..
	} = ir.value(source).expect("`UPDATE $u` reads its subject").def
	else {
		panic!("the subject is an op's value");
	};
	assert_eq!(exclusion_of(&held, subject), None, "the `UPDATE $u` subject read is compute");
	assert_eq!(
		exclusion_of(&held, later),
		Some(SliceExclusion::CrossIteration),
		"the write's keys widen across the `FOR`"
	);

	// A read that another iteration's write may reach is not disjoint from it.
	let ir = bind("FOR $i IN [1, 2] { LET $a = t:1.n; UPDATE t:1 SET n = $a + 1 }").await;
	let each = loop_of(&ir, LoopKind::For);
	let held = slice::of(&ir, &mut FactCache::new(), &cx, each);
	let reads = data_reads_below(&ir, &env, each);
	assert!(!reads.is_empty(), "the body reads `t:1.n`");
	for read in reads {
		assert_eq!(
			exclusion_of(&held, read),
			Some(SliceExclusion::CrossIteration),
			"`t:1` is written by every iteration"
		);
	}

	// What computes from the record image of a nested write waits for apply, since the image is
	// produced in apply order. A compound assignment reads its place inside `logical.assign`, so
	// the image is read by an explicit field read.
	let ir = bind("FOR $i IN 1..3 { UPDATE t:1 SET n = n + $i }").await;
	let each = loop_of(&ir, LoopKind::For);
	let held = slice::of(&ir, &mut FactCache::new(), &cx, each);
	let image_reads = field_reads(&ir, "n");
	assert!(!image_reads.is_empty(), "the assignment reads `n` off the image");
	for read in image_reads {
		assert_eq!(
			exclusion_of(&held, read),
			Some(SliceExclusion::DependsOnApply),
			"the image is produced in apply order"
		);
	}
	let sums: Vec<OpId> = ops_below(&ir, each)
		.into_iter()
		.filter(|op| kind_of(&ir, *op).tag() == OpTag::Expr(ExprTag::Binary))
		.collect();
	assert!(!sums.is_empty(), "the assignment adds to the image");
	for sum in sums {
		assert!(exclusion_of(&held, sum).is_some(), "the SET arithmetic is residue");
	}

	// A write that ignores failures skips the failing row.
	let ir = bind("INSERT IGNORE INTO t { id: 1 }").await;
	assert_eq!(
		slice::on_failure(&ir, loop_of(&ir, LoopKind::Write)),
		OnFailure::SkipRow,
		"`INSERT IGNORE` skips a failing record"
	);
	let ir = bind("UPDATE t SET x = 1").await;
	assert_eq!(
		slice::on_failure(&ir, loop_of(&ir, LoopKind::Write)),
		OnFailure::Raise,
		"an ordinary write raises"
	);
}

#[tokio::test]
async fn certainty_cases() {
	let env = Env::new();
	let cx = env.cx();

	// A read in the projection runs for every row.
	let ir = bind("SELECT author.name FROM post").await;
	let select = loop_of(&ir, LoopKind::Select);
	let read = field_reads(&ir, "name")[0];
	assert_eq!(
		certainty::of(&ir, &mut FactCache::new(), &cx, select, read),
		ReadCertainty::Certain,
		"the deref runs in every iteration"
	);

	// A read in an arm of an `IF` runs when the arm is taken.
	let ir = bind("SELECT IF flag { author.name } FROM post").await;
	let select = loop_of(&ir, LoopKind::Select);
	let read = field_reads(&ir, "name")[0];
	assert_eq!(
		certainty::of(&ir, &mut FactCache::new(), &cx, select, read),
		ReadCertainty::Conditional,
		"the arm may not be taken"
	);

	// A `RETURN` in the body ends the outer loop, so no read below it is certain to run in an
	// iteration: the answer is the weakest.
	let ir = bind("FOR $x IN (SELECT * FROM t) { FOR $y IN $x.tags { RETURN t:1.n } }").await;
	let outer = loop_of(&ir, LoopKind::For);
	let reads = field_reads(&ir, "n");
	assert!(!reads.is_empty(), "the nested body reads `t:1.n`");
	for read in reads {
		assert_eq!(
			certainty::of(&ir, &mut FactCache::new(), &cx, outer, read),
			ReadCertainty::PastExit,
			"the `RETURN` may end the loop before the read"
		);
	}

	// A loop that may stop early, or that a `LIMIT` stops, reads nothing for certain.
	let ir = bind("FOR $i IN [1, 2, 3] { IF $i > 1 { BREAK }; RETURN t:1.n }").await;
	let each = loop_of(&ir, LoopKind::For);
	let reads = data_reads_below(&ir, &env, each);
	assert!(!reads.is_empty(), "the body reads `t:1.n`");
	assert_eq!(
		certainty::of(&ir, &mut FactCache::new(), &cx, each, reads[0]),
		ReadCertainty::PastExit,
		"`BREAK` may end the loop before the read"
	);
	let ir = bind("SELECT author.name FROM post LIMIT 1").await;
	let select = loop_of(&ir, LoopKind::Select);
	let read = field_reads(&ir, "name")[0];
	assert_eq!(
		certainty::of(&ir, &mut FactCache::new(), &cx, select, read),
		ReadCertainty::PastExit,
		"the limit stops the scan"
	);
}

/// The full legality verdict of the first loop of `kind` in `sql`, under the builtin registry.
async fn legality_of(sql: &str, kind: LoopKind) -> loop_legality::LoopLegality {
	let env = Env::new();
	let ir = bind(sql).await;
	let each = loop_of(&ir, kind);
	loop_legality::of_with(&ir, &mut FactCache::new(), &env.cx(), each).expect("the loop is judged")
}

/// [`legality_of`] for `sql` bound over a database `setup` builds ([`bind_over`]), judged under
/// the catalog its binder read.
#[cfg(feature = "kv-mem")]
async fn legality_over(setup: &str, sql: &str, kind: LoopKind) -> loop_legality::LoopLegality {
	let env = Env::new();
	let bound = bind_over(setup, sql).await;
	let each = loop_of(&bound.ir, kind);
	loop_legality::of_with(&bound.ir, &mut FactCache::new(), &bound.cx(&env.registry), each)
		.expect("the loop is judged")
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn legality_cases() {
	use crate::exec::ir::iface::r#loop::{
		ApplyCut, BarrierCause, Exit, ExitCause, LoopMode, LoopRefusal,
	};

	// An array may repeat an element, so a loop that writes along an unrelated key is serial.
	let held = legality_over(
		"DEFINE TABLE t SCHEMALESS;",
		"FOR $i IN [1, 2, 3] { UPDATE t:1 SET n = $i }",
		LoopKind::For,
	)
	.await;
	assert_eq!(held.apply, ApplyCut::Ordered, "the body writes");
	assert_eq!(held.multiplicity, Multiplicity::MayRepeat);
	assert_eq!(held.mode(), LoopMode::Serial(LoopRefusal::MayRepeatWrite));

	// A range delivers each integer once: the same body is independent.
	let held = legality_of("FOR $i IN 1..3 { UPDATE t:1 SET n = $i }", LoopKind::For).await;
	assert_eq!(held.multiplicity, Multiplicity::Distinct);
	assert_eq!(held.mode(), LoopMode::Independent);

	// A write whose subject is the row of a scan is pinned to its table and keyed by the row's id.
	let held =
		legality_of("FOR $u IN (SELECT * FROM user) { UPDATE $u SET seen = true }", LoopKind::For)
			.await;
	assert_eq!(held.multiplicity, Multiplicity::Distinct);
	assert_eq!(held.key_path, ElemPath::Id);
	assert_eq!(held.barrier, None, "provenance pins the subject to `user`");
	assert_eq!(held.apply, ApplyCut::Ordered);
	// The write's footprint states its key unknown, so it may write a record of `user` other than
	// the element's: the loop is serial.
	assert_eq!(held.mode(), LoopMode::Serial(LoopRefusal::SourceWritten));

	// A stored result is an array of unknown provenance: the subject's table is not pinned.
	let held = legality_of(
		"LET $us = (SELECT * FROM user); FOR $u IN $us { UPDATE $u SET seen = true }",
		LoopKind::For,
	)
	.await;
	assert_eq!(held.barrier, Some(BarrierCause::UnpinnedDynamicTarget));
	assert_eq!(
		held.mode(),
		LoopMode::Serial(LoopRefusal::Barrier(BarrierCause::UnpinnedDynamicTarget))
	);

	// A decisive element ends the loop in order. A builtin called with a closure is a closure loop;
	// a method call dispatches on its receiver when it runs, and is no loop.
	let held = legality_of("RETURN array::find([1, 2, 3], |$v| $v > 1)", LoopKind::Closure).await;
	assert_eq!(held.exit, Exit::Ordered(ExitCause::Decisive));
	assert_eq!(held.mode(), LoopMode::Serial(LoopRefusal::OrderedExit));
	assert_eq!(held.legality().sink, SinkCombine::NotSplittable);

	// Each refusal of the compute slice names itself; a read of the clock is row-local.
	for (sql, refusal) in [
		("RETURN array::map([1, 2], |$v| rand())", ChainRefusal::Entropy),
		("RETURN array::map([1, 2], |$v| sequence::nextval('s'))", ChainRefusal::AdvancesState),
		("RETURN array::map([1, 2], |$v| sleep(1ms))", ChainRefusal::Blocks),
		("RETURN array::map([1, 2], |$v| fn::f())", ChainRefusal::Opaque),
		("RETURN array::map([1, 2], |$v| http::get('http://localhost/'))", ChainRefusal::External),
	] {
		let held = legality_of(sql, LoopKind::Closure).await;
		assert_eq!(held.mode(), LoopMode::Serial(LoopRefusal::Effect(refusal)), "`{sql}`");
	}
	let held = legality_of("RETURN array::map([1, 2], |$v| time::now())", LoopKind::Closure).await;
	assert_eq!(held.mode(), LoopMode::Independent, "the clock is a statement constant");

	// The body of a closure the loop invokes runs in the loop, so a barrier in it is the loop's.
	let held =
		legality_of("RETURN array::map([1, 2], |$v| (DEFINE TABLE y))", LoopKind::Closure).await;
	assert_eq!(held.barrier, Some(BarrierCause::CatalogChange));
	assert_eq!(held.mode(), LoopMode::Serial(LoopRefusal::Barrier(BarrierCause::CatalogChange)));

	// A `BREAK` ends the loop it is written in; a nested loop absorbs its own, and `CONTINUE`
	// ends only its iteration.
	let held = legality_of("FOR $i IN [1, 2, 3] { IF $i > 1 { BREAK } }", LoopKind::For).await;
	assert_eq!(held.exit, Exit::Ordered(ExitCause::Break));
	assert_eq!(held.mode(), LoopMode::Serial(LoopRefusal::OrderedExit));
	let ir = bind("FOR $i IN [1, 2] { FOR $j IN [3, 4] { BREAK } }").await;
	let loops = ops_tagged(&ir, OpTag::Logical(LogTag::Loop));
	assert_eq!(loops.len(), 2, "the two FOR loops are in the container");
	assert_eq!(loop_legality::exit_of(&ir, loops[0]), Exit::None, "the inner loop absorbs it");
	assert_eq!(loop_legality::exit_of(&ir, loops[1]), Exit::Ordered(ExitCause::Break));
	let held = legality_of(
		"FOR $i IN [1, 2, 3] { IF $i > 1 { CONTINUE }; SELECT * FROM person }",
		LoopKind::For,
	)
	.await;
	assert_eq!(held.exit, Exit::None, "`CONTINUE` ends only the iteration");

	// Each barrier names itself.
	for (sql, cause) in [
		("FOR $i IN [1, 2] { SLEEP 1ms }", BarrierCause::Sleep),
		("FOR $i IN [1, 2] { DEFINE TABLE x }", BarrierCause::CatalogChange),
	] {
		let held = legality_of(sql, LoopKind::For).await;
		assert_eq!(held.barrier, Some(cause), "`{sql}`");
		assert_ne!(held.mode(), LoopMode::Independent, "`{sql}`");
		assert!(!held.may_fan_out(), "`{sql}`");
	}

	// A scan with a row-local body is independent, and its projections agree with the verdict.
	let held = legality_of("SELECT *, time::now() FROM t", LoopKind::Select).await;
	assert_eq!(held.mode(), LoopMode::Independent);
	assert_eq!(held.source, SourceSplit::Range);
	assert_eq!(held.carried, loop_legality::Carried::None);
	let facts = held.facts();
	assert_eq!(facts.mode, held.mode());
	assert_eq!(facts.apply, held.apply);
	assert_eq!(facts.exit, held.exit);
	assert_eq!(facts.delivery, held.delivery);
	assert_eq!(facts.multiplicity, held.multiplicity);
	assert_eq!(facts.key_path, held.key_path);
	assert_eq!(held.legality().source, SourceSplit::Range);
	assert_eq!(held.legality().sink, SinkCombine::Concat);
	assert_eq!(held.legality().chain, ChainLegality::RowLocal);
	assert_eq!(held.legality().error_order, ErrorOrder::FirstByInputPosition);
	assert!(held.may_fan_out());

	// A write loop's sink holds state a part cannot own.
	let held = legality_of("UPDATE t SET x = 1", LoopKind::Write).await;
	assert_eq!(held.apply, ApplyCut::Ordered);
	assert_eq!(held.legality().sink, SinkCombine::NotSplittable);

	// This entry point answers the projection under the default context.
	let ir = bind("SELECT * FROM t").await;
	let select = loop_of(&ir, LoopKind::Select);
	let frozen = loop_legality::of(&ir, &mut FactCache::new(), select).expect("a loop");
	let env = Env::new();
	let full =
		loop_legality::of_with(&ir, &mut FactCache::new(), &env.cx(), select).expect("a loop");
	assert_eq!(frozen.mode, full.mode());
	assert_eq!(frozen.apply, full.apply);

	// An op that is not a loop, and an id that names nothing, are stale.
	let stmt = first_tagged(&ir, OpTag::Logical(LogTag::Stmt));
	assert!(matches!(
		loop_legality::of(&ir, &mut FactCache::new(), stmt),
		Err(crate::exec::ir::error::IrError::Stale(_))
	));
	assert!(matches!(
		loop_legality::of(&ir, &mut FactCache::new(), OpId::default()),
		Err(crate::exec::ir::error::IrError::Stale(_))
	));

	// Every loop of the container is listed once, in pre-order.
	let ir = bind("FOR $i IN [1] { SELECT * FROM t }").await;
	let listed: Vec<OpId> = loop_legality::all_loops(&ir, &mut FactCache::new(), &env.cx())
		.into_iter()
		.map(|(op, _)| op)
		.collect();
	assert_eq!(listed, ops_tagged(&ir, OpTag::Logical(LogTag::Loop)));
}

#[tokio::test]
async fn delivery_is_read_whole_for_table_sources() {
	use crate::exec::ir::iface::r#loop::SourceDelivery;

	// A table scan may fail on stored data, so its error precedes every body error.
	for (sql, kind) in [
		("SELECT * FROM t", LoopKind::Select),
		("SELECT * FROM t:1..3", LoopKind::Select),
		("UPDATE t SET x = 1", LoopKind::Write),
		("FOR $u IN (SELECT * FROM user) { RETURN $u }", LoopKind::For),
	] {
		let held = legality_of(sql, kind).await;
		assert_eq!(held.delivery, SourceDelivery::ReadWhole, "`{sql}`");
	}
}

/// The arrival a loop's legality states follows the consumer that combines its rows: a sort that
/// keeps every row positions them, an aggregate of associative combines is adjacent, and a loop
/// whose rows a clause counts, whose sink concatenates, or whose combine is not associative
/// arrives in iteration order.
#[tokio::test]
async fn a_loops_arrival_follows_the_consumer_that_combines_its_rows() {
	for (sql, arrival) in [
		("SELECT * FROM person ORDER BY age", Arrival::Positioned),
		("SELECT city, count() FROM person GROUP BY city", Arrival::Adjacent),
		("SELECT array::group(age) FROM person GROUP ALL", Arrival::Adjacent),
		("SELECT city, math::sum(age) FROM person GROUP BY city", Arrival::Serial),
		("SELECT name FROM person ORDER BY age LIMIT 10", Arrival::Serial),
		("SELECT * FROM person", Arrival::Serial),
		("SELECT * FROM person LIMIT 10 START 5", Arrival::Serial),
	] {
		let verdict = legality_of(sql, LoopKind::Select).await.legality();
		assert_eq!(verdict.arrival, arrival, "`{sql}`");
	}
	// A write applies the effects of its iterations in iteration order.
	let verdict = legality_of("UPDATE person SET age = 1", LoopKind::Write).await.legality();
	assert_eq!(verdict.arrival, Arrival::Serial);
}

/// The ops of every loop of `sql`, the legality of each in pre-order.
async fn all_legalities(sql: &str) -> Vec<(OpId, loop_legality::LoopLegality)> {
	let env = Env::new();
	let ir = bind(sql).await;
	loop_legality::all_loops(&ir, &mut FactCache::new(), &env.cx())
}

/// The refusal the effects of the last statement of `sql` name.
async fn statement_refusal(sql: &str) -> Option<ChainRefusal> {
	let env = Env::new();
	let ir = bind(sql).await;
	let statements = ops_tagged(&ir, OpTag::Logical(LogTag::Stmt));
	let last = *statements.last().expect("the request holds a statement");
	loop_legality::refusal_of(effects::of_with(&ir, &mut FactCache::new(), &env.cx(), last).set)
}

/// The legality of each arm of each `IF` of the first `FOR` loop of `sql`, and the loop's
/// `foreach` projection over them.
async fn foreach_of(sql: &str) -> crate::exec::logical::pipelines::ForeachLegality {
	let env = Env::new();
	let ir = bind(sql).await;
	let each = loop_of(&ir, LoopKind::For);
	let mut facts = FactCache::new();
	let held = loop_legality::of_with(&ir, &mut facts, &env.cx(), each).expect("a loop");
	let arms = loop_legality::if_arms(&ir, &mut facts, &env.cx(), each);
	held.foreach(arms)
}

/// Checks that the verdicts of a loop's projections agree with the pipeline verdicts, except as
/// noted below:
///
/// - a write is apply work, so the compute slice stays row-local where the pipeline's chain refuses
///   it, and the loop's apply cut records the write;
/// - `may_raise` comes from transitive fallibility: the IR answer raises wherever the pipeline's
///   allow-list does, and it may also raise for a pipeline the allow-list clears;
/// - a statement in `FROM` is an ordinal source of the loop that reads it, where the pipeline reads
///   it whole.
#[tokio::test]
async fn port_of_pipelines_tests() {
	use ChainLegality::{Refused, RowLocal};
	use SourceSplit::{Ordinal, Range};

	use crate::exec::ir::iface::r#loop::ApplyCut;
	use crate::exec::logical::pipelines::{AggregateCombine, Paging};

	// The chain verdict follows the effects of each projection.
	for (projected, expected) in [
		("time::now()", RowLocal),
		("age + 1", RowLocal),
		("(SELECT VALUE name FROM company)", RowLocal),
		("rand()", Refused(ChainRefusal::Entropy)),
		("sequence::nextval('s')", Refused(ChainRefusal::AdvancesState)),
		("sleep(1ms)", Refused(ChainRefusal::Blocks)),
		("[sleep(1ms), rand()]", Refused(ChainRefusal::Entropy)),
	] {
		let held =
			legality_of(&format!("SELECT VALUE {projected} FROM person"), LoopKind::Select).await;
		assert_eq!(held.legality().chain, expected, "{projected}");
	}
	let held = legality_of("SELECT VALUE (CREATE log:1) FROM person", LoopKind::Select).await;
	assert_eq!(held.apply, ApplyCut::Ordered, "a write is apply work");

	// Each breaker sets the source, sink and paging of the loop.
	for (sql, source, sink, paging) in [
		("SELECT * FROM person", Range, SinkCombine::Concat, Paging::None),
		("SELECT * FROM person ORDER BY age", Range, SinkCombine::StableMerge, Paging::None),
		("SELECT * FROM person ORDER BY age LIMIT 10", Range, SinkCombine::TopK, Paging::None),
		(
			"SELECT count() FROM person GROUP ALL",
			Range,
			SinkCombine::Aggregate(vec![AggregateCombine::Exact]),
			Paging::None,
		),
		(
			"SELECT city, math::sum(age) FROM person GROUP BY city",
			Range,
			SinkCombine::Aggregate(vec![AggregateCombine::InexactFloat]),
			Paging::None,
		),
		("SELECT * FROM person LIMIT 10 START 5", Range, SinkCombine::Concat, Paging::One),
		("SELECT * FROM person, company", Ordinal, SinkCombine::Concat, Paging::None),
	] {
		let verdict = legality_of(sql, LoopKind::Select).await.legality();
		assert_eq!(verdict.source, source, "{sql}");
		assert_eq!(verdict.sink, sink, "{sql}");
		assert_eq!(verdict.paging, paging, "{sql}");
		assert_eq!(verdict.chain, RowLocal, "{sql}");
	}
	// A statement written in place, and two clauses over the same rows. A `LIMIT` stands between
	// the loop that reads the rows and the loop that projects them, so the outer read is the loop
	// over the statement's result.
	let ir = bind("SELECT * FROM (SELECT * FROM person LIMIT 5) LIMIT 2").await;
	let loops = loop_legality::all_loops(&ir, &mut FactCache::new(), &Env::new().cx());
	let reads_statement = |loop_op: OpId| {
		let source = ir.op(loop_op).expect("the loop exists").operands[0];
		matches!(
			ir.value(source).expect("the source exists").def,
			crate::exec::ir::core::value::ValueDef::OpResult { op, .. }
				if kind_of(&ir, op).tag() == OpTag::Logical(LogTag::Subquery)
		)
	};
	let inner = loops.iter().find(|(_, held)| held.source == Range).expect("the inner scan");
	let outer = loops.iter().find(|(op, _)| reads_statement(*op)).expect("the outer read");
	assert_eq!(outer.1.source, Ordinal);
	assert_eq!(inner.1.legality().paging, Paging::One);
	assert_eq!(outer.1.legality().paging, Paging::One);
	assert_eq!(inner.1.legality().sink, SinkCombine::Concat);

	// Iterations are independent unless the body writes or returns, or a BREAK of the loop stops
	// it.
	let reads = foreach_of("FOR $i IN [1, 2, 3] { SELECT * FROM person WHERE age > $i }").await;
	assert!(reads.iterations_independent);
	assert!(reads.arms.is_empty());
	let writes = foreach_of("FOR $i IN [1, 2, 3] { CREATE log SET n = $i }").await;
	assert!(!writes.iterations_independent);
	for (sql, independent) in [
		("FOR $i IN [1, 2] { IF $i = 1 { BREAK }; THROW 'x' }", false),
		("FOR $i IN [1, 2, 3] { RETURN $i }", false),
		("FOR $i IN [1, 2, 3] { IF $i > 1 { RETURN $i } ELSE { SELECT * FROM person } }", false),
		("FOR $i IN [1, 2, 3] { { IF $i > 1 { BREAK } } }", false),
		("FOR $i IN [1, 2, 3] { FOR $j IN [1, 2] { RETURN $j } }", false),
		("FOR $i IN [1, 2, 3] { FOR $j IN [1, 2] { BREAK } }", true),
		("FOR $i IN [1, 2, 3] { IF $i > 1 { CONTINUE }; SELECT * FROM person }", true),
	] {
		assert_eq!(foreach_of(sql).await.iterations_independent, independent, "{sql}");
	}
	let arms = foreach_of(
		"FOR $i IN [1, 2, 3] { IF $i > 1 { CREATE log SET n = $i } ELSE { SELECT * FROM person } }",
	)
	.await;
	assert!(!arms.iterations_independent);
	assert_eq!(arms.arms, vec![Refused(ChainRefusal::Writes), RowLocal]);

	// A call whose body the container does not hold refuses the chain.
	for (projected, refusal) in [
		("fn::f()", ChainRefusal::Opaque),
		("$f(1)", ChainRefusal::Opaque),
		("array::map($a, $f)", ChainRefusal::Opaque),
		("array::filter($a, |$x| rand() > 0.5)", ChainRefusal::Entropy),
		("$o.nosuchmethod(1)", ChainRefusal::Opaque),
		("http::get('http://localhost/')", ChainRefusal::External),
		("ml::m<1.0.0>(1)", ChainRefusal::External),
	] {
		let held =
			legality_of(&format!("SELECT VALUE {projected} FROM person"), LoopKind::Select).await;
		assert_eq!(held.legality().chain, Refused(refusal), "{projected}");
	}

	// A read of the clock stays row-local, also inside a closure.
	for projected in ["time::now()", "array::map([1, 2], |$x| time::now())"] {
		let held =
			legality_of(&format!("SELECT VALUE {projected} FROM person"), LoopKind::Select).await;
		assert_eq!(held.legality().chain, RowLocal, "{projected}");
	}

	// An effect set of non-determinism other than the clock refuses as a write.
	assert_eq!(
		loop_legality::refusal_of(EffectSet::NON_DETERMINISTIC_BUT_FOR_CLOCK),
		Some(ChainRefusal::Writes)
	);

	// A method call takes the effects of its implementation. A name with no builtin for an object
	// receiver may run a closure the receiver carries; a write of such a call is apply work, and
	// the apply residue names an opaque body before a write outside the transaction.
	let held = legality_of("SELECT VALUE 'abc'.len() FROM person", LoopKind::Select).await;
	assert_eq!(held.legality().chain, RowLocal);
	for (projected, refusal) in [
		("$v.shuffle()", ChainRefusal::Entropy),
		("$v.put('x')", ChainRefusal::Opaque),
		("$v.get()", ChainRefusal::Opaque),
	] {
		let held =
			legality_of(&format!("SELECT VALUE {projected} FROM person"), LoopKind::Select).await;
		assert_eq!(held.legality().chain, Refused(refusal), "{projected}");
	}

	// A statement refusal covers every pipeline of the statement.
	for (sql, refusal) in [
		("SELECT * FROM person LIMIT rand::int(1, 5)", Some(ChainRefusal::Entropy)),
		(
			"SELECT *, (SELECT VALUE rand() FROM ONLY 1) AS r FROM person ORDER BY age",
			Some(ChainRefusal::Entropy),
		),
		(
			"SELECT sequence::nextval('s') FROM person ORDER BY age LIMIT 3",
			Some(ChainRefusal::AdvancesState),
		),
		("UPDATE person SET age = 1", Some(ChainRefusal::Writes)),
		("SELECT * FROM person LIMIT { SLEEP 1s; 1 }", Some(ChainRefusal::Blocks)),
		("SELECT $o.nosuchmethod() FROM person", Some(ChainRefusal::Opaque)),
		("SELECT time::now() FROM person", None),
		("SELECT * FROM person ORDER BY age LIMIT 10", None),
	] {
		assert_eq!(statement_refusal(sql).await, refusal, "{sql}");
	}

	// Every loop is listed once. A nested loop follows its host or the statement it reads, in
	// pre-order.
	let env = Env::new();
	for sql in [
		"SELECT * FROM person",
		"SELECT *, (SELECT VALUE name FROM company) AS c FROM person",
		"SELECT * FROM person, company",
		"FOR $i IN [1, 2] { SELECT * FROM person }",
	] {
		let ir = bind(sql).await;
		let listed: Vec<OpId> = loop_legality::all_loops(&ir, &mut FactCache::new(), &env.cx())
			.into_iter()
			.map(|(op, _)| op)
			.collect();
		assert_eq!(listed, ops_tagged(&ir, OpTag::Logical(LogTag::Loop)), "{sql}");
	}

	// Top-k sees through the operators that keep every row.
	for sql in [
		"SELECT name FROM person ORDER BY age LIMIT 10",
		"SELECT name, age + 1 AS next FROM person ORDER BY age LIMIT 10 START 5",
	] {
		let verdict = legality_of(sql, LoopKind::Select).await.legality();
		assert_eq!(verdict.source, Range, "{sql}");
		assert_eq!(verdict.sink, SinkCombine::TopK, "{sql}");
		assert_eq!(verdict.paging, Paging::None, "{sql}");
	}
	// A limit under the sort is not the sort's: the sort is a stable merge of its input.
	let loops = all_legalities("SELECT * FROM (SELECT * FROM person LIMIT 5) ORDER BY age").await;
	let sinks: Vec<(SinkCombine, Paging)> = loops
		.iter()
		.map(|(_, held)| {
			let verdict = held.legality();
			(verdict.sink, verdict.paging)
		})
		.collect();
	assert!(sinks.contains(&(SinkCombine::Concat, Paging::One)), "{sinks:?}");
	assert!(sinks.contains(&(SinkCombine::StableMerge, Paging::None)), "{sinks:?}");

	// every_breaker_keys_its_segment: a sort and a grouping each give the loop that feeds them
	// its combine, and a statement that reads no breaker concatenates.
	let verdict = legality_of("SELECT * FROM person ORDER BY age", LoopKind::Select).await;
	assert_eq!(verdict.legality().sink, SinkCombine::StableMerge);
	let verdict =
		legality_of("SELECT city, count() FROM person GROUP BY city", LoopKind::Select).await;
	assert_eq!(verdict.legality().sink, SinkCombine::Aggregate(vec![AggregateCombine::Exact]));

	// Wherever the allow-list says a pipeline may raise, the compute slice's transitive
	// fallibility says so too.
	for sql in [
		"SELECT * FROM person WHERE string::len(name) > 2",
		"SELECT * FROM person WHERE age + 1 > 18",
		"SELECT * FROM person WHERE age IN [1, 2]",
		"SELECT * FROM person WHERE age ?? 1 > 2",
		"SELECT * FROM person WHERE <int> name > 2",
		"SELECT * FROM person WHERE city.x = 'y'",
		"SELECT * FROM person WHERE name @@ 'a'",
		"SELECT * FROM person WHERE name = (SELECT VALUE name FROM company LIMIT 1)",
		"SELECT age * 2 FROM person",
		"SELECT math::abs(age) FROM person",
		"SELECT * FROM person FETCH city",
		"SELECT * OMIT age FROM person",
	] {
		let held = legality_of(sql, LoopKind::Select).await;
		assert!(held.legality().may_raise, "{sql}");
	}

	// A parameter that stays in the statement makes its loop may_raise.
	for sql in [
		"{ LET $n = (SELECT VALUE age FROM person LIMIT 1)[0]; RETURN SELECT * FROM person WHERE age > $n }",
		"SELECT * FROM person WHERE $p != NONE",
	] {
		let loops = all_legalities(sql).await;
		let (_, last) = loops.last().expect("the statement holds a loop");
		assert!(last.legality().may_raise, "{sql}");
	}

	// A refused chain refuses its statement.
	let mut refused = 0;
	for sql in [
		"SELECT * FROM person",
		"SELECT *, rand() FROM person",
		"SELECT * FROM person WHERE age > rand::int(1, 5)",
		"SELECT sequence::nextval('s') FROM person ORDER BY age LIMIT 3",
		"SELECT *, (SELECT VALUE rand() FROM ONLY 1) AS r FROM person ORDER BY age",
		"SELECT * FROM person ORDER BY age LIMIT rand::int(1, 5)",
		"SELECT $o.nosuchmethod() FROM person",
		"SELECT fn::f() FROM person",
	] {
		let statement = statement_refusal(sql).await;
		for (_, held) in all_legalities(sql).await {
			if let Refused(_) = held.legality().chain {
				refused += 1;
				assert!(statement.is_some(), "`{sql}`: a chain refuses and the statement does not");
			}
		}
	}
	assert!(refused > 3, "{refused} refused chains");
}

/// The `GenerateRecordId` stage of a write declares entropy exactly where the write's subjects may
/// name no id, so that the slice puts the draw in the residue of a loop that applies in order, and
/// leaves the stage of a write whose ids are given in the compute slice.
#[tokio::test]
async fn the_stage_that_generates_a_record_id_draws_entropy_only_where_it_draws_the_id() {
	let env = Env::new();
	let cx = env.cx();
	for (sql, draws) in [
		// A table alone, a count of ids and a subject the plan does not state draw an id.
		("CREATE t SET n = 1", true),
		("CREATE |t:3| SET n = 1", true),
		("CREATE t:7000, u SET n = 1", true),
		("LET $subject = u; CREATE $subject SET n = 1", true),
		// The objects an `INSERT` writes hold an id only when they name one.
		("INSERT INTO t (SELECT n FROM t)", true),
		// An id written out and a range of ids hold theirs.
		("CREATE t:7000 SET n = 1", false),
		("CREATE |t:5000..5003| SET n = 1", false),
		("CREATE t:1, t:2 SET n = 1", false),
	] {
		let ir = bind(sql).await;
		let write = loop_of(&ir, LoopKind::Write);
		let stages: Vec<OpId> = ops_tagged(&ir, OpTag::Logical(LogTag::WriteStage))
			.into_iter()
			.filter(|op| {
				matches!(
					kind_of(&ir, *op),
					OpKind::Logical(LogOp::WriteStage {
						stage: WriteStageKind::GenerateRecordId,
						..
					})
				)
			})
			.collect();
		assert_eq!(stages.len(), 1, "`{sql}` holds one stage that generates the id");
		let drawn = slice::judged_effects(&ir, &mut FactCache::new(), &cx, write, stages[0])
			.contains(EffectSet::ENTROPY);
		assert_eq!(drawn, draws, "`{sql}`");
		let held = slice::of(&ir, &mut FactCache::new(), &cx, write);
		let why = exclusion_of(&held, stages[0]);
		if draws {
			assert_eq!(why, Some(SliceExclusion::Effect(ChainRefusal::Entropy)), "`{sql}`");
		} else {
			assert_eq!(why, None, "`{sql}`: the stage that holds an id given is compute");
		}
	}
}
