//! Tests of fold eligibility, the folder's outcome mapping and the runner seam.

use std::future::{Future, ready};
use std::pin::Pin;

use super::evaluate::{BUILTIN_FOLDER, CapabilityPolicy, FoldRun, FoldRunner, VmFolder, eligible};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::build::logical::bind_request;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope, Walk, WalkOrder};
use crate::exec::ir::core::walk::ancestors;
use crate::exec::ir::expr::ops::{ExprOp, FoldedError};
use crate::exec::ir::iface::bounds::FOLD_MAX_BYTES;
use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};
use crate::exec::ir::iface::fallibility::ContextDep;
use crate::exec::ir::iface::fold::{
	FoldConsumer, FoldOutcome, FoldRequest, NotFoldable, PlanTimeFolder,
};
use crate::exec::ir::iface::frame::FrameTable;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::premise::PremiseSet;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::{RequestInput, RequestStatement};
use crate::exec::vm::fold_host::HOST_RUNNER;
use crate::expr::logical::EffectSet;
use crate::val::Value;

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

/// Binds `sql` into a container in the logical dialect.
async fn bind(sql: &str) -> Ir {
	let env = Env::new();
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let request_statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(
		&request_statements,
		IrScope::Request {
			fragment: None,
		},
	);
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

/// The operand of the first `Return` of `ir`, which is the value a test folds or inspects.
fn first_return_operand(ir: &Ir) -> ValueId {
	let ret = all_ops(ir)
		.into_iter()
		.find(|op| ir.op(*op).is_some_and(|data| data.kind.tag() == OpTag::Logical(LogTag::Return)))
		.expect("the request returns");
	ir.op(ret).expect("the op exists").operands[0]
}

fn request(value: ValueId) -> FoldRequest {
	FoldRequest {
		value,
		consumer: FoldConsumer::BranchChoice,
	}
}

/// The eligibility answer for the value the first `RETURN` of `sql` returns.
async fn judge(sql: &str) -> Result<(), NotFoldable> {
	let env = Env::new();
	let ir = bind(sql).await;
	eligible(&ir, &mut FactCache::new(), &env.cx(), first_return_operand(&ir))
}

/// Each of these statements gets a refusal reason from eligibility alone,
/// before the program is lowered or run.
#[tokio::test]
async fn eligible_reasons_in_order() {
	type Reason = fn(&NotFoldable) -> bool;
	let cases: &[(&str, Reason)] = &[
		("RETURN time::now()", |reason| matches!(reason, NotFoldable::Clock)),
		(
			"RETURN rand::float()",
			|reason| matches!(reason, NotFoldable::Effect(effects) if effects.contains(EffectSet::ENTROPY)),
		),
		(
			"DEFINE SEQUENCE s; RETURN sequence::nextval('s')",
			|reason| matches!(reason, NotFoldable::Effect(effects) if effects.contains(EffectSet::ADVANCES_STATE)),
		),
		("RETURN t:1.a", |reason| matches!(reason, NotFoldable::ReadsRecords)),
		("RETURN $auth.id", |reason| {
			matches!(reason, NotFoldable::ContextDep(ContextDep::PrincipalValue))
		}),
	];
	for (sql, reason) in cases {
		match judge(sql).await {
			Err(why) => assert!(reason(&why), "`{sql}` is refused for {why:?}"),
			Ok(()) => panic!("`{sql}` is eligible"),
		}
	}
}

/// A pure call over literals is eligible, and so is a literal.
#[tokio::test]
async fn pure_literal_expressions_are_eligible() {
	for sql in ["RETURN 1", "RETURN 1 + 2", "RETURN math::abs(-3)", "RETURN [1, 2] + [3]"] {
		assert_eq!(judge(sql).await, Ok(()), "`{sql}`");
	}
}

/// A top-level `LET` binds a session variable, which a later statement reads from the session at
/// run time, so it is refused. A stored function is refused because its effects are unknown,
/// and the unknown effects include the clock. A
/// value an error-catching region evaluates is never folded.
#[tokio::test]
async fn eligible_refuses_slots_and_error_scopes() {
	assert_eq!(
		judge("LET $p = 1; RETURN $p + 1").await,
		Err(NotFoldable::ContextDep(ContextDep::PrincipalValue)),
		"a LET value is read from the session"
	);
	assert_eq!(
		judge("RETURN fn::f()").await,
		Err(NotFoldable::Clock),
		"a stored function is not folded"
	);

	let env = Env::new();
	let ir = bind("SELECT count(1 + 'x') AS c FROM [1, 2] GROUP ALL").await;
	let scoped = all_ops(&ir)
		.into_iter()
		.find(|op| {
			let is_binary = matches!(
				ir.op(*op).map(|data| &data.kind),
				Some(OpKind::Expr(ExprOp::Binary { .. }))
			);
			is_binary
				&& ancestors(&ir, *op).any(|(region, _)| {
					matches!(
						ir.region(region).map(|held| held.kind),
						Some(RegionKind::ErrorScope(_))
					)
				})
		})
		.expect("the aggregate argument is evaluated inside an error scope");
	let value = ir.op(scoped).expect("the op exists").results[0];
	assert_eq!(
		eligible(&ir, &mut FactCache::new(), &env.cx(), value),
		Err(NotFoldable::HostRefused("error scope"))
	);
}

/// The builtin folder has no runner, so every eligible value answers `NotYet`.
#[tokio::test]
async fn no_runner_answers_not_yet() {
	let env = Env::new();
	let ir = bind("RETURN 1 + 2").await;
	let outcome = BUILTIN_FOLDER.fold(&ir, &env.cx(), request(first_return_operand(&ir))).await;
	assert!(matches!(outcome, FoldOutcome::NotFoldable(NotFoldable::NotYet)), "{outcome:?}");
}

/// A runner that answers the same thing for every value.
struct Fixed(fn() -> FoldRun);

impl FoldRunner for Fixed {
	fn run<'a>(
		&'a self,
		_ir: &'a Ir,
		_registry: &'a FunctionRegistry,
		_value: ValueId,
		_capabilities: &'a CapabilityPolicy,
	) -> Pin<Box<dyn Future<Output = FoldRun> + Send + 'a>> {
		Box::pin(ready((self.0)()))
	}
}

static SMALL: Fixed = Fixed(|| FoldRun::Value(Value::from(7i64), 2));
static TOO_BIG: Fixed =
	Fixed(|| FoldRun::Value(Value::String("a".repeat(FOLD_MAX_BYTES + 1).as_str().into()), 0));
static RAISES: Fixed = Fixed(|| {
	FoldRun::Raised(FoldedError {
		text: "boom".to_owned(),
	})
});
static REFUSES: Fixed = Fixed(|| FoldRun::Refused(NotFoldable::FuelExhausted));

/// A runner's answer maps to an outcome with no premise: a value within the size bound is a
/// constant that keeps its depth, a larger one is too large, a raise stays a raise and a refusal
/// is the reason it gave.
#[tokio::test]
async fn outcome_mapping() {
	let env = Env::new();
	let ir = bind("RETURN 1 + 2").await;
	let req = request(first_return_operand(&ir));

	let folder = VmFolder::new(CapabilityPolicy::AllowAll, &SMALL);
	match folder.fold(&ir, &env.cx(), req).await {
		FoldOutcome::Constant {
			value,
			premises,
			depth,
		} => {
			assert_eq!(value, Value::from(7i64));
			assert_eq!(premises, PremiseSet::new());
			assert_eq!(depth, 2);
		}
		other @ (FoldOutcome::Raises {
			..
		}
		| FoldOutcome::NotFoldable(_)) => {
			panic!("a small value is not a constant: {other:?}")
		}
	}

	let folder = VmFolder::new(CapabilityPolicy::AllowAll, &TOO_BIG);
	assert!(matches!(
		folder.fold(&ir, &env.cx(), req).await,
		FoldOutcome::NotFoldable(NotFoldable::TooLarge)
	));

	let folder = VmFolder::new(CapabilityPolicy::AllowAll, &RAISES);
	match folder.fold(&ir, &env.cx(), req).await {
		FoldOutcome::Raises {
			error,
			premises,
			depth,
		} => {
			assert_eq!(error.text, "boom");
			assert_eq!(premises, PremiseSet::new());
			assert_eq!(depth, 0);
		}
		other @ (FoldOutcome::Constant {
			..
		}
		| FoldOutcome::NotFoldable(_)) => {
			panic!("a raise is not a raise: {other:?}")
		}
	}

	let folder = VmFolder::new(CapabilityPolicy::AllowAll, &REFUSES);
	assert!(matches!(
		folder.fold(&ir, &env.cx(), req).await,
		FoldOutcome::NotFoldable(NotFoldable::FuelExhausted)
	));
}

/// A value eligibility refuses never reaches the runner.
#[tokio::test]
async fn an_ineligible_value_is_not_run() {
	let env = Env::new();
	let ir = bind("RETURN rand::float()").await;
	let folder = VmFolder::new(CapabilityPolicy::AllowAll, &SMALL);
	let outcome = folder.fold(&ir, &env.cx(), request(first_return_operand(&ir))).await;
	assert!(matches!(outcome, FoldOutcome::NotFoldable(NotFoldable::Effect(_))), "{outcome:?}");
}

/// The operand of every `Return` of `ir`, in program order.
fn return_operands(ir: &Ir) -> Vec<ValueId> {
	all_ops(ir)
		.into_iter()
		.filter_map(|op| {
			let data = ir.op(op)?;
			(data.kind.tag() == OpTag::Logical(LogTag::Return)).then(|| data.operands[0])
		})
		.collect()
}

/// Folds the values the `RETURN`s of `sql` return through the real host, once as a batch and once
/// request by request, and answers both.
async fn fold_both(sql: &str) -> (Vec<FoldOutcome>, Vec<FoldOutcome>) {
	let env = Env::new();
	let ir = bind(sql).await;
	let reqs: Vec<FoldRequest> = return_operands(&ir).into_iter().map(request).collect();
	let folder = VmFolder::new(CapabilityPolicy::AllowAll, &HOST_RUNNER);
	let batch = folder.fold_all(&ir, &env.cx(), &reqs).await;
	let mut alone = Vec::new();
	for req in &reqs {
		alone.push(folder.fold(&ir, &env.cx(), *req).await);
	}
	(batch, alone)
}

/// Whether two outcomes are the same answer.
fn same(a: &FoldOutcome, b: &FoldOutcome) -> bool {
	format!("{a:?}") == format!("{b:?}")
}

fn assert_batch_equals_alone(batch: &[FoldOutcome], alone: &[FoldOutcome]) {
	assert_eq!(batch.len(), alone.len());
	for (at, (b, a)) in batch.iter().zip(alone).enumerate() {
		assert!(same(b, a), "request {at}: batch {b:?}, alone {a:?}");
	}
}

/// A value that raises between two that fold leaves the outer two folded and is itself a raise.
#[tokio::test]
async fn batch_with_a_raising_middle_value() {
	let (batch, alone) =
		fold_both("RETURN 1 + 2; RETURN array::repeat(0, 100000000); RETURN 3 + 4").await;
	assert_batch_equals_alone(&batch, &alone);
	assert!(
		matches!(&batch[0], FoldOutcome::Constant { value, .. } if *value == Value::from(3i64))
	);
	assert!(matches!(&batch[1], FoldOutcome::Raises { .. }), "{:?}", batch[1]);
	assert!(
		matches!(&batch[2], FoldOutcome::Constant { value, .. } if *value == Value::from(7i64))
	);
}

/// Two raising values in one batch each keep the raise they get alone.
#[tokio::test]
async fn batch_with_two_raising_values() {
	let (batch, alone) = fold_both(
		"RETURN array::repeat(0, 100000000); RETURN 1 + 2; \
		 RETURN array::repeat(1, 200000000); RETURN 5",
	)
	.await;
	assert_batch_equals_alone(&batch, &alone);
	for at in [0, 2] {
		assert!(matches!(&batch[at], FoldOutcome::Raises { .. }), "{:?}", batch[at]);
	}
	assert!(matches!(&batch[1], FoldOutcome::Constant { .. }));
}

/// A value that exhausts its fuel does not take the fuel of the values after it.
#[tokio::test]
async fn batch_with_a_fuel_exhausting_value() {
	// Each term charges its array twice, as a result and as an argument, and the sum of eight is
	// past the fold budget.
	let term = "array::len(array::repeat(NONE, 10000))";
	let hungry = [term; 8].join(" + ");
	let (batch, alone) =
		fold_both(&format!("RETURN 1 + 2; RETURN {hungry}; RETURN 3 + 4; RETURN math::abs(-3)"))
			.await;
	assert_batch_equals_alone(&batch, &alone);
	assert!(
		matches!(&batch[1], FoldOutcome::NotFoldable(NotFoldable::FuelExhausted)),
		"{:?}",
		batch[1]
	);
	for at in [0, 2, 3] {
		assert!(matches!(&batch[at], FoldOutcome::Constant { .. }), "{:?}", batch[at]);
	}
}

/// Values judged ineligible, folded and raising mix in one batch in request order.
#[tokio::test]
async fn batch_equals_per_request_over_mixed_values() {
	let (batch, alone) = fold_both(
		"RETURN 1; RETURN rand::float(); RETURN [1, 2] + [3]; RETURN time::now(); \
		 RETURN math::abs(-3); RETURN array::repeat(0, 100000000); RETURN 'a' + 'b'",
	)
	.await;
	assert_batch_equals_alone(&batch, &alone);
}
