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

/// A `LET` value is a slot, not a leaf, so it is refused. A stored function is refused because
/// its effects are unknown. A value an error-catching region evaluates is never folded.
#[tokio::test]
async fn eligible_refuses_slots_and_error_scopes() {
	assert_eq!(
		judge("LET $p = 1; RETURN $p + 1").await,
		Err(NotFoldable::NotConstantInputs),
		"a LET value is not a leaf"
	);
	assert_eq!(
		judge("RETURN fn::f()").await,
		Err(NotFoldable::HostRefused("udf")),
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
		other => panic!("a small value is not a constant: {other:?}"),
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
		other => panic!("a raise is not a raise: {other:?}"),
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
