//! Tests of the fold pass: branch choice, the literal `IF` collapse, the algebraic rules and the
//! rules a type fact makes sound.

use super::driver::{LoPass, PassConfig, PassReport, run_passes};
use super::fold::{AlgebraicRules, FactRules, FoldPass};
use super::tests_support::{
	Env, RecordingObserver, ScriptedFolder, bind, create, empty_ir, kind_of, ops_of_tag, spec,
	verify_logical,
};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::pattern::{GREEDY_DEFAULT, PatternSet, apply_greedy};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
#[cfg(feature = "egraph")]
use crate::exec::ir::egraph::rules_logical::CATALOGUE;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag, FoldedError};
use crate::exec::ir::iface::conversion::ExprDialect;
use crate::exec::ir::iface::fold::{FoldConsumer, FoldOutcome, NotYetFolder};
use crate::exec::ir::iface::premise::PremiseSet;
use crate::exec::ir::iface::types::{FactSource, TypeKnowledge};
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::{BinaryOperator, Kind};
use crate::val::Value;

const SHORT_CIRCUIT: OpTag = OpTag::Expr(ExprTag::ShortCircuit);

fn constant(value: Value) -> FoldOutcome {
	FoldOutcome::Constant {
		value,
		premises: PremiseSet::new(),
		depth: 0,
	}
}

/// How many `IF` ops, in either dialect, `ir` holds.
fn ifs(ir: &Ir) -> usize {
	ops_of_tag(ir, OpTag::Expr(ExprTag::If)).len()
		+ ops_of_tag(ir, OpTag::Logical(LogTag::If)).len()
}

/// How many `expr.binary` ops of `operator` `ir` holds.
fn binaries(ir: &Ir, operator: &BinaryOperator) -> usize {
	ops_of_tag(ir, OpTag::Expr(ExprTag::Binary))
		.into_iter()
		.filter(
			|op| matches!(kind_of(ir, *op), OpKind::Expr(ExprOp::Binary { op: held }) if held == operator),
		)
		.count()
}

/// Whether a literal of `ir` prints with `text` in it.
fn has_literal(ir: &Ir, text: &str) -> bool {
	ops_of_tag(ir, OpTag::Expr(ExprTag::Literal))
		.into_iter()
		.any(|op| format!("{:?}", kind_of(ir, op)).contains(text))
}

/// Runs the fold pass over `ir` under the release configuration.
async fn fold(
	ir: &mut Ir,
	folder: &ScriptedFolder,
	observer: &mut RecordingObserver,
) -> PassReport {
	let env = Env::new();
	let cx = env.cx();
	let mut facts = FactCache::new();
	let passes: Vec<Box<dyn LoPass>> = vec![Box::new(FoldPass)];
	let reports = run_passes(&passes, ir, &mut facts, folder, &cx, &PassConfig::RELEASE, observer)
		.await
		.expect("the pass runs");
	reports.into_iter().next().expect("one pass ran").1
}

/// A folded condition keeps the arm its truthiness selects and drops the other, and the observer
/// hears of the value while its defining op is still in the container.
#[tokio::test]
async fn branch_choice_keeps_the_taken_arm() {
	let mut ir = bind("RETURN IF 1 + 1 = 2 { 'yes' } ELSE { 'no' }").await;
	assert_eq!(ifs(&ir), 1);
	let folder = ScriptedFolder::new().on("Equal", constant(Value::Bool(true)));
	let mut observer = RecordingObserver::default();
	let report = fold(&mut ir, &folder, &mut observer).await;
	assert!(report.rewrites >= 1);
	assert_eq!(ifs(&ir), 0);
	assert!(has_literal(&ir, "yes"));
	assert!(!has_literal(&ir, "no"));
	assert_eq!(observer.seen.len(), 1);
	assert_eq!(observer.seen[0].1, FoldConsumer::BranchChoice);
	assert_eq!(observer.defined, [true]);

	let mut ir = bind("RETURN IF 1 + 1 = 2 { 'yes' } ELSE { 'no' }").await;
	let folder = ScriptedFolder::new().on("Equal", constant(Value::Bool(false)));
	fold(&mut ir, &folder, &mut RecordingObserver::default()).await;
	assert_eq!(ifs(&ir), 0);
	assert!(has_literal(&ir, "no"));
	assert!(!has_literal(&ir, "yes"));
}

/// A literal condition needs no fold: the folder is never asked and the observer is never told.
#[tokio::test]
async fn literal_if_collapses_without_a_fold() {
	for (sql, kept, dropped) in [
		("RETURN IF true { 'yes' } ELSE { 'no' }", "yes", "no"),
		("RETURN IF 0 { 'yes' } ELSE { 'no' }", "no", "yes"),
	] {
		let mut ir = bind(sql).await;
		let folder = ScriptedFolder::new();
		let mut observer = RecordingObserver::default();
		fold(&mut ir, &folder, &mut observer).await;
		assert_eq!(ifs(&ir), 0, "`{sql}`");
		assert!(has_literal(&ir, kept), "`{sql}`");
		assert!(!has_literal(&ir, dropped), "`{sql}`");
		assert!(folder.asked().is_empty(), "`{sql}`");
		assert!(observer.seen.is_empty(), "`{sql}`");
	}
}

/// A fold that raises is not applied: the `IF` stays and raises at run time where it stands.
#[tokio::test]
async fn raises_is_not_applied() {
	let mut ir = bind("RETURN IF 1 + 1 = 2 { 'yes' } ELSE { 'no' }").await;
	let raises = FoldOutcome::Raises {
		error: FoldedError {
			text: "boom".to_owned(),
		},
		premises: PremiseSet::new(),
		depth: 0,
	};
	let folder = ScriptedFolder::new().on("Equal", raises);
	let mut observer = RecordingObserver::default();
	fold(&mut ir, &folder, &mut observer).await;
	assert_eq!(ifs(&ir), 1);
	assert!(has_literal(&ir, "yes") && has_literal(&ir, "no"));
	assert_eq!(folder.asked().len(), 1);
	assert!(observer.seen.is_empty());
}

/// Each decision that consumes a folded value is reported once, in program order.
#[tokio::test]
async fn observer_sees_each_consumed_fold() {
	let mut ir =
		bind("RETURN [IF 1 + 1 = 2 { 1 } ELSE { 2 }, IF 2 + 2 = 4 { 3 } ELSE { 4 }]").await;
	assert_eq!(ifs(&ir), 2);
	let folder = ScriptedFolder::new().on("Equal", constant(Value::Bool(true)));
	let mut observer = RecordingObserver::default();
	fold(&mut ir, &folder, &mut observer).await;
	assert_eq!(ifs(&ir), 0);
	assert_eq!(observer.seen.len(), 2);
	assert!(observer.seen.iter().all(|(_, consumer)| *consumer == FoldConsumer::BranchChoice));
	assert_eq!(observer.defined, [true, true]);
}

/// An arm holding a `BREAK` that leaves it is not collapsed, because inlining it would move the
/// place the `BREAK` lands. A value arm catches a `RETURN` written in it, so an arm whose `RETURN`
/// lands in a nested arm is collapsed.
#[tokio::test]
async fn an_arm_with_escaping_control_keeps_its_if() {
	let mut ir = bind("FOR $i IN [1, 2] { IF true { IF $p { BREAK }; 4 } }").await;
	let before = ifs(&ir);
	assert_eq!(before, 2);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ifs(&ir), before, "the `BREAK` lands on the loop, beyond the arm");

	let mut ir = bind("LET $r = IF true { IF $p { RETURN 3 }; 4 }; RETURN $r").await;
	assert_eq!(ifs(&ir), 2);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ifs(&ir), 1, "the `RETURN` lands on the nested arm, inside the collapsed one");
}

/// A conjunct that folds to `true` leaves the `AND` chain and one that folds to `false` stays as
/// a literal `false`.
#[tokio::test]
async fn folded_conjuncts_leave_or_stay_in_the_chain() {
	let mut ir = bind("SELECT * FROM t WHERE a = 1 AND 1 < 2").await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 1);
	let folder = ScriptedFolder::new().on("LessThan", constant(Value::Bool(true)));
	let mut observer = RecordingObserver::default();
	fold(&mut ir, &folder, &mut observer).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 0);
	assert_eq!(binaries(&ir, &BinaryOperator::LessThan), 0);
	assert_eq!(binaries(&ir, &BinaryOperator::Equal), 1);
	assert_eq!(observer.seen.len(), 1);
	assert_eq!(observer.defined, [true]);

	let mut ir = bind("SELECT * FROM t WHERE a = 1 AND 1 > 2").await;
	let folder = ScriptedFolder::new().on("MoreThan", constant(Value::Bool(false)));
	fold(&mut ir, &folder, &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 1);
	assert_eq!(binaries(&ir, &BinaryOperator::MoreThan), 0);
	assert!(has_literal(&ir, "false"));
}

/// No rule in `AlgebraicRules` is also in the e-graph's `CATALOGUE`.
#[tokio::test]
async fn algebraic_rows_not_in_l6() {
	let names: Vec<&str> = AlgebraicRules::patterns().iter().map(|rule| rule.name()).collect();
	assert_eq!(
		names,
		[
			"false_and_lazy",
			"true_or_lazy",
			"true_and_lazy",
			"false_or_lazy",
			"and_true",
			"or_false",
			"and_false",
			"or_true",
			"inside_empty",
			"not_inside_empty"
		]
	);
	#[cfg(feature = "egraph")]
	for held in CATALOGUE {
		assert!(!names.contains(&held.name), "{} is held by the catalogue", held.name);
	}

	let mut ir = bind("RETURN [false AND $p, true OR $p]").await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 2);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 0);

	let mut ir = bind("RETURN [1 INSIDE [], 1 NOTINSIDE []]").await;
	assert_eq!(binaries(&ir, &BinaryOperator::Inside), 1);
	assert_eq!(binaries(&ir, &BinaryOperator::NotInside), 1);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(binaries(&ir, &BinaryOperator::Inside), 0);
	assert_eq!(binaries(&ir, &BinaryOperator::NotInside), 0);
}

/// `x AND true` and `x OR false` are `x` only where the result is read for its truthiness or `x`
/// is a boolean: in value position `0 AND true` is `0` and `'' OR false` is `false`.
#[tokio::test]
async fn x_and_true_needs_a_boolean_consumer() {
	let mut ir = bind("RETURN [0 AND true, '' OR false]").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 2);

	let mut ir = bind("SELECT * FROM t WHERE (1 < 2) AND true").await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 1);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 0);
	assert_eq!(binaries(&ir, &BinaryOperator::LessThan), 1);
}

/// A neutral literal is dropped wherever it is written: on the left of a gate, on the right of one
/// over a plain row read, and among the conjuncts of a filter. The conjunct it leaves is evaluated
/// once, as it was.
#[tokio::test]
async fn a_written_neutral_literal_is_dropped() {
	let mut ir = bind("RETURN [true AND $p, false OR $p]").await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 2);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 0);

	let mut ir = bind("SELECT * FROM t WHERE a = 'x' OR false").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 0);

	let mut ir = bind("SELECT * FROM t WHERE true AND a = 1 AND true AND b = 2").await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 3);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 1);
	assert_eq!(binaries(&ir, &BinaryOperator::Equal), 2);
}

/// `x AND false` is the literal `false` where `x` is read for its truthiness and can neither raise
/// nor differ between two evaluations, and the literal carries no text of the expression it
/// replaced.
#[tokio::test]
async fn a_decided_literal_carries_no_text() {
	let mut ir = bind("SELECT * FROM t WHERE 1 = 1 AND false").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 0);
	let literal = ops_of_tag(&ir, OpTag::Expr(ExprTag::Literal))
		.into_iter()
		.find(|op| format!("{:?}", kind_of(&ir, *op)).contains("false"))
		.expect("the decided literal stands");
	assert_eq!(ir.op(literal).and_then(|data| data.loc.text()), None);
}

/// A cast of a value already of the cast's kind is the value, and so is the truthiness of a
/// boolean.
#[tokio::test]
async fn fact_rules_erase_convert_and_truthy() {
	let mut ir = bind("RETURN <int> 5").await;
	assert_eq!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Convert)).len(), 1);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Convert)).len(), 0);
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	// A literal carries the kind the binder gives it, a fact sourced from the literal.
	let mut truth = spec(OpKind::Expr(ExprOp::Literal(Box::new(Value::Bool(true)))), &[], 1);
	truth.results[0] = TypeKnowledge::Fact {
		kind: Kind::Bool,
		source: FactSource::Literal,
	};
	let literal = rw.create(InsertPoint::End(block), truth).expect("the op is created");
	let held = rw.ir().op(literal).expect("the op exists").results.clone();
	let truthy = OpKind::Expr(ExprOp::Truthy);
	let (_, coerced) = create(&mut rw, InsertPoint::End(block), truthy, &[held[0]], 1);
	let add = OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Add,
	});
	let (user, _) = create(&mut rw, InsertPoint::End(block), add, &[coerced[0], coerced[0]], 1);

	let mut set = PatternSet::<ExprDialect>::new();
	for rule in FactRules::patterns() {
		set.add(rule);
	}
	let report = apply_greedy(
		&mut ir,
		&mut facts,
		&set,
		&NotYetFolder,
		crate::exec::ir::analysis::cx::builtin_registry(),
		GREEDY_DEFAULT,
	)
	.await
	.expect("the rules apply");
	assert_eq!(report.rewrites, 1);
	assert!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Truthy)).is_empty());
	assert_eq!(ir.op(user).expect("the user stays").operands.as_slice(), [held[0], held[0]]);
}

/// A clock read in a `WHERE` container is never folded. The binder keeps `time::now()` a call, and
/// the fold host refuses any value that reads the clock, so the call stays in the container after
/// the fold pass runs with the real folder: the clock is read when the statement runs, never baked
/// into the plan as a literal.
#[tokio::test]
async fn a_clock_read_in_a_where_container_stays_a_call() {
	use crate::exec::ir::fold::evaluate::{BUILTIN_FOLDER, eligible};
	use crate::exec::ir::iface::fold::NotFoldable;

	let mut ir = bind("SELECT * FROM t WHERE at > time::now() - 1d").await;
	let clock_calls = |ir: &Ir| {
		ops_of_tag(ir, OpTag::Expr(ExprTag::Call))
			.into_iter()
			.filter(|op| {
				matches!(kind_of(ir, *op), OpKind::Expr(ExprOp::Call(call))
					if call.function == crate::expr::Function::Normal("time::now".to_owned()))
			})
			.count()
	};
	assert_eq!(clock_calls(&ir), 1, "the binder keeps the clock read a call");

	let env = Env::new();
	let cx = env.cx();
	let mut facts = FactCache::new();
	let passes: Vec<Box<dyn LoPass>> = vec![Box::new(FoldPass)];
	run_passes(
		&passes,
		&mut ir,
		&mut facts,
		&BUILTIN_FOLDER,
		&cx,
		&PassConfig::RELEASE,
		&mut RecordingObserver::default(),
	)
	.await
	.expect("the pass runs");
	assert_eq!(clock_calls(&ir), 1, "the fold pass left the clock read in place");
	assert!(!has_literal(&ir, "Datetime"), "no instant is baked into the container");

	// The value `time::now() - 1d` computes is refused for the clock.
	let subtract = ops_of_tag(&ir, OpTag::Expr(ExprTag::Binary))
		.into_iter()
		.find(|op| {
			matches!(
				kind_of(&ir, *op),
				OpKind::Expr(ExprOp::Binary {
					op: BinaryOperator::Subtract
				})
			)
		})
		.expect("the subtraction");
	let value = ir.op(subtract).expect("the op exists").results[0];
	assert_eq!(eligible(&ir, &mut FactCache::new(), &cx, value), Err(NotFoldable::Clock));
}

/// A statement `IF` is replaced by what the binder emits for the statement it selects: an `eval` of
/// `NONE` when no arm is left and there is no `ELSE`, and the selected arm as a block of its own.
#[tokio::test]
async fn a_statement_if_collapses_to_the_statement_it_selects() {
	let mut ir = bind("IF false { 'yes' };").await;
	assert_eq!(ifs(&ir), 1);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ifs(&ir), 0);
	assert_eq!(ops_of_tag(&ir, OpTag::Logical(LogTag::Eval)).len(), 1);
	assert!(ops_of_tag(&ir, OpTag::Logical(LogTag::Block)).is_empty());
	assert!(has_literal(&ir, "None"));
	assert!(!has_literal(&ir, "yes"));
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let mut ir = bind("IF true { 'yes' };").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ifs(&ir), 0);
	assert_eq!(ops_of_tag(&ir, OpTag::Logical(LogTag::Block)).len(), 1);
	assert!(has_literal(&ir, "yes"));
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let mut ir = bind("IF false { 'arm-a' } ELSE { 'arm-b' };").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, OpTag::Logical(LogTag::Block)).len(), 1);
	assert!(has_literal(&ir, "arm-b") && !has_literal(&ir, "arm-a"));
}

/// A filter whose conjuncts are all the literal `true` admits every row and is erased; one that
/// yields `false` stays, and so does one whose `true` a fold decided.
#[tokio::test]
async fn a_filter_that_admits_every_row_is_erased() {
	let filters = |ir: &Ir| ops_of_tag(ir, OpTag::Logical(LogTag::Filter)).len();
	let env = Env::new();

	let mut ir = bind("SELECT * FROM t WHERE true").await;
	assert_eq!(filters(&ir), 1);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(filters(&ir), 0);
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let mut ir = bind("SELECT * FROM t WHERE true OR a = 1").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(filters(&ir), 0);

	let mut ir = bind("SELECT * FROM t WHERE false").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(filters(&ir), 1);

	let mut ir = bind("SELECT * FROM t WHERE 1 < 2").await;
	let folder = ScriptedFolder::new().on("LessThan", constant(Value::Bool(true)));
	fold(&mut ir, &folder, &mut RecordingObserver::default()).await;
	assert_eq!(filters(&ir), 1);
	assert!(has_literal(&ir, "true"));
}

/// A broadcast over a value that is no collection is its body, with the value as the element. A
/// broadcast over a value the type facts say nothing of stays a loop.
#[tokio::test]
async fn a_broadcast_over_a_value_that_is_no_collection_is_its_body() {
	let loops = |ir: &Ir| ops_of_tag(ir, OpTag::Logical(LogTag::Loop)).len();
	let env = Env::new();

	let mut ir = bind("RETURN user:alice.principals[1]").await;
	assert_eq!(loops(&ir), 1);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(loops(&ir), 0);
	assert_eq!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Index)).len(), 1);
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let mut ir = bind("RETURN { a: 1 }.a").await;
	let before = loops(&ir);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert!(loops(&ir) <= before);
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let mut ir = bind("RETURN $p.principals[1]").await;
	let before = loops(&ir);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(loops(&ir), before, "a free parameter may hold an array");
}

/// A comparison operand that folds to a constant becomes a literal an access path reads, and one
/// that builds a range does not: the access path reads the bounds it was written with.
#[tokio::test]
async fn a_folded_comparison_operand_becomes_a_literal() {
	let mut ir = bind("SELECT * FROM t WHERE a = 1 + 1").await;
	assert_eq!(binaries(&ir, &BinaryOperator::Add), 1);
	let folder = ScriptedFolder::new().on("Add", constant(Value::from(2_i64)));
	let mut observer = RecordingObserver::default();
	fold(&mut ir, &folder, &mut observer).await;
	assert_eq!(binaries(&ir, &BinaryOperator::Add), 0);
	assert!(has_literal(&ir, "2"));
	assert_eq!(binaries(&ir, &BinaryOperator::Equal), 1);
	assert!(observer.seen.iter().all(|(_, consumer)| *consumer == FoldConsumer::IndexChoice));
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let mut ir = bind("SELECT * FROM t WHERE a INSIDE 1..=2").await;
	let folder = ScriptedFolder::new().on("RangeInclusive", constant(Value::from(2_i64)));
	fold(&mut ir, &folder, &mut RecordingObserver::default()).await;
	assert_eq!(binaries(&ir, &BinaryOperator::RangeInclusive), 1);

	// A fold that raises is not applied.
	let mut ir = bind("SELECT * FROM t WHERE a = 1 + 1").await;
	let raises = FoldOutcome::Raises {
		error: FoldedError {
			text: "boom".to_owned(),
		},
		premises: PremiseSet::new(),
		depth: 0,
	};
	fold(&mut ir, &ScriptedFolder::new().on("Add", raises), &mut RecordingObserver::default())
		.await;
	assert_eq!(binaries(&ir, &BinaryOperator::Add), 1);
}

/// `x AND false` is `false` and `x OR true` is `true` where `x` is a boolean that evaluating
/// discards unobserved; a `x` that may be any value, in a position that reads it, stays.
#[tokio::test]
async fn the_literal_on_the_right_decides_a_boolean_left() {
	let mut ir = bind("SELECT * FROM t WHERE 1 = 1 AND false").await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 1);
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 0);
	assert_eq!(ops_of_tag(&ir, OpTag::Logical(LogTag::Filter)).len(), 1, "a `false` filter stays");
	assert!(has_literal(&ir, "false"));

	let mut ir = bind("SELECT * FROM t WHERE 1 = 2 OR true").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 0);
	assert!(ops_of_tag(&ir, OpTag::Logical(LogTag::Filter)).is_empty());

	let mut ir = bind("RETURN [$p AND false]").await;
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert_eq!(ops_of_tag(&ir, SHORT_CIRCUIT).len(), 1, "a value is not read for its truthiness");
}
