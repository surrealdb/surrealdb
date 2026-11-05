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

/// An arm holding a `BREAK` or a `RETURN` that leaves it is not collapsed, because inlining it
/// would move the place the signal lands. No arm is a context, so a `RETURN` in a nested arm leaves
/// both.
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
	assert_eq!(ifs(&ir), 2, "the `RETURN` lands on the statement, beyond the arm");
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

/// A comparison operand that is already the record-id op its constant would be rebuilt as is
/// folded once and left as it stands: the pass settles without reaching its round bound.
#[tokio::test]
async fn a_record_id_operand_is_folded_once_and_left_in_place() {
	let mut ir = bind("SELECT * FROM t WHERE id = t:1").await;
	let id = crate::val::RecordId::new(
		surrealdb_strand::TableName::new(surrealdb_strand::Strand::new("t")),
		1_i64,
	);
	let folder = ScriptedFolder::new().on("RecordId(RecordIdAttrs", constant(Value::RecordId(id)));
	let report = fold(&mut ir, &folder, &mut RecordingObserver::default()).await;
	let record_ids = |ir: &Ir| ops_of_tag(ir, OpTag::Expr(ExprTag::RecordId)).len();
	assert_eq!(record_ids(&ir), 1);
	assert_eq!(report.stopped_by, None);
	let asked = folder
		.asked()
		.iter()
		.filter(|req| {
			ir.value(req.value).is_none_or(|data| {
				matches!(data.def, crate::exec::ir::core::value::ValueDef::OpResult { op, .. }
					if ir.op(op).is_some_and(|op| matches!(op.kind, OpKind::Expr(ExprOp::RecordId(_)))))
			})
		})
		.count();
	assert_eq!(asked, 1, "the record-id operand is folded once");
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
}

/// The shape `WHERE id = $id`: a parameter operand is replaced by the record-id op its value is
/// built as in the first round, and the second round leaves that op as it stands, so the pass
/// settles with the record-id op folded once.
#[tokio::test]
async fn a_param_operand_folded_to_a_record_id_is_left_in_place() {
	let mut ir = bind("SELECT * FROM t WHERE id = $id").await;
	let id = crate::val::RecordId::new(
		surrealdb_strand::TableName::new(surrealdb_strand::Strand::new("t")),
		1_i64,
	);
	let folder = ScriptedFolder::new()
		.on("FreeParam", constant(Value::RecordId(id.clone())))
		.on("RecordId(RecordIdAttrs", constant(Value::RecordId(id)));
	let report = fold(&mut ir, &folder, &mut RecordingObserver::default()).await;
	assert_eq!(report.stopped_by, None);
	assert_eq!(ops_of_tag(&ir, OpTag::Expr(ExprTag::RecordId)).len(), 1);
	assert!(ops_of_tag(&ir, OpTag::Expr(ExprTag::FreeParam)).is_empty());
	let asked_of = |tag: ExprTag| {
		folder
			.asked()
			.iter()
			.filter(|req| {
				ir.value(req.value).is_some_and(|data| {
					matches!(data.def, crate::exec::ir::core::value::ValueDef::OpResult { op, .. }
						if ir.op(op).is_some_and(|op| op.kind.tag() == OpTag::Expr(tag)))
				})
			})
			.count()
	};
	assert_eq!(asked_of(ExprTag::RecordId), 1, "the record-id op is folded once");
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
}

/// Replacing an op inside an `ErrorScope` with a value defined outside that `ErrorScope` across
/// `LiteralLeft`, `NeutralLeft`, `NeutralRight`, `ConvertOfFact` and `TruthyOfFact` erases the op
/// without recording a cross-`ErrorScope` `MergedInto` provenance link, so `verify_logical` passes.
#[tokio::test]
async fn cross_error_scope_fold_rules_preserve_provenance() {
	use crate::exec::ir::expr::ops::{
		ConvertAttrs, ConvertMode, ConvertSite, OnFail, ShortCircuitOp,
	};
	use crate::exec::ir::iface::conversion::EliminationReason;
	use crate::exec::ir::iface::ids::ValueId;
	use crate::exec::ir::iface::region_kind::RegionKind;

	let mut ir = bind("SELECT count(1) FROM [1] GROUP ALL").await;
	let agg = ops_of_tag(&ir, OpTag::Expr(ExprTag::Agg))[0];
	let scope_region = ir.op(agg).expect("the agg exists").regions[0];
	let scope_block = ir.region(scope_region).expect("the error scope exists").blocks[0];
	let yield_op = *ir.block(scope_block).expect("the block exists").ops.last().expect("a yield");
	let old_val = ir.op(yield_op).expect("the yield exists").operands[0];
	let old_lit = match ir.value(old_val).expect("the value exists").def {
		crate::exec::ir::core::value::ValueDef::OpResult {
			op,
			..
		} => op,
		crate::exec::ir::core::value::ValueDef::BlockArg {
			..
		} => panic!("expected an op result"),
	};

	let bool_fact = TypeKnowledge::Fact {
		kind: Kind::Bool,
		source: FactSource::Literal,
	};
	let make_bool = |rw: &mut Rewriter<'_>, at: InsertPoint, b: bool| -> ValueId {
		let mut s = spec(OpKind::Expr(ExprOp::Literal(Box::new(Value::Bool(b)))), &[], 1);
		s.results[0] = bool_fact.clone();
		let op = rw.create(at, s).expect("the bool literal is created");
		rw.ir().op(op).expect("the op exists").results[0]
	};
	let make_and = |rw: &mut Rewriter<'_>, at: InsertPoint, left: ValueId, right: ValueId| {
		let mut s = spec(
			OpKind::Expr(ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			}),
			&[left],
			1,
		);
		s.results[0] = bool_fact.clone();
		let gate = rw.create(at, s).expect("the short-circuit op is created");
		let (_, lazy_block) =
			rw.create_region(gate, RegionKind::LazyOperand, &[]).expect("the lazy region");
		create(rw, InsertPoint::End(lazy_block), OpKind::Expr(ExprOp::Yield), &[right], 0);
		rw.ir().op(gate).expect("the op exists").results[0]
	};

	let mut facts = FactCache::new();
	{
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let outer_at = InsertPoint::Before(agg);
		let inner_at = InsertPoint::Before(yield_op);
		let outer_false = make_bool(&mut rw, outer_at, false);
		let outer_true = make_bool(&mut rw, outer_at, true);
		let inner_true_a = make_bool(&mut rw, inner_at, true);
		let inner_true_b = make_bool(&mut rw, inner_at, true);

		// 1. LiteralLeft (`false_and_lazy`): left is defined outside the ErrorScope.
		let r1 = make_and(&mut rw, inner_at, outer_false, inner_true_a);
		// 2. NeutralLeft (`true_and_lazy`): yielded right is defined outside the ErrorScope.
		let r2 = make_and(&mut rw, inner_at, inner_true_a, outer_true);
		// 3. NeutralRight (`and_true`): left is defined outside the ErrorScope.
		let r3 = make_and(&mut rw, inner_at, outer_true, inner_true_b);
		// 4. ConvertOfFact (`convert_of_fact`): operand is defined outside the ErrorScope.
		let convert_kind = OpKind::Expr(ExprOp::Convert(Box::new(ConvertAttrs {
			mode: ConvertMode::Cast,
			kind: Kind::Bool,
			site: ConvertSite(0),
			on_fail: OnFail::Error,
		})));
		let (_, r4) = create(&mut rw, inner_at, convert_kind, &[outer_true], 1);
		// 5. TruthyOfFact (`truthy_of_fact`): operand is defined outside the ErrorScope.
		let (_, r5) = create(&mut rw, inner_at, OpKind::Expr(ExprOp::Truthy), &[outer_true], 1);

		let (_, arr) =
			create(&mut rw, inner_at, OpKind::Expr(ExprOp::Array), &[r1, r2, r3, r4[0], r5[0]], 1);
		rw.replace_all_uses(old_val, arr[0]).expect("the yield reads the array");
		rw.erase(old_lit, EliminationReason::DeadPure).expect("the old literal is erased");
	}

	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the initial container verifies");
	fold(&mut ir, &ScriptedFolder::new(), &mut RecordingObserver::default()).await;
	assert!(ops_of_tag(&ir, SHORT_CIRCUIT).is_empty());
	assert!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Convert)).is_empty());
	assert!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Truthy)).is_empty());
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the folded container verifies");
}

/// A statement `IF` whose taken arm has a token block argument that cannot be spliced by
/// `inline_region` is refused before any block op is created or erased.
#[tokio::test]
async fn non_collapsible_statement_if_leaves_container_untouched() {
	use crate::exec::ir::iface::types::TokenClass;

	let mut ir = bind("IF true { 'yes' };").await;
	let if_op = ops_of_tag(&ir, OpTag::Logical(LogTag::If))[0];
	let taken = ir.op(if_op).expect("the if exists").regions[0];
	let taken_block = ir.region(taken).expect("the taken region exists").blocks[0];
	let eval_op = *ir.block(taken_block).expect("the block exists").ops.last().expect("an eval");

	let mut facts = FactCache::new();
	{
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let tok = rw.add_token_arg(taken_block, TokenClass::Store).expect("the token arg is added");
		// `eval_op` reads `tok`, while the enclosing `IF` holds no `Store` token operand of its
		// own, so `tokens_collapsible` refuses the arm upfront without failing the pass.
		rw.add_token_operand(eval_op, tok).expect("the token operand is added");
	}

	let blocks_before = ops_of_tag(&ir, OpTag::Logical(LogTag::Block)).len();
	let env = Env::new();
	let effects = env.cx();
	let folder = ScriptedFolder::new();
	let mut observer = RecordingObserver::default();
	let mut cx = super::driver::LoCx {
		ir: &mut ir,
		facts: &mut facts,
		folder: &folder,
		effects: &effects,
		cfg: &PassConfig::RELEASE,
		observer: &mut observer,
	};
	FoldPass.run(&mut cx).await.expect("FoldPass runs without failing");
	assert_eq!(ifs(&ir), 1, "the non-collapsible IF stays in place");
	assert_eq!(ops_of_tag(&ir, OpTag::Logical(LogTag::Block)).len(), blocks_before);
}

/// Inlining an identity function call `fn::id($x) { $x }` inside an `ErrorScope` when its argument
/// is defined outside that `ErrorScope` replaces uses without violating `VerifyRule::ErrorScope`.
#[tokio::test]
async fn cross_error_scope_inline_calls_preserves_provenance() {
	use std::sync::Arc;

	use surrealdb_expr::parser::TopLevelExpr;

	use super::driver::NoObserver;
	use super::inline::InlineCalls;
	use super::tests_support::{literal, request_scope};
	use crate::catalog::{FunctionDefinition, LogicalBlockBody, LogicalPermissionBody};
	use crate::exec::ir::build::logical::bind_request;
	use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx, SemanticCatalog};
	use crate::exec::ir::iface::conversion::EliminationReason;
	use crate::exec::ir::{RequestInput, RequestStatement};
	use crate::expr::statements::DefineStatement;

	struct IdCatalog(Arc<FunctionDefinition>);

	impl SemanticCatalog for IdCatalog {
		fn function(&self, name: &str) -> CatalogAnswer<Arc<FunctionDefinition>> {
			if name == "id" {
				CatalogAnswer::Found(Arc::clone(&self.0))
			} else {
				CatalogAnswer::Unknown
			}
		}
	}

	let parsed = crate::syn::parse("DEFINE FUNCTION fn::id($x: any) { RETURN $x }")
		.expect("the definition parses");
	let Some(TopLevelExpr::Expr(crate::expr::Expr::Define(define))) =
		parsed.expressions.into_iter().next()
	else {
		panic!("expected a DEFINE FUNCTION");
	};
	let DefineStatement::Function(stmt) = *define else {
		panic!("expected a function definition");
	};
	let def = Arc::new(FunctionDefinition {
		name: stmt.name,
		args: stmt.args,
		block: stmt.block.clone(),
		body: LogicalBlockBody::of(&stmt.block),
		permissions: stmt.permissions.clone(),
		permissions_body: LogicalPermissionBody::of(&stmt.permissions),
		returns: stmt.returns,
		comment: None,
		auth_limit: crate::catalog::auth::AuthLimit::new_no_limit(),
		graphql_alias: None,
		graphql_deprecated: None,
	});

	let env = Env::new();
	let catalog = IdCatalog(def);
	let base_cx = env.cx();
	let cx = EffectCx {
		registry: base_cx.registry,
		catalog: &catalog,
		frames: base_cx.frames,
	};
	let req_parsed = crate::syn::parse("SELECT count(fn::id(1)) FROM [1] GROUP ALL")
		.expect("the request parses")
		.expressions;
	let request_statements = RequestStatement::of(&req_parsed);
	let input = RequestInput::request(&request_statements, request_scope());
	let mut ir = bind_request(&input, &cx).await.expect("the request binds");

	let agg = ops_of_tag(&ir, OpTag::Expr(ExprTag::Agg))[0];
	let call_op = ops_of_tag(&ir, OpTag::Expr(ExprTag::Call))[0];
	let old_val = ir.op(call_op).expect("the call exists").operands[0];
	let old_lit = match ir.value(old_val).expect("the value exists").def {
		crate::exec::ir::core::value::ValueDef::OpResult {
			op,
			..
		} => op,
		crate::exec::ir::core::value::ValueDef::BlockArg {
			..
		} => panic!("expected an op result"),
	};

	let mut facts = FactCache::new();
	{
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let (_, outer_val) = create(&mut rw, InsertPoint::Before(agg), literal(42), &[], 1);
		rw.replace_all_uses(old_val, outer_val[0]).expect("the call reads the outer literal");
		rw.erase(old_lit, EliminationReason::DeadPure).expect("the old literal is erased");
	}

	verify_logical(&ir, &mut FactCache::new(), &env).expect("the initial container verifies");
	let passes: Vec<Box<dyn LoPass>> = vec![Box::new(InlineCalls)];
	let reports = run_passes(
		&passes,
		&mut ir,
		&mut facts,
		&NotYetFolder,
		&cx,
		&PassConfig::ALL_ON,
		&mut NoObserver,
	)
	.await
	.expect("InlineCalls runs");
	assert_eq!(reports[0].1.rewrites, 1);
	assert!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Call)).is_empty());
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the inlined container verifies");
}
