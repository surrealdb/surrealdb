//! What every rewrite rule must not break, asserted over a corpus of statements.
//!
//! A rule rewrites what a statement *does*; it may never change what the statement
//! *means*. These are the invariants that holds to, checked for each statement in
//! [`CORPUS`] rather than for one rule in isolation — because a rule that is sound
//! alone can still be unsound after another has run, and that combination is what
//! a per-rule test cannot see.
//!
//! The corpus is the input space. A statement added here is checked against every
//! property, so a rule written later inherits the coverage without being named.

#![cfg(all(test, feature = "kv-mem"))]

use super::test_fixture::{Fixture, owner};
use super::{Rules, bind_with};
use crate::expr::logical::LogicalOp;

/// The schema every corpus statement is bound against.
const SCHEMA: &str = "
	DEFINE TABLE person SCHEMAFULL;
	DEFINE FIELD name ON person TYPE string;
	DEFINE FIELD age ON person TYPE int;
	DEFINE FIELD city ON person TYPE option<string>;
	DEFINE INDEX person_age ON person FIELDS age;
	DEFINE TABLE company SCHEMAFULL;
	DEFINE FIELD name ON company TYPE string;
	DEFINE TABLE works_at TYPE RELATION IN person OUT company;
";

/// The statements every property is checked against.
///
/// Add a shape here when a rule starts handling it; every property then covers it
/// without a test of its own.
const CORPUS: &[&str] = &[
	// sources and projection
	"SELECT * FROM person",
	"SELECT name FROM person",
	"SELECT name, age FROM person",
	"SELECT * FROM person:tobie",
	// predicates the rules are expected to rewrite
	"SELECT * FROM person WHERE age > 18",
	"SELECT * FROM person WHERE age > 18 AND age > 3",
	"SELECT * FROM person WHERE true",
	"SELECT * FROM person WHERE false",
	"SELECT * FROM person WHERE age > 18 AND city = 'SF'",
	"SELECT * FROM person WHERE age > 18 OR age < 5",
	"SELECT * FROM person WHERE NOT (age > 18 AND city = 'SF')",
	"SELECT * FROM person WHERE age > 1 + 2",
	"SELECT * FROM person WHERE city = NONE",
	// ordering, limits, grouping
	"SELECT * FROM person ORDER BY age",
	"SELECT * FROM person ORDER BY age LIMIT 10",
	"SELECT * FROM person LIMIT 10 START 5",
	"SELECT count() FROM person GROUP ALL",
	"SELECT city, count() FROM person GROUP BY city",
	// subqueries and bindings
	"SELECT * FROM person WHERE id IN (SELECT VALUE id FROM person WHERE age > 18)",
	"SELECT (SELECT VALUE name FROM company) AS names FROM person",
	// graph
	"SELECT ->works_at->company FROM person",
	"SELECT * FROM person WHERE ->works_at->company",
	// effects that must survive rewriting unchanged
	"CREATE person:tobie SET name = 'Tobie', age = 30",
	"UPDATE person:tobie SET age = 31",
	"UPDATE person:tobie SET age = age + 1",
	"DELETE person:tobie",
	"SELECT id, (CREATE log:1) AS made FROM person",
	// subqueries in a data clause: one the statement may evaluate once where
	// nothing it writes reaches `company`, and three it must keep evaluating
	// per row
	"UPDATE person SET city = (SELECT VALUE name FROM company)",
	"UPDATE person SET city = (SELECT VALUE name FROM company WHERE name = $this)",
	"UPDATE person SET city = (CREATE log:1).id",
	"UPDATE person SET city = (SELECT VALUE rand::uuid() FROM company)",
	// mock record sets, in the two positions that reach them
	"CREATE |person:3|",
	"CREATE |person:1..4|",
	"SELECT * FROM |person:3|",
	"RETURN |person:3|",
	// control flow the rules collapse
	"RETURN IF true THEN 1 ELSE 2 END",
	"RETURN IF false THEN (CREATE log:1) ELSE 2 END",
	"RETURN IF $c THEN 1 ELSE IF true THEN 2 ELSE 3 END",
	"SELECT * FROM person WHERE age > rand::int(1, 10)",
	"SELECT * FROM person WHERE age > time::now()",
];

/// Bind `sql` with and without the rules, as the owner against [`SCHEMA`].
async fn both(fixture: &Fixture, sql: &str) -> (LogicalOp, LogicalOp) {
	let lowered = || {
		let ast = crate::syn::parse(sql).expect("corpus statement parses");
		let top = ast.expressions.into_iter().next().expect("one statement");
		let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = top else {
			panic!("corpus statement is a bare expression: {sql}");
		};
		crate::expr::logical::lower(expr).expect("corpus statement lowers")
	};
	let bound = fixture.bound_as(&owner(), Vec::new()).await;
	let bx = fixture.bind_context(&bound);
	let run = bind_with(lowered(), &bx, Rules::Run).await.expect("binds with rules");
	let skip = bind_with(lowered(), &bx, Rules::Skip).await.expect("binds without rules");
	(run, skip)
}

/// Rewriting terminates and settles: running the rules again changes nothing.
///
/// A rule pair that undoes each other's work (`A → B → A`) loops here rather than
/// in a query, and a rule that never reaches a fixed point shows up as a plan that
/// keeps changing.
#[tokio::test]
async fn rewriting_reaches_a_fixed_point() {
	let fixture = Fixture::new(SCHEMA).await;
	for sql in CORPUS {
		let (once, _) = both(&fixture, sql).await;
		let bound = fixture.bound_as(&owner(), Vec::new()).await;
		let bx = fixture.bind_context(&bound);
		let twice = bind_with(once.clone(), &bx, Rules::Run).await.expect("rebinds");
		assert_eq!(
			format!("{once:?}"),
			format!("{twice:?}"),
			"rewriting {sql} again changed the plan: the rules have not settled"
		);
	}
}

/// Rewriting never introduces a write.
///
/// The direction matters, and only one direction is a bug. A rule may *drop* a
/// write it has proved unreachable — collapsing `IF false THEN (CREATE …)` is
/// exactly that, and dropping the write is the point of it. A rule may never
/// *introduce* one, because nothing a statement did not ask for may reach
/// storage.
///
/// Asserting equality here instead would forbid dead-branch elimination, which
/// is how this property was first written and what the corpus caught.
#[tokio::test]
async fn rewriting_never_introduces_a_write() {
	let fixture = Fixture::new(SCHEMA).await;
	for sql in CORPUS {
		let (run, skip) = both(&fixture, sql).await;
		if super::write_walk::first_write(&run).is_some() {
			assert!(
				super::write_walk::first_write(&skip).is_some(),
				"rewriting {sql} introduced a write the statement did not have"
			);
		}
	}
}

/// Rewriting does not widen what a statement's expressions do.
///
/// The union over a plan's scalars of what each declares — entropy, clock reads,
/// external reads and writes, state advances — is not a rule's to grow. A rule
/// that introduces a call the statement did not make, or evaluates one where the
/// statement asked for none, shows up here even when the plan is otherwise
/// unrecognisable.
///
/// Narrowing is allowed and expected: dropping a provably dead branch drops its
/// effects with it, which is the point of dropping it. So this asserts
/// containment rather than equality, and the direction matters — a rule may take
/// effects away, never add them.
#[tokio::test]
async fn rewriting_does_not_widen_effects() {
	let fixture = Fixture::new(SCHEMA).await;
	let registry = crate::exec::function::FunctionRegistry::with_builtins();
	for sql in CORPUS {
		let (run, skip) = both(&fixture, sql).await;
		let before = super::effects::effects_of_plan(&skip, &registry);
		let after = super::effects::effects_of_plan(&run, &registry);
		assert!(
			before.set.contains(after.set),
			"rewriting {sql} introduced an effect the statement did not have: \
			 {:?} became {:?}",
			before.set,
			after.set
		);
	}
}

/// The corpus reaches statements whose effects differ, so the effect property is
/// not vacuous.
///
/// A containment assertion over a corpus that is uniformly `PURE` would pass
/// however wrong the rules were. This asserts the input space actually spans the
/// effects the property is meant to protect, and fails if a corpus edit quietly
/// removes the only statement carrying one.
#[tokio::test]
async fn the_corpus_exercises_the_effects_it_asserts_on() {
	use crate::expr::logical::EffectSet;

	let fixture = Fixture::new(SCHEMA).await;
	let registry = crate::exec::function::FunctionRegistry::with_builtins();
	let mut seen = EffectSet::PURE;
	let mut impure = false;
	for sql in CORPUS {
		let (_, skip) = both(&fixture, sql).await;
		let effects = super::effects::effects_of_plan(&skip, &registry);
		seen |= effects.set;
		impure |= effects.impure;
	}
	// `CLOCK` is deliberately absent. `fold_constants` evaluates `time::now()`
	// once per statement, before the rules run, so the clock read is a literal by
	// the time either plan exists and neither carries the effect. A rule cannot
	// introduce or lose it through `time::now()` for the same reason. If a clock
	// builtin that does not fold is added, put a statement using it in the corpus
	// and add `CLOCK` here.
	for (flag, name) in [(EffectSet::WRITES_DATA, "WRITES_DATA"), (EffectSet::ENTROPY, "ENTROPY")] {
		assert!(
			seen.contains(flag),
			"no corpus statement carries {name}: the effect property cannot catch a rule \
			 that introduces one"
		);
	}
	assert!(
		impure,
		"no corpus statement is impure: nothing guards against a rule evaluating \
		 twice what must run once"
	);
}

/// Every corpus statement binds both ways without error.
///
/// The cheapest property, and the one that fails first: a rule that panics or
/// refuses on a shape it did not expect is found here before any behaviour is
/// compared.
#[tokio::test]
async fn every_corpus_statement_binds_both_ways() {
	let fixture = Fixture::new(SCHEMA).await;
	for sql in CORPUS {
		let _ = both(&fixture, sql).await;
	}
}
