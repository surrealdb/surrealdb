//! `TopLevelExpr::read_only()` must over-approximate: it may call a read-only
//! statement writable, but never a writing statement read-only.
//!
//! The executor selects the transaction type from this predicate before the
//! statement is planned (`Executor::execute_plan_impl_inner`), so a clause it
//! fails to inspect lets a mutation reach a read-only transaction and fail
//! partway through. It also makes the same statement behave differently
//! depending on whether the caller wrapped it in `BEGIN`, which opens a write
//! transaction unconditionally.
//!
//! The predicate exists twice — once over the surface AST (`sql::TopLevelExpr`)
//! and once over the expression tree it converts to (`expr::TopLevelExpr`) —
//! because the transaction mode is chosen on whichever form the caller holds.
//! The two enums evolve separately, so every fixture here answers through both
//! and the answers must match: a variant handled on one side and not the other
//! surfaces as a disagreement rather than as a mutation reaching a read-only
//! transaction.

use crate::expr::TopLevelExpr;

/// Parse one statement and report whether it claims to be read-only, asserting
/// the surface AST and the expression tree agree on the answer.
fn read_only(sql: &str) -> bool {
	let mut ast = crate::syn::parse(sql).expect("fixture should parse");
	assert_eq!(ast.expressions.len(), 1, "expected exactly one statement in {sql:?}");
	assert_agree(&ast.expressions.remove(0), sql)
}

/// Answer through both implementations and require them to match.
fn assert_agree(surface: &crate::sql::TopLevelExpr, context: &str) -> bool {
	let converted: TopLevelExpr = surface.clone().into();
	let answer = surface.read_only();
	assert_eq!(
		answer,
		converted.read_only(),
		"`sql::TopLevelExpr::read_only` and `expr::TopLevelExpr::read_only` disagree on {context}"
	);
	answer
}

/// Assert that no case claims to be read-only, skipping shapes the grammar
/// rejects: a clause that cannot carry a mutation cannot be a hole.
///
/// Skips are reported rather than passed over silently, and at least one case
/// must run, so a grammar change that stops these fixtures parsing surfaces as
/// lost coverage instead of a still-green test.
fn assert_none_claim_read_only(cases: &[(&str, &str)]) {
	let mut exercised = 0;
	for (label, sql) in cases {
		if crate::syn::parse(sql).is_err() {
			println!("skipped (does not parse): {label}: {sql}");
			continue;
		}
		exercised += 1;
		assert!(!read_only(sql), "{label}: `{sql}` claimed to be read-only");
	}
	assert!(exercised > 0, "every fixture was skipped, so this test asserted nothing");
}

/// Every clause of a `SELECT` that can carry a user expression, each with a
/// mutation embedded in it. None of these may claim to be read-only.
#[test]
fn a_mutation_in_any_select_clause_requires_a_write_transaction() {
	let cases = [
		("projection", "SELECT *, (CREATE log) AS w FROM person"),
		("what", "SELECT * FROM (CREATE log)"),
		("cond", "SELECT * FROM person WHERE (CREATE log).n > 0"),
		("omit", "SELECT * OMIT (CREATE log).n FROM person"),
		("limit", "SELECT * FROM person LIMIT (CREATE log).n"),
		("start", "SELECT * FROM person START (CREATE log).n"),
		("version", "SELECT * FROM person VERSION (CREATE log).n"),
		("timeout", "SELECT * FROM person TIMEOUT (CREATE log).n"),
		("fetch", "SELECT * FROM person FETCH tags[WHERE (CREATE log)]"),
		("split", "SELECT * FROM person SPLIT ON tags[WHERE (CREATE log)]"),
		("group", "SELECT tags FROM person GROUP BY tags[WHERE (CREATE log)]"),
		("order", "SELECT * FROM person ORDER BY tags[WHERE (CREATE log)]"),
	];
	assert_none_claim_read_only(&cases);
}

/// The idiom parts that carry expressions, reached through a projection.
#[test]
fn a_mutation_in_any_idiom_part_requires_a_write_transaction() {
	let cases = [
		("where filter", "SELECT tags[WHERE (CREATE log)] FROM person"),
		("value index", "SELECT tags[(CREATE log).n] FROM person"),
		("method argument", "SELECT tags.at((CREATE log).n) FROM person"),
		("destructure alias", "SELECT person.{ name, tag: tags[WHERE (CREATE log)] } FROM person"),
		("graph lookup cond", "SELECT ->likes[WHERE (CREATE log)]->thing FROM person"),
	];
	assert_none_claim_read_only(&cases);
}

/// `INFO` names what it reports on with expressions, and takes a `VERSION`
/// expression, all of which the statement evaluates before it reads anything.
#[test]
fn a_mutation_in_an_info_subject_or_version_requires_a_write_transaction() {
	let cases = [
		("root version", "INFO FOR ROOT VERSION (CREATE log:1).id"),
		("ns version", "INFO FOR NS VERSION (CREATE log:1).id"),
		("db version", "INFO FOR DB VERSION (CREATE log:1).id"),
		("table version", "INFO FOR TABLE person VERSION (CREATE log:1).id"),
		("table name", "INFO FOR TABLE (CREATE log:1).id"),
		("user name", "INFO FOR USER (CREATE log:1).id"),
		("index name", "INFO FOR INDEX (CREATE log:1).id ON person"),
		("index table", "INFO FOR INDEX idx ON (CREATE log:1).id"),
	];
	assert_none_claim_read_only(&cases);
}

/// A graph lookup's subject carries record-id range bounds, which are
/// record-id keys and so carry arbitrary expressions.
#[test]
fn a_mutation_in_a_lookup_subject_requires_a_write_transaction() {
	let cases = [
		("lookup range upper bound", "SELECT * FROM b:1->computed_edge:[6]..=[(CREATE log:1).id]"),
		("lookup range lower bound", "SELECT * FROM b:1->computed_edge:[(CREATE log:1).id]..=[6]"),
		(
			"lookup subject in projection",
			"SELECT * FROM person:1->likes:[(CREATE log:2).id]..->post",
		),
	];
	assert_none_claim_read_only(&cases);
}

/// `RETURN`'s fetch clause carries expressions just as a `SELECT`'s does.
#[test]
fn a_mutation_in_a_return_fetch_requires_a_write_transaction() {
	assert_none_claim_read_only(&[(
		"return fetch",
		"RETURN person FETCH tags[WHERE (CREATE log)]",
	)]);
}

/// The predicate must stay useful: genuinely read-only statements keep their
/// read transaction. Without these, "return false always" would satisfy the
/// tests above and cost every SELECT a write transaction.
#[test]
fn read_only_statements_keep_their_read_transaction() {
	let cases = [
		"SELECT * FROM person",
		"SELECT *, count() AS n FROM person GROUP ALL",
		"SELECT * FROM person WHERE age > 30 ORDER BY name LIMIT 10 START 5",
		"SELECT * FROM person FETCH friends",
		"SELECT * FROM person SPLIT ON tags",
		"SELECT name FROM person GROUP BY name",
		"SELECT * FROM person TIMEOUT 5s",
		"SELECT ->likes->thing FROM person",
		"SELECT tags[WHERE value > 1] FROM person",
		"RETURN person FETCH friends",
		"RETURN 1 + 2",
		// Registered builtin methods dispatch to builtins, so only their
		// receiver and arguments matter — including on receivers whose value
		// the plan cannot see, and with a pure literal closure argument.
		"RETURN $s.len()",
		"RETURN 'a'.len()",
		"SELECT tags.map(|$t| $t + 1) FROM person",
	];
	for sql in cases {
		assert!(read_only(sql), "`{sql}` should not need a write transaction");
	}
}

/// Mutations are writable wherever they appear, which is the baseline the
/// clause cases above extend.
#[test]
fn mutations_require_a_write_transaction() {
	for sql in ["CREATE person", "UPDATE person SET a = 1", "DELETE person", "DEFINE TABLE person"]
	{
		assert!(!read_only(sql), "`{sql}` claimed to be read-only");
	}
}

/// Composite literals evaluate their element expressions in place, so a
/// mutation inside one needs a write transaction.
#[test]
fn a_mutation_in_a_composite_literal_requires_a_write_transaction() {
	let cases = [
		("array literal", "RETURN [(CREATE log)]"),
		("set literal", "RETURN <set> [(CREATE log)]"),
		("object literal", "RETURN { w: (CREATE log) }"),
		("record id key", "RETURN r_thing:[(CREATE log).n]"),
		("record id range", "SELECT * FROM r_thing:[(CREATE log).n].."),
	];
	assert_none_claim_read_only(&cases);
}

/// Closures execute bodies the statement text may not reveal. A call on
/// anything but a closure literal, a call argument, and a writing literal
/// body must all over-approximate to writable.
#[test]
fn closures_that_may_write_require_a_write_transaction() {
	let cases = [
		("call on a param", "RETURN $fn(1)"),
		("call argument", "RETURN (|$n| $n)((CREATE log).n)"),
		("writing literal body, called", "RETURN (|| { CREATE log })()"),
		("writing literal body, as a value", "RETURN [|| { CREATE log }]"),
		(
			"writing literal body, as a builtin argument",
			"RETURN array::map([1], || { CREATE log })",
		),
		// A method call whose name is not a registered builtin resolves to a
		// closure stored on the receiver, whose body the statement text does
		// not reveal — through the idiom path and the postfix operator alike.
		("closure field invoked as a method", "RETURN $o.w()"),
		("closure field invoked as a postfix method", "RETURN ($o).w()"),
		("closure field invoked mid-idiom", "SELECT handlers.w().n FROM person"),
	];
	assert_none_claim_read_only(&cases);
}

/// The precision that keeps the previous test honest: a call on a closure
/// literal whose body is read-only keeps its read transaction.
#[test]
fn a_pure_literal_closure_call_keeps_its_read_transaction() {
	for sql in ["RETURN (|$n: number| $n + 1)(1)", "SELECT * FROM (|| 1)()"] {
		assert!(read_only(sql), "`{sql}` should not need a write transaction");
	}
}
