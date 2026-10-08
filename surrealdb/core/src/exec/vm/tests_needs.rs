//! What an expression needs of the transaction and the session, derived from
//! the logical expression, equals what its compiled form declares.

use super::tests::{SCHEMA, planner};
use crate::exec::AccessMode;
use crate::exec::operators::test_util::TestDb;
use crate::expr::logical::{LogicalExpr, LogicalOp};

/// Expressions of every shape the VM hands to a compiled expression, each
/// with the access mode and context level its compiled form declares.
const EXPRESSIONS: &[&str] = &[
	"1 + 2",
	"[1, 2, { a: 3 }]",
	"{ 1, 2 }",
	"$limit - 28",
	"person:1",
	"person:[1, 2]",
	"person:1.name",
	"person:1.tags[0]",
	"person:1.tags[$]",
	"person:1.tags.*",
	"person:1.nested.{ x }",
	"person:1.nested.{ y: x }",
	"person:1.tags[WHERE $this != 'a']",
	"person:1.tags.len()",
	"person:1.name.slice(1)",
	"person:1.name.unknown_method()",
	"[1, 2][age]",
	"'abc'.slice(age)",
	"person:1.name ?? 'x'",
	"IF person:1.age > 4 THEN 'a' ELSE 'b' END",
	"math::abs(-3)",
	"string::len(person:1.name)",
	"type::field('age')",
	"type::fields(['age', 'name'])",
	"fn::inc(1)",
	"fn::oldest()",
	"fn::fact(5)",
	"[(SELECT * FROM person)]",
	"(SELECT VALUE age FROM person LIMIT 1)[0]",
	"[(RETURN 1)]",
	"[(CREATE person:100 SET name = 'x')]",
	"[(UPDATE person:1 SET age = 31)]",
	"[(DELETE person:100)]",
	"[(DEFINE TABLE other)]",
	"[(INFO FOR DB)]",
	"[(INFO FOR ROOT)]",
	"[(LET $x = 1)]",
	"[(SELECT VALUE fn::inc(age) FROM person)]",
	"[(SELECT VALUE (CREATE person:107) FROM person)]",
	"[(SELECT * FROM person WHERE fn::oldest() = name)]",
	"[(SLEEP 1ms)]",
	"[(EXPLAIN SELECT * FROM person)]",
	"[(EXPLAIN ANALYZE SELECT * FROM person)]",
	"[(SELECT * FROM [])]",
	"[(SELECT name FROM [])]",
	"[(SELECT VALUE name FROM [{ name: 1 }])]",
	"mod::secret::run()",
	"silo::acme::pkg::<1.0.0>::run()",
	"[(RETURN 1), (CREATE person:101)]",
	"fn::inc((CREATE person:102).len())",
	"person:1->knows",
	"person:1->knows->person",
	"person:1<-knows<-person.name",
	"count(person:1->knows)",
	"person:1.{1..3}.tags",
	"person:1.{1..3}(.nested)",
	"array::map([1, 2], |$x| $x + 1)",
	"array::map([1, 2], |$x| (CREATE person:103))",
	"array::any([1, 2], |$x| $x > 1)",
	"array::map(person:1.tags, $fn)",
	"|$a: int| $a + 1",
	"|$a: int| (CREATE person:104)",
	"(|$a: int| $a + 1)(2)",
	"(|$a: int| (CREATE person:105))(2)",
	"$fn(1)",
	"person:1.nested.fn()",
	"<int> '3'",
	"-person:1.age",
	"!person:1.age",
	"person:1.age..",
	"..person:1.age",
	"THROW 'x'",
	"search::score(1)",
	"rand::uuid()",
	"time::now()",
	"sleep(1ms)",
	"value::chain(1, |$x| $x)",
	"|t:3|",
	"person:1.tags.map(|$t| $t)",
	"person:1.tags.map(|$t| (CREATE person:106))",
	"[person:1, person:2].*.name",
	"[person:1, person:2].name",
	"person:1 = person:2",
	"person:1.age IN [1, 2]",
	"person:1.name @@ 'x'",
];

#[tokio::test]
async fn an_expression_needs_what_its_compiled_form_declares() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let planner = planner(&ctx);
	let mut checked = 0;
	let mut seen = std::collections::BTreeSet::new();
	let mut mismatches = Vec::new();
	for sql in EXPRESSIONS {
		let Ok(ast) = crate::syn::parse(sql) else {
			continue;
		};
		let Some(crate::sql::TopLevelExpr::Expr(expr)) = ast.expressions.into_iter().next() else {
			continue;
		};
		let Ok(op) = crate::expr::logical::lower(expr) else {
			continue;
		};
		let Ok(op) = planner.bind(op).await else {
			continue;
		};
		let LogicalOp::Eval(eval) = op else {
			continue;
		};
		let expr: LogicalExpr = eval.expr;
		let Ok(compiled) = planner.physical_expr(expr.clone()).await else {
			continue;
		};
		let mut functions = Vec::new();
		crate::exec::logical::stored_functions_of(&expr, &mut functions);
		let mut stored = Vec::new();
		for function in functions {
			let mode = planner.resolve_function_access_mode(&function).await;
			stored.push((function, mode));
		}
		let needs =
			crate::exec::logical::needs_of_expr(&expr, planner.function_registry(), &|function| {
				stored
					.iter()
					.find(|(f, _)| f == function)
					.map_or(AccessMode::ReadWrite, |(_, mode)| *mode)
			});
		checked += 1;
		seen.insert(format!("{:?}/{:?}", needs.access_mode, needs.context));
		let declared = (compiled.access_mode(), compiled.required_context());
		if (needs.access_mode, needs.context) != declared {
			mismatches.push(format!(
				"{sql}: derived {:?}/{:?}, declared {:?}/{:?}",
				needs.access_mode, needs.context, declared.0, declared.1
			));
		}
	}
	assert!(checked > EXPRESSIONS.len() / 2, "only {checked} expressions compiled");
	assert_eq!(seen.len(), 4, "every access mode and context level is exercised: {seen:?}");
	assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
