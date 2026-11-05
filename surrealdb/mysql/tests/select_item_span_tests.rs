//! The span of a select item is the source text of its expression: every token of it,
//! parentheses included, and neither the alias nor what surrounds the item.

use surrealdb_mysql::ast::stmt::{SelectItem, Statement};
use surrealdb_mysql::parse_statement;

/// The source text each item of the select list of `sql` spans, with `None` for a wildcard.
fn item_texts(sql: &str) -> Vec<Option<&str>> {
	let Statement::Select(select) = parse_statement(sql).expect("a select that parses") else {
		panic!("{sql} is not a select");
	};
	select
		.projection
		.iter()
		.map(|item| match item {
			SelectItem::Expr {
				span,
				..
			} => Some(sql.get(span.start..span.end).expect("a span that indexes the text")),
			SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => None,
		})
		.collect()
}

#[test]
fn an_item_spans_its_whole_expression() {
	let cases: &[(&str, &[&str])] = &[
		("SELECT 1", &["1"]),
		("SELECT 1 + 2, 3", &["1 + 2", "3"]),
		// Parentheses are part of the item: the expression inside them does not span them.
		("SELECT (1 + 2) * 3", &["(1 + 2) * 3"]),
		("SELECT a * (b + c)", &["a * (b + c)"]),
		("SELECT -(1 + 2)", &["-(1 + 2)"]),
		("SELECT ((1 + 2))", &["((1 + 2))"]),
		("SELECT ( 1 )", &["( 1 )"]),
		("SELECT (a), ((b))", &["(a)", "((b))"]),
		// Postfix forms, whose own spans begin after their left operand.
		(
			"SELECT a IS NULL, a IS NOT NULL, a IS TRUE",
			&["a IS NULL", "a IS NOT NULL", "a IS TRUE"],
		),
		("SELECT a IN (1, 2), a NOT IN (SELECT 1)", &["a IN (1, 2)", "a NOT IN (SELECT 1)"]),
		("SELECT a BETWEEN 1 AND 2", &["a BETWEEN 1 AND 2"]),
		("SELECT a COLLATE utf8mb4_bin", &["a COLLATE utf8mb4_bin"]),
		("SELECT a = ANY (SELECT 1)", &["a = ANY (SELECT 1)"]),
		("SELECT COUNT(*), count( DISTINCT a )", &["COUNT(*)", "count( DISTINCT a )"]),
		("SELECT SUM(a) OVER (ORDER BY b)", &["SUM(a) OVER (ORDER BY b)"]),
		("SELECT CASE WHEN a THEN 1 ELSE 2 END", &["CASE WHEN a THEN 1 ELSE 2 END"]),
		(
			"SELECT CAST(a AS CHAR), (SELECT 1), EXISTS (SELECT 1)",
			&["CAST(a AS CHAR)", "(SELECT 1)", "EXISTS (SELECT 1)"],
		),
		// An alias is not part of the item, with or without AS and however it is quoted.
		("SELECT a + b AS c, d e, 1 `f g`", &["a + b", "d", "1"]),
		// Neither are the keywords before the list, the clauses after it, or the commas.
		("SELECT DISTINCT a + b FROM t WHERE a > 1", &["a + b"]),
		("SELECT ALL a, b FROM t", &["a", "b"]),
		("SELECT a UNION SELECT b", &["a"]),
		// Comments between the tokens of an item are inside it; those outside are not.
		("SELECT /* x */ 1 /* y */ + /* z */ 2 /* w */ , 3", &["1 /* y */ + /* z */ 2", "3"]),
		("SELECT 1 -- x\n + 2 -- y\n, 3", &["1 -- x\n + 2", "3"]),
		("SELECT\n  1\n  +\n  2\n", &["1\n  +\n  2"]),
		// Offsets are bytes, and fall on character boundaries.
		("SELECT '日本語' + 1, é + 2, 'x'", &["'日本語' + 1", "é + 2", "'x'"]),
	];
	for (sql, expected) in cases {
		let texts: Vec<_> =
			item_texts(sql).into_iter().map(|text| text.expect("an item")).collect();
		assert_eq!(texts, *expected, "{sql}");
	}
}

#[test]
fn a_wildcard_has_no_expression_span() {
	assert_eq!(item_texts("SELECT *, 1 + 1, t.*"), [None, Some("1 + 1"), None]);
}
