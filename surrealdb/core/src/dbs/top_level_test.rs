//! What the executor derives from a statement before it runs.
//!
//! [`TopLevelStatement`] holds runtime syntax, and the text it gives to name a
//! statement to observers must never carry a plaintext credential.

use surrealdb_types::ToSql;

use crate::dbs::TopLevelStatement;

/// Observer rendering must redact plaintext credentials in every nested position.
#[test]
fn statement_text_never_carries_a_plaintext_password() {
	for sql in [
		"DEFINE USER api ON DATABASE PASSWORD 'hunter2' ROLES VIEWER",
		// A password in a nested position must be redacted too, so the render
		// is not special-cased to one statement kind.
		"DEFINE FUNCTION fn::x() { DEFINE USER api ON NAMESPACE PASSWORD 'hunter2' ROLES OWNER }",
	] {
		let mut ast = crate::syn::parse(sql).expect("fixture should parse");
		let surface = ast.expressions.remove(0);
		assert!(
			surface.to_sql().contains("hunter2"),
			"the fixture must actually carry a plaintext password, or it asserts nothing"
		);
		let text = TopLevelStatement::Ast(surface).to_sql();
		assert!(!text.contains("hunter2"), "statement text disclosed a plaintext password: {text}");
	}
}

/// Compiling a request directly gives the same statement type and canonical
/// text as the AST path, whatever operator shape the optimizer builds later.
#[test]
fn flat_requests_preserve_statement_metadata() {
	use surrealdb_expr::parser::{ExecutionStatement, ParserConfig, ParserSettings};

	use crate::observe::StatementType;
	let settings = ParserSettings::all_features(&ParserConfig::default());
	for (source, kind) in [
		("SELECT * FROM person WHERE age > 18", StatementType::Select),
		("LET $a = 1 + 2", StatementType::Let),
		("RETURN (SELECT VALUE name FROM person)", StatementType::Return),
		("{ RETURN 1 }", StatementType::Block),
		("INFO FOR DB", StatementType::Info),
		("EXPLAIN SELECT * FROM person", StatementType::Explain),
	] {
		let parsed = surrealdb_expr::parser::execution_query_with_settings(source, settings)
			.expect("request parses")
			.remove(0);
		let ExecutionStatement::Logical(statement) = parsed else {
			panic!("engine statement must compile directly");
		};
		let direct = TopLevelStatement::Surreal(statement);
		let native =
			TopLevelStatement::Ast(crate::syn::parse(source).unwrap().expressions.remove(0));
		assert_eq!(direct.statement_type(), kind);
		assert_eq!(direct.read_only(), native.read_only());
		assert_eq!(direct.kind_reads_only(), native.kind_reads_only());
		assert_eq!(direct.to_sql(), native.to_sql());
	}
}

/// Transaction control and `USE` are top-level units. None is an expression, so no
/// `LogicalSequence` can hold one as a sibling. Each statement inside a transaction
/// lowers to a plan that holds no sequence.
#[test]
fn transaction_statements_are_never_siblings() {
	use std::ops::ControlFlow;

	use surrealdb_expr::parser::TopLevelExpr;

	use crate::dbs::top_level::Dispatch;
	use crate::expr::logical::{LogicalOp, RequestStatementKind, lower};

	let ast = crate::syn::parse("BEGIN; LET $a = 1; COMMIT; USE NS x; LET $b = 1; CANCEL;")
		.expect("request parses");
	assert_eq!(ast.expressions.len(), 6, "each statement is a unit of its own");
	let mut shapes = Vec::new();
	for unit in ast.expressions {
		if let TopLevelExpr::Expr(expr) = &unit {
			let op = lower(expr.clone()).expect("statement lowers");
			assert!(matches!(op, LogicalOp::Let(_)), "a LET stays a statement of its own");
			let sequence = op.visit(&mut |node| {
				if matches!(node, LogicalOp::Sequence(_)) {
					ControlFlow::Break(())
				} else {
					ControlFlow::Continue(())
				}
			});
			assert!(sequence.is_continue(), "a statement of a transaction holds no sequence");
		}
		let unit = TopLevelStatement::Ast(unit);
		shapes.push((unit.dispatch(), unit.request_kind()));
	}
	assert_eq!(
		shapes,
		[
			(
				Dispatch::Begin(surrealdb_expr::parser::BeginOptions::default()),
				RequestStatementKind::Begin
			),
			(Dispatch::Runnable, RequestStatementKind::Run),
			(Dispatch::Commit, RequestStatementKind::Commit),
			(Dispatch::Runnable, RequestStatementKind::Run),
			(Dispatch::Runnable, RequestStatementKind::Run),
			(Dispatch::Cancel, RequestStatementKind::Cancel),
		]
	);
}
