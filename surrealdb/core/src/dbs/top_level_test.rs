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
		// Reached through a nested position rather than at top level, which is
		// why the render is not a carve-out for one statement kind.
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

/// Direct compilation preserves the observer category and canonical text
/// independently of the optimizer's eventual operator shape.
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
