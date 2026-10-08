//! What the executor derives from a statement before it runs.
//!
//! [`TopLevelStatement`] holds the surface AST, and the text it gives to name a
//! statement to observers must never carry a plaintext credential.

use surrealdb_types::ToSql;

use crate::dbs::TopLevelStatement;

/// SECURITY: statement text must never carry a plaintext credential.
///
/// The surface form does — it is what the user typed — and lowering is what
/// replaces it with the derived hash and verifier. That makes rendering the
/// surface form directly a disclosure into the tracing span, the slow log and
/// any observer that asks for statement text, so [`TopLevelStatement`] renders
/// through the lowered form instead.
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
