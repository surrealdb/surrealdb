//! What the executor derives from a statement before it runs.
//!
//! [`TopLevelStatement`] holds the surface AST, and two of the answers it
//! gives are checked over the whole language-test corpus: the text that names
//! a statement to observers, which must never carry a plaintext credential,
//! and the `FOR UPDATE` admission, which runs on the surface statement before
//! lowering and must agree with the expression-layer check the legacy engine
//! applies to the same statement.

use std::path::{Path, PathBuf};

use surrealdb_types::ToSql;

use crate::dbs::TopLevelStatement;

/// Call `f` for every statement in the language-test corpus.
///
/// A fixture that does not parse under the default configuration (deliberate
/// parse-error cases, syntax behind a parser feature flag) is skipped.
///
/// Returns `Some(count)` of the statements visited, so a caller can refuse to
/// pass on a sweep that compared almost nothing, or `None` when there is no
/// corpus to walk — the published crate carries no `language-tests` directory
/// beside it, and a consumer running `cargo test` on it has nothing to sweep
/// rather than a failure to report.
pub(super) fn each_corpus_statement(
	mut f: impl FnMut(&Path, &crate::sql::TopLevelExpr),
) -> Option<usize> {
	let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("..")
		.join("..")
		.join("language-tests")
		.join("tests");
	if !corpus.is_dir() {
		println!("skipped: no language-test corpus at {}", corpus.display());
		return None;
	}

	let mut files = Vec::new();
	collect_surql(&corpus, &mut files);
	assert!(!files.is_empty(), "the corpus directory exists but holds no `.surql` fixtures");

	let mut visited = 0;
	let mut unparsed = 0;
	for path in files {
		let Ok(source) = std::fs::read_to_string(&path) else {
			continue;
		};
		let Ok(ast) = crate::syn::parse(&source) else {
			unparsed += 1;
			continue;
		};
		for expr in &ast.expressions {
			f(&path, expr);
			visited += 1;
		}
	}
	// Fixtures behind a parser feature flag do not parse under the default
	// settings, so some always fail. Reporting the count makes a jump in it
	// visible; the floor in `assert_swept` is what fails on one.
	println!("swept {visited} statements; {unparsed} files did not parse");
	Some(visited)
}

/// Every `.surql` file under `dir`, recursively.
fn collect_surql(dir: &Path, out: &mut Vec<PathBuf>) {
	let Ok(entries) = std::fs::read_dir(dir) else {
		return;
	};
	for entry in entries.flatten() {
		let path = entry.path();
		if path.is_dir() {
			collect_surql(&path, out);
		} else if path.extension().and_then(|x| x.to_str()) == Some("surql") {
			out.push(path);
		}
	}
}

/// The number of statements the corpus sweeps reach, rounded down.
///
/// A floor rather than an equality so that adding fixtures does not fail the
/// suite; raise it when the corpus grows. Set near the real count because the
/// failure this guards is a change that stops most of the corpus parsing, and
/// a floor far below the count passes that unchanged.
const CORPUS_FLOOR: usize = 13_800;

/// A corpus that suddenly stops parsing would leave these tests green while
/// asserting nothing. An absent corpus is the one case that is not that: there
/// was nothing to sweep, which [`each_corpus_statement`] reports as `None`.
fn assert_swept(visited: Option<usize>) {
	let Some(visited) = visited else {
		return;
	};
	assert!(
		visited >= CORPUS_FLOOR,
		"only {visited} statements were compared, below the {CORPUS_FLOOR} the corpus holds; \
		 the sweep is asserting less than it looks like it does"
	);
}

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

/// The `FOR UPDATE` rules are checked on the surface statement before it is
/// lowered, and on the expression-layer statement by the legacy engine. Both
/// checks must admit and refuse the same statements with the same message, or
/// a `SELECT` embedded in a converted expression would be judged differently
/// from the same text at the top level.
#[test]
fn for_update_validation_agrees_across_statement_forms() {
	fn compare(sql: &str, stmt: &crate::sql::statements::SelectStatement) {
		let surface = crate::dbs::validate_for_update_ast(stmt).map_err(|e| e.to_string());
		let lowered =
			crate::dbs::validate_for_update(&stmt.clone().into()).map_err(|e| e.to_string());
		assert_eq!(surface, lowered, "FOR UPDATE validation differs between forms for: {sql}");
	}

	// One statement per rule, so each is exercised whatever the corpus holds.
	// The first is admitted; every other one is refused for a different
	// reason.
	for sql in [
		"SELECT * FROM t:1 FOR UPDATE",
		"SELECT * FROM t FOR UPDATE",
		"SELECT * FROM t:1..9 FOR UPDATE",
		"SELECT * FROM t:1, t:2 LIMIT 1 FOR UPDATE",
		"SELECT * FROM t:1 GROUP ALL FOR UPDATE",
		"SELECT * FROM t:1 SPLIT a FOR UPDATE",
		"SELECT * FROM t:1 VERSION d'2024-01-01T00:00:00Z' FOR UPDATE",
		"SELECT * FROM (SELECT * FROM t FOR UPDATE)",
	] {
		let mut ast = crate::syn::parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Select(stmt)) =
			ast.expressions.remove(0)
		else {
			panic!("{sql}: expected a SELECT");
		};
		compare(sql, &stmt);
	}

	let mut for_update = 0;
	let visited = each_corpus_statement(|path, expr| {
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Select(stmt)) = expr else {
			return;
		};
		if stmt.for_update {
			for_update += 1;
		}
		compare(&path.display().to_string(), stmt);
	});
	assert_swept(visited);
	assert!(
		visited.is_none() || for_update > 0,
		"the corpus sweep saw no FOR UPDATE statement, so it compared only the admitted path"
	);
}
