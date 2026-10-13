//! A statement must answer the same whichever form it is in.
//!
//! [`TopLevelStatement`] carries either the surface AST or the lowered
//! expression tree, and answers the executor's questions from whichever it
//! holds. The two enums it walks evolve separately, so every derived answer
//! exists twice and the two can drift apart: a variant classified on one side
//! and not the other would send the same query down a different path
//! depending only on which front end submitted it.
//!
//! These sweep the language-test corpus, converting each statement and
//! comparing every answer.

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

/// Every answer a statement gives, compared across both of its forms in one
/// walk of the corpus.
///
/// One test rather than one per answer because each was re-reading and
/// re-parsing all 2000-odd fixtures to ask a different question of the same
/// statements.
///
/// `read_only` and `statement_type` are the two independent answers here.
/// `kind_reads_only` and `query_type` are derived from `statement_type`, so
/// they agree across forms by construction and asserting them separately would
/// only restate that.
#[test]
fn both_forms_answer_a_statement_the_same_way() {
	let mut rendered = 0;
	let visited = each_corpus_statement(|path, expr| {
		let surface = TopLevelStatement::Ast(expr.clone());
		let lowered = TopLevelStatement::Plan(expr.clone().into());
		let context = path.display();

		// What the observer records, and — through it — what a transport
		// reads to learn whether a result names a new subscription.
		assert_eq!(
			surface.statement_type(),
			lowered.statement_type(),
			"statement_type differs between the two forms in {context}"
		);

		// What the executor's drivers act on. A statement that dispatched
		// differently by form would run under a different driver.
		assert_eq!(
			surface.dispatch(),
			lowered.dispatch(),
			"dispatch differs between the two forms in {context}"
		);

		// The predicate that chooses a transaction mode.
		assert_eq!(
			surface.read_only(),
			lowered.read_only(),
			"read_only differs between the two forms in {context}"
		);

		// `PASSWORD` is excluded because lowering it is not a function: each
		// conversion salts the hash afresh, so two conversions of one
		// statement cannot produce the same text. That case is covered by
		// `statement_text_never_carries_a_plaintext_password` instead.
		if !expr.to_sql().to_ascii_uppercase().contains("PASSWORD") {
			assert_eq!(
				surface.to_sql(),
				lowered.to_sql(),
				"statement text differs between the two forms in {context}"
			);
			rendered += 1;
		}
	});
	assert_swept(visited);
	// The render comparison covers a subset of the same sweep, so it is only a
	// number worth asserting on when there was a corpus behind it.
	assert!(
		visited.is_none_or(|visited| rendered * 100 / visited.max(1) > 95),
		"only {rendered} of {visited:?} statements had their text compared"
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
