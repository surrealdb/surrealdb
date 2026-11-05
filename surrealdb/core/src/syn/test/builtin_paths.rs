//! Every builtin path and keyword relate-target parses.

use surrealdb_expr::expr::builtin_paths::{PATHS, PathKind};

#[test]
fn paths_parse() {
	let mut failures = Vec::new();
	for (path, (kind, _, _)) in PATHS.entries() {
		let path = path.into_inner();
		let source = match kind {
			PathKind::Constant(_) => format!("RETURN {path};"),
			PathKind::Function => format!("RETURN {path}(0);"),
		};
		if let Err(e) = crate::syn::parse(&source) {
			failures.push(format!("`{path}` failed to parse: {e}"));
		}
	}
	assert!(
		failures.is_empty(),
		"builtin function/constant paths failed:\n{}",
		failures.join("\n")
	);
}

#[test]
fn keyword_relate_targets_parse() {
	let mut failures = Vec::new();
	for source in [
		"RELATE a:1->edge->sleep:b;",
		"RELATE sleep:a->sleep->sleep:b;",
		"RELATE sleep:b<-edge<-a:1;",
		"RELATE a:1->edge->(SELECT * FROM b);",
		"RELATE a:1->edge->SELECT * FROM b;",
	] {
		if let Err(e) = crate::syn::parse(source) {
			failures.push(format!("`{source}` failed to parse: {e}"));
		}
	}
	assert!(failures.is_empty(), "RELATE keyword targets failed:\n{}", failures.join("\n"));
}
