//! Parity checks that every builtin path and keyword relate-target parses
//! in both parsers.
//!
//! They live in core because they drive the greenfield parser alongside
//! `surrealdb-syn`; neither parser crate can see the other.

use surrealdb_sql::builtin_paths::{PATHS, PathKind};

#[test]
fn paths_parse_in_both_parsers() {
	let mut failures = Vec::new();
	for (path, (kind, _)) in PATHS.entries() {
		let path = path.into_inner();
		let source = match kind {
			PathKind::Constant(_) => format!("RETURN {path};"),
			PathKind::Function => format!("RETURN {path}(0);"),
		};
		if let Err(e) = crate::syn::parse(&source) {
			failures.push(format!("`{path}` failed to parse in the core parser: {e}"));
		}
		if let Err(e) =
			parser::Parser::enter_parse::<ast::Query>(&source, parser::Config::all_features())
		{
			failures.push(format!(
				"`{path}` failed to parse in the new parser:\n{}",
				e.render_char_buffer().write_to_string()
			));
		}
	}
	assert!(
		failures.is_empty(),
		"builtin function/constant paths diverged between the parsers:\n{}",
		failures.join("\n")
	);
}

#[test]
fn keyword_relate_targets_parse_in_both_parsers() {
	let mut failures = Vec::new();
	for source in [
		"RELATE a:1->edge->sleep:b;",
		"RELATE sleep:a->sleep->sleep:b;",
		"RELATE sleep:b<-edge<-a:1;",
		"RELATE a:1->edge->(SELECT * FROM b);",
		"RELATE a:1->edge->SELECT * FROM b;",
	] {
		if let Err(e) = crate::syn::parse(source) {
			failures.push(format!("`{source}` failed to parse in the core parser: {e}"));
		}
		if let Err(e) =
			parser::Parser::enter_parse::<ast::Query>(source, parser::Config::all_features())
		{
			failures.push(format!(
				"`{source}` failed to parse in the new parser:\n{}",
				e.render_char_buffer().write_to_string()
			));
		}
	}
	assert!(
		failures.is_empty(),
		"RELATE keyword-target parsing diverged between the parsers:\n{}",
		failures.join("\n")
	);
}
