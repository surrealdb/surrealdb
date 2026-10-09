#![no_main]

use libfuzzer_sys::fuzz_target;
use surrealdb_sql::Ast;
use surrealdb_syn::parser::ParserSettings;
use surrealdb_types::ToSql;

fuzz_target!(|query: Ast| {
	let format = query.to_sql();
	// Parsing recurses on the call stack, so the re-parse keeps bounded depth
	// limits: the stored-text ceilings, the most the parser admits anywhere.
	// A generated tree deeper than those is not a round-trip failure.
	let res = surrealdb_core::syn::parse_with_settings(
		&format.as_bytes(),
		ParserSettings {
			object_recursion_limit: ParserSettings::STORED_TEXT.object_recursion_limit,
			query_recursion_limit: ParserSettings::STORED_TEXT.query_recursion_limit,
			expr_recursion_limit: ParserSettings::STORED_TEXT.expr_recursion_limit,
			files_enabled: true,
			surrealism_enabled: true,
			..ParserSettings::default()
		},
		|parser| parser.parse_query(),
	);
	if let Err(e) = &res
		&& e.to_string().contains("recursion depth limit")
	{
		return;
	}

	if let Err(e) = res {
		panic!("Failed to parse format\n{e}\n\nSOURCE:\n{format}\nDEBUG:\n{:#?}", query);
	}
});
