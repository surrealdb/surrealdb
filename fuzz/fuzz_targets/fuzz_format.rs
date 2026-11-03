#![no_main]

use libfuzzer_sys::fuzz_target;
use surrealdb_core::syn::runtime::ParserSettings;
use surrealdb_core::syn::Query as Ast;
use surrealdb_types::ToSql;

fuzz_target!(|query: Ast| {
	let format = query.to_sql();
	let res = surrealdb_core::syn::runtime::query_with_settings(
		&format,
		ParserSettings {
			object_recursion_limit: ParserSettings::STORED_TEXT.object_recursion_limit,
			query_recursion_limit: ParserSettings::STORED_TEXT.query_recursion_limit,
			expr_recursion_limit: ParserSettings::STORED_TEXT.expr_recursion_limit,
			surrealism_enabled: true,
			..ParserSettings::default()
		},
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
