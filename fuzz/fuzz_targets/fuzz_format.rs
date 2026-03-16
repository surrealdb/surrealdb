#![no_main]

use libfuzzer_sys::fuzz_target;
use surrealdb_core::sql::Ast;
use surrealdb_core::syn::ParserSettings;
use surrealdb_types::ToSql;

fuzz_target!(|query: Ast| {
	let format = query.to_sql();
	let res = surrealdb_core::syn::parse_with_settings(
		&format.as_bytes(),
		ParserSettings {
			object_recursion_limit: 1_000_000,
			query_recursion_limit: 1_000_000,
			files_enabled: true,
			surrealism_enabled: true,
			..ParserSettings::default()
		},
		async |parser, stk| parser.parse_query(stk).await,
	);

	if let Err(e) = res {
		panic!("Failed to parse format\n{e}\n\nSOURCE:\n{format}\nDEBUG:\n{:#?}", query);
	}
});
