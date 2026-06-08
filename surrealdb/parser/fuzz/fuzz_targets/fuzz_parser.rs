#![no_main]

use libfuzzer_sys::fuzz_target;
use surrealdb_parser::Parser;

fuzz_target!(|data: &str| {
	if let Err(e) = Parser::enter_parse::<surrealdb_ast::Query>(&data, Default::default()) {
		let _ = e.render_char_buffer().write_to_string();
	}
});
