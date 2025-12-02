use std::io::Read;

use ast::Query;
use common::source_error::Renderer;

fn read_input() -> String {
	if let Some(arg) = std::env::args().nth(1) {
		std::fs::read_to_string(arg).unwrap()
	} else {
		std::io::read_to_string(std::io::stdin()).unwrap()
	}
}

fn main() {
	let input = read_input();

	match surrealdb_parser::parse::Parser::enter_parse::<Query>(
		input.as_bytes(),
		Default::default(),
	) {
		Ok(_) => {
			todo!()
		}
		Err(e) => {
			println!("{}", Renderer::styled().render(&*e))
		}
	}
}
