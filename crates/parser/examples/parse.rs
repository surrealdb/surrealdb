use std::io::{Read, stdout};

use ast::Query;

fn read_input() -> String {
	if let Some(arg) = std::env::args().nth(1) {
		std::fs::read_to_string(arg).unwrap()
	} else {
		std::io::read_to_string(std::io::stdin()).unwrap()
	}
}

fn main() {
	let input = read_input();

	match surrealdb_parser::parse::Parser::enter_parse::<Query>(&input, Default::default()) {
		Ok((node, ast)) => {
			println!("{}", std::fmt::from_fn(|fmt| { ast::vis::visualize_ast(node, &ast, fmt) }))
		}
		Err(e) => e.render_char_buffer().write_styled(&mut stdout().lock()).unwrap(),
	}
}
