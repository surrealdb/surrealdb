#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]
use std::io::stdout;
use std::{fmt, iter};

use ast::{Ast, Query};
use reblessive::Stack;
use surrealdb_parser::Config;

fn read_input() -> String {
	if let Some(arg) = std::env::args().nth(1) {
		std::fs::read_to_string(arg).unwrap()
	} else {
		std::io::read_to_string(std::io::stdin()).unwrap()
	}
}

fn main() {
	let input = read_input();

	let mut stack = Stack::new();
	let mut ast = Ast::empty();

	let mut offset = 0;

	let config = Config {
		depth_limit: 1000,
		generate_warnings: true,
		feature_bearer_access: true,
		feature_surrealism: true,
		quirk_redefine: true,
		quirk_block_first_no_semi: true,
		quirk_delete_permission_field: true,
		quirk_legacy_place_productions: true,
	};

	for idx in input.char_indices().map(|x| x.0).skip(1).chain(iter::once(input.len())) {
		let found = if idx == input.len() {
			surrealdb_parser::Parser::enter_parse_reuse::<Query>(
				&input[offset..],
				&mut stack,
				&mut ast,
				config,
			)
			.map(|x| Some((x, (idx - offset) as u32)))
		} else {
			surrealdb_parser::Parser::enter_partial_parse::<Query>(
				&input[offset..idx],
				&mut stack,
				&mut ast,
				config,
			)
		};

		match found {
			Ok(None) => {
				ast.clear();
			}
			Ok(Some((node, parsed))) => {
				let Some(expr) = node.exprs else {
					ast.clear();
					continue;
				};

				for expr in ast.iter_list(Some(expr)) {
					println!("{}", fmt::from_fn(|fmt| ast::vis::visualize_ast(&expr, &ast, fmt)));
				}
				offset += parsed as usize;
				ast.clear();
			}
			Err(e) => {
				e.render_char_buffer().write_styled(&mut stdout().lock()).unwrap();
				return;
			}
		}
	}
}
