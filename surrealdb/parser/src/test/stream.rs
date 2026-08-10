use core::fmt;
use std::iter;
use std::path::Path;
use std::time::Instant;

use ast::Query;
use reblessive::Stack;

use crate::Config;
use crate::test::{SEPERATOR, walk_dir};

fn stream_test<F, P>(path: &Path, config: Config, filter: P, mut cleanup: F)
where
	P: Fn(&Path) -> bool,
	F: FnMut(String) -> String,
{
	let start = Instant::now();
	let mut count = 0;

	let mut successfull = true;
	walk_dir(path, &mut |path| {
		if path.extension().and_then(|x| x.to_str()) != Some("surql") {
			return;
		}

		if !filter(path) {
			return;
		}

		let source = std::fs::read_to_string(path).unwrap();
		let source = cleanup(source);

		let mut expect = match crate::Parser::enter_parse::<Query>(&source, config) {
			Ok((root, ast)) => Ok((root.exprs, ast)),
			Err(e) => Err(format!("ERROR:{}", e.render_char_buffer().write_to_string())),
		};

		let mut offset = 0;

		let mut stack = Stack::new();
		let mut ast = ast::Ast::empty();

		for i in source.char_indices().map(|x| x.0).skip(1).chain(iter::once(source.len())) {
			let slice = &source[offset..i];
			let res = if i == source.len() {
				count += 1;
				crate::Parser::enter_parse_reuse::<Query>(slice, &mut stack, &mut ast, config)
					.map(|x| Some((x, (i - offset) as u32)))
			} else {
				count += 1;
				crate::Parser::enter_partial_parse::<Query>(slice, &mut stack, &mut ast, config)
			};

			match res {
				Err(found) => {
					let Err(_) = expect else {
						println!("Test failed on file `{}`", path.display());
						println!("Streaming query returned unexpected error");
						println!("Got:");
						println!("{}", found.render_char_buffer().write_to_string());
						successfull = false;
						return;
					};
					return;
				}
				Ok(None) => {}
				Ok(Some((q, used))) => {
					let Some(expr) = q.exprs else {
						ast.clear();
						continue;
					};

					offset += used as usize;

					let Ok((expect, expect_ast)) = &mut expect else {
						// Streaming can return a bunch of successfull results
						// before an error happens.
						ast.clear();
						continue;
					};

					for expr in ast.iter_list(Some(expr)) {
						let Some(expect_some) = expect else {
							println!("Test failed on file `{}`", path.display());
							println!("Streaming query returned more results then expected");
							println!("Got:");
							println!(
								"{}",
								fmt::from_fn(|f| ast::vis::visualize_ast(&expr, &ast, f))
							);
							successfull = false;
							return;
						};

						let expect_expr = expect_ast[*expect_some].cur;
						*expect = expect_ast[*expect_some].next;

						let expect =
							fmt::from_fn(|f| ast::vis::visualize_ast(&expect_expr, expect_ast, f))
								.to_string();
						let found =
							fmt::from_fn(|f| ast::vis::visualize_ast(&expr, &ast, f)).to_string();

						if expect != found {
							println!("Test failed on file `{}`", path.display());
							println!(
								"Streaming query returned a different expression then expected"
							);
							println!("Expected:");
							println!("{expect}");
							println!("Got:");
							println!("{found}");
							successfull = false;
							return;
						}
					}
				}
			}

			ast.clear();
		}

		match expect {
			Ok((None, _)) => {}
			Ok((Some(x), ast)) => {
				println!("Test failed on file `{}`", path.display());
				let expect = fmt::from_fn(|f| ast::vis::visualize_ast(&x, &ast, f)).to_string();
				println!("Streaming query returned less results then expected");
				println!("Expected:");
				println!("{expect}");
				successfull = false;
			}
			Err(e) => {
				println!("Test failed on file `{}`", path.display());
				println!(
					"Streaming query parsed query successfully while expecting it to return an error"
				);
				println!("Expected:");
				println!("{e}");
				successfull = false;
			}
		}
	});

	println!("Parsing all tests took {:?}", start.elapsed());
	println!("Invoked parser {count} times");

	assert!(successfull, "Not all tests were successfull")
}

#[test]
fn files_test() {
	let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("test").join("files");
	// remove the long comment at the end to speed up parsing a bit.
	let cleanup = |s: String| s.split_once(SEPERATOR).map(|x| x.0.to_string()).unwrap_or(s);

	stream_test(
		&path,
		Config {
			depth_limit: 1000,
			generate_warnings: true,
			feature_bearer_access: true,
			feature_surrealism: true,
			quirk_redefine: false,
			quirk_block_first_no_semi: false,
			quirk_delete_permission_field: false,
			quirk_legacy_place_productions: false,
		},
		|_| true,
		cleanup,
	);
}

#[test]
fn files_quirk_test() {
	let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("test").join("files_quirk");
	// remove the long comment at the end to speed up parsing a bit.
	let cleanup = |s: String| s.split_once(SEPERATOR).map(|x| x.0.to_string()).unwrap_or(s);

	stream_test(
		&path,
		Config {
			depth_limit: 1000,
			generate_warnings: true,
			feature_bearer_access: true,
			feature_surrealism: true,
			quirk_redefine: true,
			quirk_block_first_no_semi: true,
			quirk_delete_permission_field: true,
			quirk_legacy_place_productions: true,
		},
		|_| true,
		cleanup,
	);
}

const IGNORE_TESTS: &[&str] = &[
	"language/control_flow/transaction/cancel_behaviour.surql",
	"language/control_flow/transaction/commit_behaviour.surql",
	"datasets/surreal-deal-store-mini.surql",
	"datasets/graph.surql",
	"bench/executor/rt_import.surql",
];

#[test]
fn all_language_tests() {
	let path = Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("..")
		.join("..")
		.join("language-tests")
		.join("tests");

	let cleanup = |s: String| {
		let mut char_idx = s.char_indices();
		while let Some((_, c)) = char_idx.next() {
			if c == '/'
				&& let Some((_, '*')) = char_idx.next()
				&& let Some((_, '*')) = char_idx.next()
			{
				while let Some((_, c)) = char_idx.next() {
					if c == '*'
						&& let Some((idx, '/')) = char_idx.next()
					{
						return s[(idx + 1)..].to_string();
					}
				}
			}
		}
		s
	};

	stream_test(
		&path,
		Config {
			depth_limit: 1000,
			generate_warnings: true,
			feature_bearer_access: true,
			feature_surrealism: true,
			quirk_redefine: true,
			quirk_block_first_no_semi: true,
			quirk_delete_permission_field: true,
			quirk_legacy_place_productions: true,
		},
		|p| !IGNORE_TESTS.iter().any(|x| p.ends_with(x)),
		cleanup,
	);
}
