use std::path::Path;
use std::{env, fmt};

use ast::Query;

use super::{END, ResultMode, SEPERATOR, walk_dir};
use crate::Config;

fn text_test_with_path_config(path: &str, config: Config) {
	let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("test").join(path);

	let mut successfull = true;

	let env = env::var("RESULT");
	let result_mode = match env.as_deref() {
		Ok("ACCEPT") => ResultMode::Accept,
		Ok("OVERWRITE") => ResultMode::Overwrite,
		_ => ResultMode::Fail,
	};

	walk_dir(&path, &mut |path| {
		if path.extension().and_then(|x| x.to_str()) != Some("surql") {
			return;
		}

		let source = std::fs::read_to_string(path).unwrap();

		let expect = source.split_once(SEPERATOR).map(|x| x.1.strip_suffix(END).unwrap_or(x.1));

		let res = crate::Parser::enter_parse::<Query>(&source, config);

		let found = match res {
			Ok((node, ast)) => {
				fmt::from_fn(|fmt| ast::vis::visualize_ast(&node, &ast, fmt)).to_string()
			}
			Err(e) => {
				format!("ERROR:{}", e.render_char_buffer().write_to_string())
			}
		};

		if let Some(expect) = expect {
			if expect != found {
				println!("RUNNING TEST: {}", path.display());
				println!("Test failed!");
				println!("Expected:");
				println!("{}", expect);
				println!("Got:");
				println!("{}", found);

				successfull = false;

				if let ResultMode::Overwrite = result_mode {
					let source = source.split_once(SEPERATOR).map(|x| x.0).unwrap_or(&source);
					std::fs::write(path, format!("{}{}{}{}", source, SEPERATOR, found, END))
						.unwrap();
				}
			}
		} else {
			println!("RUNNING TEST: {}", path.display());
			println!("Test has no expectation");
			println!("Got:");
			println!("{}", found);

			successfull = false;

			if let ResultMode::Overwrite | ResultMode::Accept = result_mode {
				let source = source.split_once(SEPERATOR).map(|x| x.0).unwrap_or(&source);
				std::fs::write(path, format!("{}{}{}{}", source, SEPERATOR, found, END)).unwrap();
			}
		}
	});

	if !successfull {
		panic!(
			"Not all tests successfull.\nSet environment variable `RESULT` to:\n - `OVERWRITE` to overwrite the result of all tests.\n - `ACCEPT` to accept results for tests which do not have a result currently"
		);
	}
}

/// Text tests, implements a small language-test like testing suite where we test the parser
/// against a text representation of the AST if the query parsed successfully and otherwise against
/// the text formatted error.
///
/// The actual tests can be found in the `files` directory.
#[test]
fn text_test() {
	text_test_with_path_config(
		"files",
		Config {
			depth_limit: 1000,
			generate_warnings: true,
			feature_bearer_access: true,
			feature_surrealism: true,
			quirk_redefine: false,
			quirk_block_first_no_semi: false,
			quirk_delete_permission_field: false,
			quirk_legacy_place_productions: false,
			quirk_no_controlflow_check: false,
		},
	)
}

#[test]
fn text_test_quirk() {
	text_test_with_path_config(
		"files_quirk",
		Config {
			depth_limit: 1000,
			generate_warnings: true,
			feature_bearer_access: true,
			feature_surrealism: true,
			quirk_redefine: true,
			quirk_block_first_no_semi: true,
			quirk_delete_permission_field: true,
			quirk_legacy_place_productions: true,
			quirk_no_controlflow_check: true,
		},
	)
}

#[test]
fn all_language_tests() {
	let mut failed = 0;
	let mut successfull = 0;
	let tests_path = Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("..")
		.join("..")
		.join("language-tests")
		.join("tests");

	walk_dir(&tests_path, &mut |path| {
		if path.extension().and_then(|x| x.to_str()) != Some("surql") {
			return;
		}

		let source = std::fs::read_to_string(path).unwrap();
		if source.contains("parsing-error") {
			return;
		}
		// An upgrade helper marked `[test] legacy-syntax` is replayed against a
		// spawned released binary and is written in that binary's SurrealQL, in
		// a spelling this parser is never asked to read.
		if source.contains("legacy-syntax") {
			return;
		}

		let res = crate::Parser::enter_parse::<Query>(
			&source,
			Config {
				depth_limit: 1000,
				generate_warnings: true,
				feature_bearer_access: true,
				feature_surrealism: true,
				quirk_redefine: true,
				quirk_block_first_no_semi: true,
				quirk_delete_permission_field: true,
				quirk_legacy_place_productions: true,
				quirk_no_controlflow_check: true,
			},
		);

		match res {
			Ok(_) => {
				successfull += 1;
			}
			Err(e) => {
				eprintln!(
					"Language test `{}` failed to parse",
					path.strip_prefix(&tests_path).unwrap().display()
				);
				let mut buf = Vec::new();
				e.render_char_buffer().write_styled(&mut buf).unwrap();
				let s = String::from_utf8(buf).unwrap();
				eprintln!("{s}");

				failed += 1;
			}
		}
	});

	if failed != 0 {
		eprintln!("\nFailed {failed} tests, parsed {successfull} tests successfully.");
		panic!("Did not parse all the tests correctly");
	}
}
