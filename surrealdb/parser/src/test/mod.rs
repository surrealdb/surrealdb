#![cfg(test)]

use std::env;
use std::path::Path;

use ast::Query;
use common::fmt_from_fn;

use crate::Config;

const SEPERATOR: &str = "/* ===== result =====\n";
const END: &str = "\n*/";

enum ResultMode {
	Accept,
	Overwrite,
	Fail,
}

fn walk_dir<F: FnMut(&Path)>(path: &Path, f: &mut F) {
	for r in std::fs::read_dir(path).unwrap() {
		let r = r.unwrap();
		let ft = r.file_type().unwrap();
		let path = r.path();
		if ft.is_file() {
			f(&path)
		}
		if ft.is_dir() {
			walk_dir(&path, f);
		}
	}
}

/// Text tests, implements a small language-test like testing suite where we test the parser
/// against a text representation of the AST if the query parsed successfully and otherwise against
/// the text formatted error.
///
/// The actual tests can be found in the `files` directory.
#[test]
fn text_test() {
	let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("test").join("files");
	println!("{}", path.display());

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

		let res = crate::Parser::enter_parse::<Query>(
			&source,
			Config {
				depth_limit: 1000,
				generate_warnings: true,
				feature_bearer_access: true,
				feature_surrealism: true,
			},
		);

		let found = match res {
			Ok((node, ast)) => {
				fmt_from_fn(|fmt| ast::vis::visualize_ast(&node, &ast, fmt)).to_string()
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

const IGNORE_TESTS: &[&str] = &[
	"language/control_flow/transaction/cancel_behaviour.surql",
	"language/control_flow/transaction/commit_behaviour.surql",
	"language/statements/define/field/permissions_full_2.0.surql",
	"language/statements/remove/config/api.surql",
	"language/statements/remove/config/default.surql",
	"language/statements/remove/config/graphql.surql",
	"language/statements/remove/config/not_exists.surql",
	"language/statements/select/fetch/objects.surql",
	"language/statements/alter/alter_param.surql",
	"language/statements/alter/alter_user.surql",
	"language/statements/alter/alter_function.surql",
	"language/statements/alter/alter_event.surql",
	"language/statements/alter/alter_config.surql",
	"language/statements/alter/alter_bucket.surql",
	"language/statements/alter/alter_api.surql",
	"language/statements/alter/alter_analyzer.surql",
	"language/statements/alter/alter_access.surql",
	"language/graph/edge_clauses.surql",
	"reproductions/alter_auth_limit_escalation.surql",
	"reproductions/7169_from_only_in_graph_lookup.surql",
];

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

		if IGNORE_TESTS.iter().any(|x| path.ends_with(x)) {
			return;
		}

		let source = std::fs::read_to_string(path).unwrap();
		if source.contains("parsing-error") {
			return;
		}

		let res = crate::Parser::enter_parse::<Query>(
			&source,
			Config {
				depth_limit: 1000,
				generate_warnings: true,
				feature_bearer_access: true,
				feature_surrealism: true,
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
