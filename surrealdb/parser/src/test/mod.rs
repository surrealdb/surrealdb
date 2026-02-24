#![cfg(test)]

use std::path::Path;
use std::{env, fmt};

use ast::Query;

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
				flexible_record_ids: true,
				generate_warnings: true,
				feature_references: true,
				feature_bearer_access: true,
				feature_define_api: true,
				feature_files: true,
				legacy_strands: false,
			},
		);

		let found = match res {
			Ok((node, ast)) => {
				fmt::from_fn(|fmt| ast::vis::visualize_ast(node, &ast, fmt)).to_string()
			}
			Err(e) => {
				format!("ERROR:{}", e.render_char_buffer().to_string())
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
		panic!("Not all tests successfull")
	}
}
