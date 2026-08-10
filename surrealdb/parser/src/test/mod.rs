#![cfg(test)]

//! This crate has it's own small test suite.
//! If you need to add a test which just tests the parsing of some surrealql source
//! please add a file test instead of a function test.
//!
//! You generally should not have to add a test here unless you are testing some uncommon
//! functionality of the parser or behavior not normally triggered.
//!
//! All `.surql` files under the `files` and `files_quirk` directory are tested in
//! the `text_test` and `text_test_quirk` test and not only test for parsing errors
//! but also tests against the generated AST.
//!
//! Test generation has a similar workflow to the language-test: First write some query to test the
//! parser again, then run the test to see if the parser produces the expected output and finally
//! use the `RESULT` environment variable setting it to either `RESULT=ACCEPT` to write expected
//! output to tests which do not have an expected output yet or `RESULT=OVERWRITE` to write output
//! to tests which already have an expected output but the actual expected output has changed.

mod stream;
mod text;

use std::path::Path;

const SEPERATOR: &str = "/* ===== result =====\n";
const END: &str = "\n*/";

enum ResultMode {
	Accept,
	Overwrite,
	Fail,
}
fn walk_dir<F: FnMut(&Path)>(path: &Path, f: &mut F) {
	let mut dirs = vec![path.to_path_buf()];
	while let Some(path) = dirs.pop() {
		for r in std::fs::read_dir(path).unwrap() {
			let r = r.unwrap();
			let ft = r.file_type().unwrap();
			let path = r.path();
			if ft.is_file() {
				f(&path)
			}
			if ft.is_dir() {
				dirs.push(path);
			}
		}
	}
}
