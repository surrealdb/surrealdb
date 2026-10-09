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

/// Parsing recurses on the call stack and [`crate::parse::Config::depth_limit`] is what bounds
/// it, so every nesting construct nested far past the default limit must stop with the depth
/// error. The thread is sized for an unoptimised build at the default limit, where frames are
/// roughly ten times their optimised size.
#[test]
fn nesting_past_the_depth_limit_is_an_error() {
	std::thread::Builder::new()
		.stack_size(12 * 1024 * 1024)
		.spawn(|| {
			const DEPTH: usize = 10_000;
			let cases = [
				("", "(", "1", ")", ""),
				("", "[", "1", "]", ""),
				("", "{ a: ", "1", " }", ""),
				("", "{ ", "1", " }", ""),
				("", "!", "true", "", ""),
				("", "fn::a(", "1", ")", ""),
				("", "(SELECT * FROM ", "a", ")", ""),
				("", "IF true { ", "1", " }", ""),
				("<", "array<", "int", ">", ">1"),
				("", "t:[", "1", "]", ""),
				("", "a[", "1", "]", ""),
				("", "a->(b WHERE ", "1", ")", ""),
			];
			for (prefix, open, inner, close, suffix) in cases {
				let source = format!(
					"RETURN {prefix}{}{inner}{}{suffix};",
					open.repeat(DEPTH),
					close.repeat(DEPTH)
				);
				let Err(e) = crate::Parser::enter_parse::<ast::Query>(&source, Default::default())
				else {
					panic!("`{open}` nested {DEPTH} deep parsed");
				};
				let rendered = e.render_char_buffer().write_to_string();
				assert!(
					rendered.contains("maximum configured recursion depth"),
					"{open}: {rendered}"
				);
			}
		})
		.expect("spawning the deep-parse thread")
		.join()
		.expect("deep-parse thread panicked");
}
