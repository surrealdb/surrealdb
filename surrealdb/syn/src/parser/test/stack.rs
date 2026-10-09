//! Parsing recurses on the call stack, so the depth limits in
//! [`ParserSettings`] are all that stands between nested query text and a
//! stack overflow. Every nesting construct below is nested far past each
//! limit, so the parser runs to the deepest point the limit admits and must
//! stop there with the limit's error.

use crate::parser::{Parser, ParserSettings};

const DEPTH: usize = 20_000;

/// `(prefix, open, inner, close, suffix)`: the source is
/// `RETURN {prefix}{open * DEPTH}{inner}{close * DEPTH}{suffix};`.
const CONSTRUCTS: &[(&str, &str, &str, &str, &str)] = &[
	("", "{ a: ", "1", " }", ""),
	("", "[", "1", "]", ""),
	("", "{ ", "1,", " }", ""),
	("", "(", "1", ")", ""),
	("", "{ ", "1", " }", ""),
	("", "-(", "1", ")", ""),
	("", "!", "true", "", ""),
	("", "<int>", "1", "", ""),
	("", "..=", "1", "", ""),
	("", "fn::a(", "1", ")", ""),
	("", "math::abs(", "1", ")", ""),
	("", "|$a| ", "1", "", ""),
	("", "(SELECT * FROM ", "a", ")", ""),
	("", "IF true { ", "1", " }", ""),
	("", "FOR $a IN [] { ", "1", " }", ""),
	("", "{ LET $a = ", "1", "; }", ""),
	("", "t:[", "1", "]", ""),
	("", "t:{ a: ", "1", " }", ""),
	("", "a[", "1", "]", ""),
	("", "a.{ b: ", "c", " }", ""),
	("", "a->(b WHERE ", "1", ")", ""),
	("<", "array<", "int", ">", ">1"),
];

fn assert_all_rejected(settings: &ParserSettings) {
	for (prefix, open, inner, close, suffix) in CONSTRUCTS {
		let source =
			format!("RETURN {prefix}{}{inner}{}{suffix};", open.repeat(DEPTH), close.repeat(DEPTH));
		let mut parser = Parser::new_with_settings(source.as_bytes(), settings.clone());
		let Err(e) = parser.parse_query() else {
			panic!("`{open}` nested {DEPTH} deep parsed");
		};
		let rendered = format!("{:?}", e.render_on(&source));
		assert!(rendered.contains("recursion depth limit"), "`{open}`: {rendered}");
	}
}

fn on_stack(size: usize, f: impl FnOnce() + Send + 'static) {
	std::thread::Builder::new()
		.stack_size(size)
		.spawn(f)
		.expect("spawning the parse thread")
		.join()
		.expect("parse thread panicked");
}

/// The default limits must fit a 2 MiB stack — the conservative worker-thread
/// size — in an unoptimised build, where frames are largest.
#[test]
fn default_limits_fit_a_small_stack_unoptimised() {
	on_stack(2 * 1024 * 1024, || assert_all_rejected(&ParserSettings::default()));
}

/// The stored-text ceilings are the highest the parser admits anywhere. This
/// runs unoptimised, where frames are several times larger than in the
/// optimised builds that parse stored text, so the thread is sized for that.
#[test]
fn stored_text_limits_are_enforced_before_the_stack_runs_out() {
	on_stack(16 * 1024 * 1024, || assert_all_rejected(&ParserSettings::STORED_TEXT));
}
