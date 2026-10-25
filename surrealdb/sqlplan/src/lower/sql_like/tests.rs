//! The pattern of a `LIKE` known only at run time converts to the same regular expression as the
//! same pattern written as a literal, and that expression matches as a `LIKE` pattern does. A
//! literal pattern too long to compile is an error.

use surrealdb_expr::expr::Function;
use surrealdb_expr::expr::logical::{LogicalExpr, LogicalLiteral};
use surrealdb_strand::Strand;

use super::like_pattern_regex;
use crate::error::PlanError;
use crate::semantics::{like_matches, sql_like_to_regex};

/// A value of the chain: the text it builds, or the answer of a test.
enum Value {
	Text(String),
	Bool(bool),
}

impl Value {
	fn text(self) -> String {
		match self {
			Value::Text(text) => text,
			Value::Bool(_) => panic!("a boolean where text belongs"),
		}
	}
}

/// Evaluates the expression `like_pattern_regex` builds, which holds only literals, `IF`s and
/// calls of `string::replace`, `string::concat` and `string::matches`, as the engine does.
fn evaluate(expr: &LogicalExpr) -> Value {
	if let LogicalExpr::Literal(LogicalLiteral::String(text)) = expr {
		return Value::Text(text.as_str().to_string());
	}
	if let LogicalExpr::If(condition) = expr {
		let (test, then) = &condition.branches[0];
		let Value::Bool(matched) = evaluate(test) else {
			panic!("a condition that is not a boolean")
		};
		return if matched {
			evaluate(then)
		} else {
			evaluate(condition.otherwise.as_ref().expect("an else branch"))
		};
	}
	let LogicalExpr::Call(call) = expr else {
		panic!("{expr:?} in the expression")
	};
	let Function::Normal(name) = &call.receiver else {
		panic!("a call of {:?}", call.receiver)
	};
	let regex = |arg: &LogicalExpr| {
		if let LogicalExpr::Literal(LogicalLiteral::Regex(regex)) = arg {
			Some(regex.0.clone())
		} else {
			None
		}
	};
	match (name.as_str(), regex(&call.args[1])) {
		("string::matches", Some(regex)) => {
			Value::Bool(regex.is_match(&evaluate(&call.args[0]).text()))
		}
		("string::replace", Some(search)) => {
			let text = evaluate(&call.args[0]).text();
			let replacement = evaluate(&call.args[2]).text();
			let mut replaced = String::new();
			let mut last = 0;
			for found in search.find_iter(&text) {
				replaced.push_str(&text[last..found.start()]);
				replaced.push_str(&replacement);
				last = found.end();
			}
			replaced.push_str(&text[last..]);
			Value::Text(replaced)
		}
		("string::replace", None) => {
			let [text, search, replacement] = [0, 1, 2].map(|at| evaluate(&call.args[at]).text());
			Value::Text(text.replace(&search, &replacement))
		}
		("string::concat", None) => {
			Value::Text(call.args.iter().map(|arg| evaluate(arg).text()).collect())
		}
		(other, _) => panic!("a call of {other}"),
	}
}

fn regex_of_chain(pattern: &str, case_insensitive: bool) -> String {
	let literal = LogicalExpr::Literal(LogicalLiteral::String(Strand::from(pattern)));
	evaluate(&like_pattern_regex(literal, case_insensitive).expect("lower the pattern")).text()
}

/// What a `LIKE` pattern means, read the way a matcher reads it: `%` is any run of characters,
/// `_` any one, a backslash makes the next character literal, and a backslash that ends the
/// pattern is itself.
fn like(subject: &[char], pattern: &[char]) -> bool {
	match pattern {
		[] => subject.is_empty(),
		['%', rest @ ..] => (0..=subject.len()).any(|skip| like(&subject[skip..], rest)),
		['_', rest @ ..] => subject.split_first().is_some_and(|(_, tail)| like(tail, rest)),
		['\\', literal, rest @ ..] | [literal, rest @ ..] => {
			subject.split_first().is_some_and(|(first, tail)| first == literal && like(tail, rest))
		}
	}
}

/// A deterministic source of numbers, so that a failure repeats.
struct Random(u64);

impl Random {
	fn next(&mut self, below: usize) -> usize {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		usize::try_from(self.0 % below as u64).expect("a small number")
	}

	fn text(&mut self, alphabet: &[char], longest: usize) -> Vec<char> {
		(0..self.next(longest + 1)).map(|_| alphabet[self.next(alphabet.len())]).collect()
	}
}

/// Characters that are syntax to a pattern, to a regular expression, to the chain itself, or
/// plain; a character of two bytes and one of three; and a line break.
const PATTERN_ALPHABET: &[char] = &[
	'a', 'b', 'A', '%', '_', '\\', '\\', '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|',
	'^', '$', '-', 'x', '0', '1', '2', '3', 'é', '日', '\n', '\u{fdd0}',
];

const SUBJECT_ALPHABET: &[char] = &['a', 'b', 'A', '%', '_', '\\', '.', '*', 'é', '日', '\n', '1'];

#[test]
fn a_pattern_known_at_run_time_converts_as_a_literal_one_does() {
	let mut random = Random(0x9e37_79b9_7f4a_7c15);
	for _ in 0..20_000 {
		let pattern: String = random.text(PATTERN_ALPHABET, 12).into_iter().collect();
		for case_insensitive in [false, true] {
			assert_eq!(
				regex_of_chain(&pattern, case_insensitive),
				sql_like_to_regex(&pattern, case_insensitive),
				"{pattern:?}"
			);
		}
	}
}

#[test]
fn the_converted_expression_matches_as_a_like_pattern_does() {
	let mut random = Random(0x2545_f491_4f6c_dd1d);
	for _ in 0..20_000 {
		let pattern = random.text(PATTERN_ALPHABET, 8);
		let subject = random.text(SUBJECT_ALPHABET, 8);
		let text: String = pattern.iter().collect();
		let regex = regex::Regex::new(&regex_of_chain(&text, false))
			.unwrap_or_else(|e| panic!("{text:?} gave an expression that does not compile: {e}"));
		let subject_text: String = subject.iter().collect();
		assert_eq!(
			regex.is_match(&subject_text),
			like(&subject, &pattern),
			"{subject_text:?} LIKE {text:?}"
		);
	}
}

#[test]
fn the_expression_of_a_pattern_is_anchored_and_spans_lines() {
	assert_eq!(regex_of_chain("a%b_", false), "(?s)^a.*b.$");
	assert_eq!(regex_of_chain("a%b_", true), "(?is)^a.*b.$");
	assert_eq!(regex_of_chain("", false), "(?s)^$");
}

#[test]
fn a_backslash_makes_the_next_character_literal() {
	assert_eq!(regex_of_chain(r"50\%", false), "(?s)^50%$");
	assert_eq!(regex_of_chain(r"a\_b", false), "(?s)^a_b$");
	assert_eq!(regex_of_chain(r"a\\%", false), r"(?s)^a\\.*$");
	assert_eq!(regex_of_chain(r"a\.b", false), r"(?s)^a\.b$");
	assert_eq!(regex_of_chain(r"a\xb", false), "(?s)^axb$");
	// A backslash that ends the pattern has nothing to escape; after an escaped backslash it is
	// the pattern's own last character.
	assert_eq!(regex_of_chain(r"a\", false), r"(?s)^a\\$");
	assert_eq!(regex_of_chain(r"a\\\", false), r"(?s)^a\\\\$");
}

#[test]
fn regular_expression_syntax_in_a_pattern_is_text() {
	let regex = regex_of_chain(".*+?()[]{}|^$", false);
	assert_eq!(regex, r"(?s)^\.\*\+\?\(\)\[\]\{\}\|\^\$$");
	let compiled = regex::Regex::new(&regex).expect("a valid expression");
	assert!(compiled.is_match(".*+?()[]{}|^$"));
	assert!(!compiled.is_match("anything else"));
}

#[test]
fn the_noncharacter_the_chain_uses_is_text_in_a_pattern() {
	let pattern = "\u{fdd0}1\u{fdd0}2\u{fdd0}3\u{fdd0}0";
	assert_eq!(regex_of_chain(pattern, false), format!("(?s)^{pattern}$"));
}

#[test]
fn a_literal_pattern_too_long_to_compile_is_an_error_not_a_mismatch() {
	let pattern = "_".repeat(1_000_000);
	let error = like_matches(&sql_like_to_regex(&pattern, false), "a").expect_err("too long");
	assert!(
		matches!(&error, PlanError::Custom(message) if message == "LIKE pattern is too complex")
	);
	assert!(like_matches(&sql_like_to_regex("a_", false), "ab").expect("a short pattern"));
	assert!(!like_matches(&sql_like_to_regex("a_", false), "abc").expect("a short pattern"));
}
