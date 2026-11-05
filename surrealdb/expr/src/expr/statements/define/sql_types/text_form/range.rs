//! The text forms of PostgreSQL range and multirange values.
//!
//! The functions are generic over the element type. Each takes the element's text function, so
//! the caller decides how a bound is written and read. They do not canonicalise: a range read
//! keeps the bounds and inclusivity it was written with, and the range family applies the
//! canonical form of the declared type.
//!
//! A range is `[lower,upper)` with `(` or `[` before the lower bound and `)` or `]` after the
//! upper bound, or `empty`. An unbounded side is an empty bound, and it is always exclusive. A
//! bound that is empty, or that holds `"`, `\`, a bracket, a comma, or white space, is
//! double-quoted, with `"` and `\` doubled inside the quotes. A multirange is `{r1,r2}`, and `{}`
//! is the empty multirange.

use std::ops::Bound;

/// A range value: the empty range, or a span with a lower and an upper bound.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Range<T> {
	Empty,
	Span {
		lower: Bound<T>,
		upper: Bound<T>,
	},
}

/// A range or multirange text that does not read, with the reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeTextError {
	pub message: String,
}

pub type TextResult<T> = Result<T, RangeTextError>;

fn error(message: impl Into<String>) -> RangeTextError {
	RangeTextError {
		message: message.into(),
	}
}

/// The text form of a range. `element` writes a bounded side's value; it is quoted when needed.
pub fn encode_text<T>(range: &Range<T>, element: impl Fn(&T) -> String) -> String {
	match range {
		Range::Empty => "empty".to_string(),
		Range::Span {
			lower,
			upper,
		} => {
			let mut out = String::new();
			match lower {
				Bound::Included(value) => {
					out.push('[');
					out.push_str(&quote_bound(&element(value)));
				}
				Bound::Excluded(value) => {
					out.push('(');
					out.push_str(&quote_bound(&element(value)));
				}
				Bound::Unbounded => out.push('('),
			}
			out.push(',');
			match upper {
				Bound::Included(value) => {
					out.push_str(&quote_bound(&element(value)));
					out.push(']');
				}
				Bound::Excluded(value) => {
					out.push_str(&quote_bound(&element(value)));
					out.push(')');
				}
				Bound::Unbounded => out.push(')'),
			}
			out
		}
	}
}

/// The text form of a multirange, `{r1,r2}`.
pub fn encode_multirange_text<T>(ranges: &[Range<T>], element: impl Fn(&T) -> String) -> String {
	let parts: Vec<String> = ranges.iter().map(|range| encode_text(range, &element)).collect();
	format!("{{{}}}", parts.join(","))
}

/// Read the text form of a range. `parse` reads a bound's text, unquoted and unescaped, and
/// returns `None` when the text is not a value of the element type.
pub fn decode_text<T>(text: &str, parse: impl Fn(&str) -> Option<T>) -> TextResult<Range<T>> {
	let trimmed = text.trim();
	if trimmed.eq_ignore_ascii_case("empty") {
		return Ok(Range::Empty);
	}
	let chars: Vec<char> = trimmed.chars().collect();
	let (range, end) = parse_span(&chars, 0, &parse)?;
	if end != chars.len() {
		return Err(error(format!("unexpected text after the range: {trimmed}")));
	}
	Ok(range)
}

/// Read the text form of a multirange, `{r1,r2}` or `{}`.
pub fn decode_multirange_text<T>(
	text: &str,
	parse: impl Fn(&str) -> Option<T>,
) -> TextResult<Vec<Range<T>>> {
	let chars: Vec<char> = text.trim().chars().collect();
	if chars.len() < 2 || chars.first() != Some(&'{') || chars.last() != Some(&'}') {
		return Err(error("a multirange is written between { and }"));
	}
	let inner = &chars[1..chars.len() - 1];
	let mut ranges = Vec::new();
	let mut at = skip_space(inner, 0);
	if at == inner.len() {
		return Ok(ranges);
	}
	loop {
		if starts_with_empty(inner, at) {
			ranges.push(Range::Empty);
			at += "empty".len();
		} else {
			let (range, next) = parse_span(inner, at, &parse)?;
			ranges.push(range);
			at = next;
		}
		at = skip_space(inner, at);
		match inner.get(at) {
			None => return Ok(ranges),
			Some(',') => at = skip_space(inner, at + 1),
			Some(other) => {
				return Err(error(format!("expected a comma between ranges, found {other}")));
			}
		}
	}
}

/// Quote a bound's text when it is empty or holds a character the text form reserves.
fn quote_bound(text: &str) -> String {
	let needs_quotes = text.is_empty()
		|| text
			.chars()
			.any(|c| c.is_whitespace() || matches!(c, '"' | '\\' | '(' | ')' | '[' | ']' | ','));
	if !needs_quotes {
		return text.to_string();
	}
	let mut out = String::with_capacity(text.len() + 2);
	out.push('"');
	for c in text.chars() {
		if matches!(c, '"' | '\\') {
			out.push(c);
		}
		out.push(c);
	}
	out.push('"');
	out
}

fn skip_space(chars: &[char], mut at: usize) -> usize {
	while chars.get(at).is_some_and(|c| c.is_whitespace()) {
		at += 1;
	}
	at
}

fn starts_with_empty(chars: &[char], at: usize) -> bool {
	let word: Vec<char> = "empty".chars().collect();
	chars
		.get(at..at + word.len())
		.is_some_and(|slice| slice.iter().zip(&word).all(|(a, b)| a.eq_ignore_ascii_case(b)))
		&& chars.get(at + word.len()).is_none_or(|c| matches!(c, ',' | '}') || c.is_whitespace())
}

/// Read one span starting at `start`, returning it with the index just past its closing bracket.
fn parse_span<T>(
	chars: &[char],
	start: usize,
	parse: &impl Fn(&str) -> Option<T>,
) -> TextResult<(Range<T>, usize)> {
	let lower_inclusive = match chars.get(start) {
		Some('[') => true,
		Some('(') => false,
		_ => return Err(error("a range starts with [ or (")),
	};
	let (lower_text, at) = read_bound(chars, start + 1, &[','])?;
	if chars.get(at) != Some(&',') {
		return Err(error("a range has no comma between its bounds"));
	}
	let (upper_text, at) = read_bound(chars, at + 1, &[']', ')'])?;
	let upper_inclusive = match chars.get(at) {
		Some(']') => true,
		Some(')') => false,
		_ => return Err(error("a range ends with ] or )")),
	};
	let lower = match lower_text {
		None => Bound::Unbounded,
		Some(text) => bound(read_text(&text, parse)?, lower_inclusive),
	};
	let upper = match upper_text {
		None => Bound::Unbounded,
		Some(text) => bound(read_text(&text, parse)?, upper_inclusive),
	};
	Ok((
		Range::Span {
			lower,
			upper,
		},
		at + 1,
	))
}

/// Read one bound up to a terminator, which is not consumed. Returns `None` for an unquoted empty
/// bound, which is unbounded; a quoted empty bound is the empty text.
fn read_bound(
	chars: &[char],
	start: usize,
	terminators: &[char],
) -> TextResult<(Option<String>, usize)> {
	let mut out = String::new();
	let mut quoted = false;
	let mut saw_quote = false;
	let mut escaped = false;
	let mut at = start;
	loop {
		let Some(&c) = chars.get(at) else {
			return Err(error("a range is not closed"));
		};
		if escaped {
			out.push(c);
			escaped = false;
		} else if c == '\\' {
			escaped = true;
		} else if c == '"' {
			if quoted && chars.get(at + 1) == Some(&'"') {
				out.push('"');
				at += 1;
			} else {
				quoted = !quoted;
				saw_quote = true;
			}
		} else if !quoted && terminators.contains(&c) {
			break;
		} else {
			out.push(c);
		}
		at += 1;
	}
	if quoted {
		return Err(error("a quoted range bound is not closed"));
	}
	if saw_quote {
		return Ok((Some(out), at));
	}
	let trimmed = out.trim();
	if trimmed.is_empty() {
		Ok((None, at))
	} else {
		Ok((Some(trimmed.to_string()), at))
	}
}

fn read_text<T>(text: &str, parse: &impl Fn(&str) -> Option<T>) -> TextResult<T> {
	parse(text)
		.ok_or_else(|| error(format!("a range bound is not a value of the element type: {text}")))
}

/// A finite bound, inclusive or exclusive.
pub fn bound<T>(value: T, inclusive: bool) -> Bound<T> {
	if inclusive {
		Bound::Included(value)
	} else {
		Bound::Excluded(value)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn int_text(text: &str) -> Option<i32> {
		text.parse().ok()
	}

	#[expect(
		clippy::trivially_copy_pass_by_ref,
		reason = "the encoder hands each bound by reference"
	)]
	fn int_to_text(value: &i32) -> String {
		value.to_string()
	}

	fn span(lower: Bound<i32>, upper: Bound<i32>) -> Range<i32> {
		Range::Span {
			lower,
			upper,
		}
	}

	#[test]
	fn text_form_writes_brackets_and_unbounded_sides() {
		assert_eq!(
			encode_text(&span(Bound::Included(1), Bound::Excluded(5)), int_to_text),
			"[1,5)"
		);
		assert_eq!(encode_text(&span(Bound::Unbounded, Bound::Included(3)), int_to_text), "(,3]");
		assert_eq!(encode_text(&span(Bound::Excluded(2), Bound::Unbounded), int_to_text), "(2,)");
		assert_eq!(encode_text(&Range::<i32>::Empty, int_to_text), "empty");
	}

	#[test]
	fn text_form_quotes_a_bound_that_holds_a_reserved_character() {
		let text = |s: &str| s.to_string();
		let range = Range::Span {
			lower: Bound::Included(text("a,b")),
			upper: Bound::Excluded(text("")),
		};
		assert_eq!(encode_text(&range, |s| s.clone()), "[\"a,b\",\"\")");
		let escaped = Range::Span {
			lower: Bound::Included(text("say \"hi\"\\")),
			upper: Bound::Unbounded,
		};
		assert_eq!(encode_text(&escaped, |s| s.clone()), "[\"say \"\"hi\"\"\\\\\",)");
	}

	#[test]
	fn text_form_reads_each_shape_back() {
		assert_eq!(
			decode_text("[1,5)", int_text).unwrap(),
			span(Bound::Included(1), Bound::Excluded(5))
		);
		assert_eq!(
			decode_text("(,3]", int_text).unwrap(),
			span(Bound::Unbounded, Bound::Included(3))
		);
		assert_eq!(decode_text(" EMPTY ", int_text).unwrap(), Range::Empty);
		assert_eq!(
			decode_text("( 1 , 5 ]", int_text).unwrap(),
			span(Bound::Excluded(1), Bound::Included(5))
		);
	}

	#[test]
	fn text_form_reads_quoted_bounds_with_their_escapes() {
		let parse = |s: &str| Some(s.to_string());
		let range = decode_text("[\"a,b\",\"\")", parse).unwrap();
		assert_eq!(
			range,
			Range::Span {
				lower: Bound::Included("a,b".to_string()),
				upper: Bound::Excluded(String::new()),
			}
		);
		let range = decode_text("[\"say \"\"hi\"\"\\\\\",)", parse).unwrap();
		assert_eq!(
			range,
			Range::Span {
				lower: Bound::Included("say \"hi\"\\".to_string()),
				upper: Bound::Unbounded,
			}
		);
	}

	#[test]
	fn text_form_round_trips_through_its_quoted_output() {
		let parse = |s: &str| Some(s.to_string());
		let original = Range::Span {
			lower: Bound::Excluded("x y".to_string()),
			upper: Bound::Included("\"q\"".to_string()),
		};
		let text = encode_text(&original, |s| s.clone());
		assert_eq!(decode_text(&text, parse).unwrap(), original);
	}

	#[test]
	fn text_form_refuses_malformed_input_and_bad_bounds() {
		assert!(decode_text("[1,5", int_text).is_err());
		assert!(decode_text("1,5)", int_text).is_err());
		assert!(decode_text("[1 5)", int_text).is_err());
		assert!(decode_text("[a,5)", int_text).is_err());
		assert!(decode_text("[1,5) x", int_text).is_err());
		assert!(decode_text("[\"1,5)", int_text).is_err());
	}

	#[test]
	fn multirange_text_writes_and_reads_its_ranges() {
		let ranges = vec![
			span(Bound::Included(1), Bound::Excluded(3)),
			span(Bound::Included(5), Bound::Excluded(7)),
		];
		let text = encode_multirange_text(&ranges, int_to_text);
		assert_eq!(text, "{[1,3),[5,7)}");
		assert_eq!(decode_multirange_text(&text, int_text).unwrap(), ranges);
		assert_eq!(encode_multirange_text::<i32>(&[], int_to_text), "{}");
		assert_eq!(decode_multirange_text("{}", int_text).unwrap(), Vec::<Range<i32>>::new());
		assert_eq!(
			decode_multirange_text("{ [1,3) , empty }", int_text).unwrap(),
			vec![span(Bound::Included(1), Bound::Excluded(3)), Range::Empty]
		);
	}

	#[test]
	fn multirange_text_refuses_a_missing_brace_or_separator() {
		assert!(decode_multirange_text("[1,3)", int_text).is_err());
		assert!(decode_multirange_text("{[1,3) [5,7)}", int_text).is_err());
		assert!(decode_multirange_text("{[1,3),}", int_text).is_err());
	}
}
