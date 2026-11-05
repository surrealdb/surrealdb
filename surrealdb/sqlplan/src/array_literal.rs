//! The text form of a PostgreSQL array, read as the array input of PostgreSQL reads it.

use std::iter::Peekable;
use std::str::Chars;

/// The most dimensions an array literal may have, as in PostgreSQL. It also bounds the recursion
/// of [`parse_array_literal`] on input from a client.
pub const MAX_ARRAY_DIMENSIONS: usize = 6;

/// One element of an array literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayElement {
	/// The unquoted word `NULL`, in any case.
	Null,
	/// The text of an element, with its quotes and escapes removed.
	Text(String),
	/// A sub-array of a multidimensional literal.
	Array(Vec<ArrayElement>),
}

/// Whether `c` is white space to the input of a PostgreSQL array: ASCII only, and the vertical tab
/// among it.
pub fn is_array_space(c: char) -> bool {
	matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{b}' | '\u{c}')
}

fn skip_space(chars: &mut Peekable<Chars<'_>>) {
	while chars.next_if(|c| is_array_space(*c)).is_some() {}
}

/// Split a PostgreSQL array literal such as `{a,"b c",NULL}` or `{{1,2},{3,4}}` into its
/// elements. `None` is a malformed literal.
///
/// - Space around the braces is skipped, and so is space before and after an element.
/// - A double quote starts or ends a quoted part of an element, wherever it stands in the element,
///   and is not part of the text. Inside one, a comma, a brace and space are text.
/// - A backslash makes the next character text, in or out of quotes.
/// - Space after the last character that is not space, quoted or escaped is not part of the
///   element.
/// - An element is [`ArrayElement::Null`] when it is the unquoted word `NULL`, in any case; one
///   that holds a quote or an escape is text.
/// - An unquoted `{` at the start of an element opens a sub-array. The elements of one array are
///   all sub-arrays or none are, sub-arrays at one level have the same shape, and there are at most
///   [`MAX_ARRAY_DIMENSIONS`] levels.
/// - A literal with an empty dimension, such as `{{}}` or `{{},{}}`, is the empty array.
/// - An element that has no text and no quote or escape, as in `{a,,b}`, is malformed, and so is an
///   unquoted brace inside an element, a quote that is not closed and text after the closing brace.
pub fn parse_array_literal(text: &str) -> Option<Vec<ArrayElement>> {
	let mut chars = text.chars().peekable();
	skip_space(&mut chars);
	if chars.next()? != '{' {
		return None;
	}
	let items = parse_items(&mut chars, 1)?;
	skip_space(&mut chars);
	if chars.next().is_some() {
		return None;
	}
	// An array with an empty dimension holds no element, however many levels it nests.
	if shape(&items)?.contains(&0) {
		return Some(Vec::new());
	}
	Some(items)
}

/// Split a one-dimensional text-array literal into its elements, `None` standing for a NULL
/// element. `None` is a malformed literal or one with more than one dimension; the rules are
/// those of [`parse_array_literal`].
pub(crate) fn parse_text_array_literal(text: &str) -> Option<Vec<Option<String>>> {
	parse_array_literal(text)?
		.into_iter()
		.map(|item| match item {
			ArrayElement::Null => Some(None),
			ArrayElement::Text(text) => Some(Some(text)),
			ArrayElement::Array(_) => None,
		})
		.collect()
}

/// The lengths of the dimensions of `items`, or `None` when its sub-arrays differ in shape or it
/// mixes sub-arrays and other elements.
fn shape(items: &[ArrayElement]) -> Option<Vec<usize>> {
	let mut inner: Option<Vec<usize>> = None;
	let mut nested = 0;
	for item in items {
		if let ArrayElement::Array(sub) = item {
			let sub = shape(sub)?;
			if inner.as_ref().is_some_and(|inner| *inner != sub) {
				return None;
			}
			inner = Some(sub);
			nested += 1;
		}
	}
	if nested != 0 && nested != items.len() {
		return None;
	}
	let mut dims = vec![items.len()];
	dims.extend(inner.unwrap_or_default());
	Some(dims)
}

/// The elements of an array whose `{` has been read, through its closing `}`. `depth` counts the
/// open arrays, this one included.
fn parse_items(chars: &mut Peekable<Chars<'_>>, depth: usize) -> Option<Vec<ArrayElement>> {
	if depth > MAX_ARRAY_DIMENSIONS {
		return None;
	}
	let mut items = Vec::new();
	skip_space(chars);
	if chars.next_if_eq(&'}').is_some() {
		return Some(items);
	}
	loop {
		skip_space(chars);
		let closing = if chars.next_if_eq(&'{').is_some() {
			items.push(ArrayElement::Array(parse_items(chars, depth + 1)?));
			skip_space(chars);
			match chars.next()? {
				c @ (',' | '}') => c,
				_ => return None,
			}
		} else {
			let (item, closing) = parse_element(chars)?;
			items.push(item);
			closing
		};
		if closing == '}' {
			return Some(items);
		}
	}
}

/// One element that is not a sub-array, and the `,` or `}` that ends it. Space before it has been
/// skipped.
fn parse_element(chars: &mut Peekable<Chars<'_>>) -> Option<(ArrayElement, char)> {
	let mut item = String::new();
	// The length of `item` up to its last character that is not trailing space.
	let mut significant = 0;
	let mut in_quotes = false;
	let mut quoted_or_escaped = false;
	let closing = loop {
		match chars.next()? {
			'\\' => {
				item.push(chars.next()?);
				significant = item.len();
				quoted_or_escaped = true;
			}
			'"' => {
				in_quotes = !in_quotes;
				if !in_quotes {
					significant = item.len();
				}
				quoted_or_escaped = true;
			}
			c if in_quotes => item.push(c),
			c @ (',' | '}') => break c,
			'{' => return None,
			c => {
				item.push(c);
				if !is_array_space(c) {
					significant = item.len();
				}
			}
		}
	};
	item.truncate(significant);
	if item.is_empty() && !quoted_or_escaped {
		return None;
	}
	if !quoted_or_escaped && item.eq_ignore_ascii_case("null") {
		return Some((ArrayElement::Null, closing));
	}
	Some((ArrayElement::Text(item), closing))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn text(item: &str) -> ArrayElement {
		ArrayElement::Text(item.to_string())
	}

	#[test]
	fn sub_arrays_nest_and_keep_their_elements() {
		assert_eq!(
			parse_array_literal("{{1,2},{3,4}}"),
			Some(vec![
				ArrayElement::Array(vec![text("1"), text("2")]),
				ArrayElement::Array(vec![text("3"), text("4")]),
			])
		);
		assert_eq!(
			parse_array_literal("{ { \"a,b\" , NULL } , {\"}\",\"NULL\"} }"),
			Some(vec![
				ArrayElement::Array(vec![text("a,b"), ArrayElement::Null]),
				ArrayElement::Array(vec![text("}"), text("NULL")]),
			])
		);
		assert_eq!(parse_array_literal("{{}}"), Some(Vec::new()));
		assert_eq!(parse_array_literal("{{},{}}"), Some(Vec::new()));
		assert_eq!(parse_array_literal("{{},{1}}"), None);
	}

	#[test]
	fn sub_arrays_have_one_shape_and_are_not_mixed_with_elements() {
		assert_eq!(parse_array_literal("{{1,2},{3}}"), None);
		assert_eq!(parse_array_literal("{{1},2}"), None);
		assert_eq!(parse_array_literal("{1,{2}}"), None);
		assert_eq!(parse_array_literal("{{{1}},{2}}"), None);
		assert_eq!(parse_array_literal("{{1} x}"), None, "text after a sub-array");
		assert_eq!(parse_array_literal("{a{b}}"), None, "a brace inside an element");
	}

	#[test]
	fn an_array_has_at_most_six_dimensions() {
		assert!(parse_array_literal("{{{{{{1}}}}}}").is_some());
		assert_eq!(parse_array_literal("{{{{{{{1}}}}}}}"), None);
		let deep = format!("{}{}", "{".repeat(100_000), "}".repeat(100_000));
		assert_eq!(parse_array_literal(&deep), None);
	}

	#[test]
	fn quoted_text_keeps_its_space_and_an_empty_last_element() {
		assert_eq!(parse_array_literal("{a,\"\"}"), Some(vec![text("a"), text("")]));
		assert_eq!(parse_array_literal("{\" a \"}"), Some(vec![text(" a ")]));
		assert_eq!(parse_array_literal("{\"\\\\\",\"\\\"\"}"), Some(vec![text("\\"), text("\"")]));
	}
}
