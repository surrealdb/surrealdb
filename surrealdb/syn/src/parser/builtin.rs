use reblessive::Stk;
use surrealdb_sql::builtin_paths::{PATHS, PathKind};
use surrealdb_sql::{Expr, Function, FunctionCall};
use unicase::UniCase;

use super::{ParseResult, Parser};
use crate::error::MessageKind;
use crate::parser::mac::expected;
use crate::parser::{SyntaxError, unexpected};
use crate::token::{Span, t};

const MAX_LEVENSTHEIN_CUT_OFF: u8 = 4;
const MAX_FUNCTION_NAME_LEN: usize = 48;
const LEVENSTHEIN_ARRAY_SIZE: usize = 1 + MAX_FUNCTION_NAME_LEN + MAX_LEVENSTHEIN_CUT_OFF as usize;

/// simple function calculating levenshtein distance with a cut-off.
///
/// levenshtein distance seems fast enough for searching possible functions to
/// suggest as the list isn't that long and the function names aren't that long.
/// Additionally this function also uses a cut off for quick rejection of
/// strings which won't lower the minimum searched distance.
///
/// Function uses stack allocated array's of size LEVENSTHEIN_ARRAY_SIZE.
/// LEVENSTHEIN_ARRAY_SIZE should the largest size in the haystack +
/// maximum cut_off + 1 for the additional value required during calculation
fn levenshtein(a: &[u8], b: &[u8], cut_off: u8) -> u8 {
	debug_assert!(LEVENSTHEIN_ARRAY_SIZE < u8::MAX as usize);
	let mut distance_array = [[0u8; LEVENSTHEIN_ARRAY_SIZE]; 2];

	if a.len().abs_diff(b.len()) > cut_off as usize {
		// moving from a to b requires atleast more then cut off insertions or deletions
		// so don't even bother.
		return cut_off + 1;
	}

	// at this point a and b shouldn't be larger then LEVENSTHEIN_ARRAY_SIZE
	// because otherwise they would have been rejected by the previous if statement.
	assert!(a.len() < LEVENSTHEIN_ARRAY_SIZE);
	assert!(b.len() < LEVENSTHEIN_ARRAY_SIZE);

	for (i, item) in distance_array[0].iter_mut().enumerate().take(a.len() + 1).skip(1) {
		*item = i as u8;
	}

	for i in 1..=b.len() {
		let current = i & 1;
		let prev = current ^ 1;
		distance_array[current][0] = i as u8;

		let mut lowest = i as u8;

		for j in 1..=a.len() {
			let cost = (a.get(j - 1).map(|x| x.to_ascii_lowercase())
				!= b.get(i - 1).map(|x| x.to_ascii_lowercase())) as u8;

			let res = (distance_array[prev][j] + 1)
				.min(distance_array[current][j - 1] + 1)
				.min(distance_array[prev][j - 1] + cost);

			distance_array[current][j] = res;
			lowest = res.min(lowest)
		}

		// The lowest value in the next calculated row will always be equal or larger
		// then the lowest value of the current row. So we can cut off search early if
		// the score can't equal the cut_off.
		if lowest > cut_off {
			return cut_off + 1;
		}
	}
	distance_array[b.len() & 1][a.len()]
}

fn find_suggestion(got: &str) -> Option<&'static str> {
	// Generate a suggestion. First look for deprecated paths.
	if let Some(surely) = PATHS.into_iter().find_map(|(path, (_, old_path))| match old_path {
		Some(s) if s.into_inner() == got => Some(path),
		_ => None,
	}) {
		return Some(surely.into_inner());
	}

	// No deprecated paths found, now use string similarity.
	// Don't search further if the levenshtein distance is greater than 4.
	let mut cut_off = MAX_LEVENSTHEIN_CUT_OFF;
	let possibly = PATHS
		.keys()
		.copied()
		.min_by_key(|x| {
			let res = levenshtein(got.as_bytes(), x.as_bytes(), cut_off);
			cut_off = res.min(cut_off);
			res
		})
		.map(|x| x.into_inner());

	if cut_off >= MAX_LEVENSTHEIN_CUT_OFF {
		return None;
	}

	possibly
}

impl Parser<'_> {
	/// Parse a builtin path.
	pub(super) async fn parse_builtin(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Expr> {
		let s = self.unescape_ident_span(start)?;
		let mut buffer = s.to_lowercase();

		let mut last_span = start;
		while self.eat(t!("::")) {
			let peek = self.peek();
			if !Self::kind_is_identifier(peek.kind) {
				unexpected!(self, peek, "an identifier")
			}
			self.pop_peek();

			buffer.push_str("::");
			let s = self.unescape_ident_span(peek.span)?;
			buffer.reserve(s.len());
			for c in s.chars() {
				for l in c.to_lowercase() {
					buffer.push(l);
				}
			}
			last_span = peek.span;
		}

		match PATHS.get_entry(&UniCase::ascii(&buffer)) {
			Some((_, (PathKind::Constant(x), _))) => Ok(Expr::Constant(x.clone())),
			Some((_, (PathKind::Function, _))) => stk
				.run(|ctx| self.parse_builtin_function(ctx, buffer))
				.await
				.map(|x| Expr::FunctionCall(Box::new(x))),
			None => {
				if let Some(suggest) = find_suggestion(&buffer) {
					Err(SyntaxError::new(format_args!(
						"Invalid function/constant path, did you maybe mean `{suggest}`"
					))
					.with_span(start.covers(last_span), MessageKind::Error))
				} else {
					Err(SyntaxError::new("Invalid function/constant path")
						.with_span(start.covers(last_span), MessageKind::Error))
				}
			}
		}
	}

	/// Parse a call to a builtin function.
	pub(super) async fn parse_builtin_function(
		&mut self,
		stk: &mut Stk,
		name: String,
	) -> ParseResult<FunctionCall> {
		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		let receiver = Function::Normal(name);
		Ok(FunctionCall {
			receiver,
			arguments: args,
		})
	}
}

#[cfg(test)]
mod test {
	use super::{MAX_FUNCTION_NAME_LEN, PATHS};

	#[test]
	fn function_name_constant_up_to_date() {
		let max = PATHS.keys().map(|x| x.len()).max().unwrap();
		// These two need to be the same but the constant needs to manually be updated
		// if PATHS ever changes so that these two values are not the same.
		assert_eq!(
			MAX_FUNCTION_NAME_LEN, max,
			"the constant MAX_FUNCTION_NAME_LEN should be {} but is {}, please update the constant",
			max, MAX_FUNCTION_NAME_LEN
		);
	}

	#[test]
	fn function_suggestion() {
		assert_eq!(super::levenshtein(b"    book", b"    ook", 5), 1);
		assert_eq!(super::find_suggestion("string::start_with"), Some("string::starts_with"));
	}
}
