//! The PostgreSQL text search types: `tsvector`, written in its canonical form, and `tsquery`,
//! checked for syntax and kept as written.
//!
//! A `tsvector` holds lexemes in byte order, each once, with its positions sorted. A position is
//! 1 to 16383 and a weight is `A` to `D`; `D` is the default weight and is not written. A lexeme
//! keeps at most 256 positions and at most 2047 bytes.

use std::collections::BTreeMap;

/// The longest lexeme in bytes.
const MAX_LEXEME_BYTES: usize = 2047;
/// The highest position a `tsvector` holds.
const MAX_POSITION: u32 = 16383;
/// The most positions one lexeme keeps.
const MAX_POSITIONS: usize = 256;
/// The largest distance a phrase operator `<N>` in a `tsquery` takes.
const MAX_DISTANCE: u32 = 16384;

/// A text search syntax error. `reason` names the type and the problem.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyntaxError {
	pub reason: String,
}

/// Write the canonical form of a `tsvector`: lexemes in byte order, each quoted, with its sorted
/// positions after a colon. Positions that repeat keep the one with the higher weight, and a
/// lexeme beyond 256 positions keeps the first 256 in position order.
pub fn canonical_tsvector(text: &str) -> Result<String, SyntaxError> {
	let mut lexemes: BTreeMap<String, Vec<(u32, u8)>> = BTreeMap::new();
	let mut chars = text.chars().peekable();
	loop {
		skip_whitespace(&mut chars);
		if chars.peek().is_none() {
			break;
		}
		let lexeme = read_lexeme(&mut chars)?;
		if lexeme.len() > MAX_LEXEME_BYTES {
			return Err(syntax("word is too long in tsvector"));
		}
		let mut positions = Vec::new();
		if chars.peek() == Some(&':') {
			chars.next();
			loop {
				positions.push(read_position(&mut chars)?);
				if chars.peek() == Some(&',') {
					chars.next();
				} else {
					break;
				}
			}
		}
		match chars.peek() {
			None => {}
			Some(c) if c.is_whitespace() => {}
			Some(c) => {
				return Err(syntax(&format!("unexpected \"{c}\" after the positions of a lexeme")));
			}
		}
		lexemes.entry(lexeme).or_default().extend(positions);
	}

	let mut out = String::new();
	for (index, (lexeme, mut positions)) in lexemes.into_iter().enumerate() {
		if index > 0 {
			out.push(' ');
		}
		out.push('\'');
		for c in lexeme.chars() {
			match c {
				'\'' => out.push_str("''"),
				'\\' => out.push_str("\\\\"),
				c => out.push(c),
			}
		}
		out.push('\'');
		// Sort by position, then by weight from the highest down, so the first entry of a
		// position is the one that keeps its weight.
		positions.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
		positions.dedup_by_key(|(position, _)| *position);
		positions.truncate(MAX_POSITIONS);
		for (position_index, (position, weight)) in positions.iter().enumerate() {
			out.push(if position_index == 0 {
				':'
			} else {
				','
			});
			out.push_str(&position.to_string());
			if let Some(letter) = weight_letter(*weight) {
				out.push(letter);
			}
		}
	}
	Ok(out)
}

/// The text of a lexeme, quoted with `'` (a doubled quote stands for one, and a backslash
/// escapes the next character), or a bare run of characters up to a space or colon.
fn read_lexeme(
	chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Result<String, SyntaxError> {
	let mut lexeme = String::new();
	if chars.peek() == Some(&'\'') {
		chars.next();
		loop {
			match chars.next() {
				None => return Err(syntax("unterminated quoted lexeme")),
				Some('\'') if chars.peek() == Some(&'\'') => {
					chars.next();
					lexeme.push('\'');
				}
				Some('\'') => break,
				Some('\\') => match chars.next() {
					None => return Err(syntax("unterminated quoted lexeme")),
					Some(escaped) => lexeme.push(escaped),
				},
				Some(c) => lexeme.push(c),
			}
		}
		return Ok(lexeme);
	}
	while let Some(&c) = chars.peek() {
		if c.is_whitespace() || c == ':' {
			break;
		}
		lexeme.push(c);
		chars.next();
	}
	if lexeme.is_empty() {
		return Err(syntax("a lexeme is expected"));
	}
	Ok(lexeme)
}

/// One position with its optional weight letter.
fn read_position(
	chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Result<(u32, u8), SyntaxError> {
	let mut digits = String::new();
	while let Some(&c) = chars.peek() {
		if !c.is_ascii_digit() {
			break;
		}
		digits.push(c);
		chars.next();
	}
	let position = digits
		.parse::<u32>()
		.ok()
		.filter(|p| (1..=MAX_POSITION).contains(p))
		.ok_or_else(|| syntax("a position is a number from 1 to 16383"))?;
	let weight = match chars.peek().map(|c| c.to_ascii_uppercase()) {
		Some('A') => 3,
		Some('B') => 2,
		Some('C') => 1,
		Some('D') => 0,
		_ => return Ok((position, 0)),
	};
	chars.next();
	Ok((position, weight))
}

/// The letter a weight is written with, or `None` for the default weight `D`.
fn weight_letter(weight: u8) -> Option<char> {
	match weight {
		3 => Some('A'),
		2 => Some('B'),
		1 => Some('C'),
		_ => None,
	}
}

fn skip_whitespace(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
	while chars.peek().is_some_and(|c| c.is_whitespace()) {
		chars.next();
	}
}

fn syntax(detail: &str) -> SyntaxError {
	SyntaxError {
		reason: format!("syntax error in tsvector: {detail}"),
	}
}

/// Check the syntax of a `tsquery`: operands with optional `:` weights and `*` prefix match,
/// the operators `!`, `&`, `|`, `<->` and `<N>`, and parentheses. Precedence is `!`, then the
/// phrase operators, then `&`, then `|`.
pub fn check_tsquery(text: &str) -> Result<(), SyntaxError> {
	let tokens = tokenise_query(text)?;
	if tokens.is_empty() {
		// An empty query is valid and matches nothing.
		return Ok(());
	}
	let mut parser = QueryParser {
		tokens: &tokens,
		index: 0,
		depth: 0,
	};
	parser.or()?;
	if parser.index != tokens.len() {
		return Err(query_syntax("unexpected token after the query"));
	}
	Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueryToken {
	Operand,
	And,
	Or,
	Not,
	Open,
	Close,
	Phrase,
}

fn tokenise_query(text: &str) -> Result<Vec<QueryToken>, SyntaxError> {
	let mut tokens = Vec::new();
	let mut chars = text.chars().peekable();
	while let Some(&c) = chars.peek() {
		match c {
			c if c.is_whitespace() => {
				chars.next();
			}
			'&' => {
				chars.next();
				tokens.push(QueryToken::And);
			}
			'|' => {
				chars.next();
				tokens.push(QueryToken::Or);
			}
			'!' => {
				chars.next();
				tokens.push(QueryToken::Not);
			}
			'(' => {
				chars.next();
				tokens.push(QueryToken::Open);
			}
			')' => {
				chars.next();
				tokens.push(QueryToken::Close);
			}
			'<' => {
				chars.next();
				let mut inner = String::new();
				loop {
					match chars.next() {
						None => return Err(query_syntax("unterminated phrase operator")),
						Some('>') => break,
						Some(d) => inner.push(d),
					}
				}
				let distance_ok = inner == "-"
					|| (!inner.is_empty()
						&& inner.bytes().all(|b| b.is_ascii_digit())
						&& inner.parse::<u32>().is_ok_and(|n| n <= MAX_DISTANCE));
				if !distance_ok {
					return Err(query_syntax("a phrase operator is <-> or <N> with N up to 16384"));
				}
				tokens.push(QueryToken::Phrase);
			}
			_ => {
				read_query_operand(&mut chars)?;
				tokens.push(QueryToken::Operand);
			}
		}
	}
	Ok(tokens)
}

/// One operand: a quoted or bare lexeme, then an optional `:` with weight letters and a `*`.
fn read_query_operand(
	chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Result<(), SyntaxError> {
	if chars.peek() == Some(&'\'') {
		chars.next();
		loop {
			match chars.next() {
				None => return Err(query_syntax("unterminated quoted operand")),
				Some('\'') if chars.peek() == Some(&'\'') => {
					chars.next();
				}
				Some('\'') => break,
				Some('\\') => {
					if chars.next().is_none() {
						return Err(query_syntax("unterminated quoted operand"));
					}
				}
				Some(_) => {}
			}
		}
	} else {
		let mut length = 0;
		while let Some(&c) = chars.peek() {
			if c.is_whitespace() || "&|!()<:".contains(c) {
				break;
			}
			length += 1;
			chars.next();
		}
		if length == 0 {
			return Err(query_syntax("an operand is expected"));
		}
	}
	if chars.peek() == Some(&':') {
		chars.next();
		while let Some(&c) = chars.peek() {
			if matches!(c.to_ascii_uppercase(), 'A' | 'B' | 'C' | 'D' | '*') {
				chars.next();
			} else {
				break;
			}
		}
	}
	Ok(())
}

fn query_syntax(detail: &str) -> SyntaxError {
	SyntaxError {
		reason: format!("syntax error in tsquery: {detail}"),
	}
}

/// The deepest parenthesis nesting a tsquery may have, so a check cannot exhaust the stack.
const MAX_QUERY_DEPTH: usize = 256;

struct QueryParser<'a> {
	tokens: &'a [QueryToken],
	index: usize,
	/// The parentheses open at the current token.
	depth: usize,
}

impl QueryParser<'_> {
	fn peek(&self) -> Option<&QueryToken> {
		self.tokens.get(self.index)
	}

	fn or(&mut self) -> Result<(), SyntaxError> {
		self.and()?;
		while self.peek() == Some(&QueryToken::Or) {
			self.index += 1;
			self.and()?;
		}
		Ok(())
	}

	fn and(&mut self) -> Result<(), SyntaxError> {
		self.phrase()?;
		while self.peek() == Some(&QueryToken::And) {
			self.index += 1;
			self.phrase()?;
		}
		Ok(())
	}

	fn phrase(&mut self) -> Result<(), SyntaxError> {
		self.unary()?;
		while self.peek() == Some(&QueryToken::Phrase) {
			self.index += 1;
			self.unary()?;
		}
		Ok(())
	}

	fn unary(&mut self) -> Result<(), SyntaxError> {
		while self.peek() == Some(&QueryToken::Not) {
			self.index += 1;
		}
		match self.peek() {
			Some(QueryToken::Open) => {
				if self.depth == MAX_QUERY_DEPTH {
					return Err(query_syntax("parentheses are nested too deeply"));
				}
				self.index += 1;
				self.depth += 1;
				self.or()?;
				self.depth -= 1;
				if self.peek() != Some(&QueryToken::Close) {
					return Err(query_syntax("a parenthesis is not closed"));
				}
				self.index += 1;
				Ok(())
			}
			Some(QueryToken::Operand) => {
				self.index += 1;
				Ok(())
			}
			Some(_) | None => Err(query_syntax("an operand or an opening parenthesis is expected")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn tsv(text: &str) -> String {
		canonical_tsvector(text).expect("a tsvector")
	}

	#[test]
	fn tsvector_sorts_lexemes_and_writes_positions_in_canonical_form() {
		assert_eq!(tsv("fat:4 cat:3 fat:2"), "'cat':3 'fat':2,4");
		assert_eq!(tsv("b a c"), "'a' 'b' 'c'");
		assert_eq!(tsv("  a   b  "), "'a' 'b'");
		assert_eq!(tsv(""), "");
	}

	#[test]
	fn tsvector_merges_duplicate_lexemes_and_positions() {
		assert_eq!(tsv("cat:3,1 cat:3 cat:2"), "'cat':1,2,3");
		assert_eq!(tsv("a a a"), "'a'");
	}

	#[test]
	fn tsvector_writes_weights_and_omits_the_default_d() {
		assert_eq!(tsv("a:1A,2B,3C,4D"), "'a':1A,2B,3C,4");
		assert_eq!(tsv("a:1d"), "'a':1");
		assert_eq!(tsv("a:5a"), "'a':5A");
	}

	#[test]
	fn tsvector_keeps_the_higher_weight_of_a_repeated_position() {
		assert_eq!(tsv("a:2C,2A"), "'a':2A");
		assert_eq!(tsv("a:2A,2C"), "'a':2A");
	}

	#[test]
	fn tsvector_quotes_lexemes_and_escapes_quotes_and_backslashes() {
		// The input `'c\\d'` holds one backslash, which the output writes doubled.
		assert_eq!(tsv("'it''s' 'a b':2 'c\\\\d'"), "'a b':2 'c\\\\d' 'it''s'");
		assert_eq!(tsv("''"), "''");
		assert_eq!(tsv("'x\\'y'"), "'x''y'");
	}

	#[test]
	fn tsvector_caps_positions_at_256_in_position_order() {
		let input: String = (1..=300).rev().map(|p| format!("{p},")).collect::<String>();
		let text = format!("a:{}", input.trim_end_matches(','));
		let out = tsv(&text);
		let expected: String = (1..=256).map(|p| p.to_string()).collect::<Vec<_>>().join(",");
		assert_eq!(out, format!("'a':{expected}"));
	}

	#[test]
	fn tsvector_caps_the_lexeme_at_2047_bytes() {
		assert!(canonical_tsvector(&"x".repeat(2047)).is_ok());
		assert!(canonical_tsvector(&"x".repeat(2048)).is_err());
	}

	#[test]
	fn tsvector_refuses_bad_positions_and_syntax() {
		for text in ["a:0", "a:16384", "a:", "a:x", "a:1E", "'unterminated", "a:1x", ":1", "'a'b"] {
			assert!(canonical_tsvector(text).is_err(), "{text:?} should be refused");
		}
		assert!(canonical_tsvector("a:16383").is_ok());
		let err = canonical_tsvector("a:0").unwrap_err();
		assert!(err.reason.starts_with("syntax error in tsvector"), "{err:?}");
	}

	#[test]
	fn tsquery_accepts_operators_phrases_weights_and_prefixes() {
		for text in [
			"fat & rat",
			"fat | rat",
			"!fat",
			"(fat | rat) & cat",
			"fat <-> rat",
			"fat <2> rat",
			"fat <0> rat",
			"fat:*",
			"fat:AB*",
			"'fat rat' <-> cat",
			"!(a | b) & c",
			"a <-> !b",
			"fat",
		] {
			assert!(check_tsquery(text).is_ok(), "{text:?} should be accepted");
		}
	}

	#[test]
	fn tsquery_refuses_malformed_queries() {
		for text in [
			"fat &",
			"& fat",
			"fat && rat",
			"(fat",
			"fat)",
			"()",
			"fat <-> ",
			"fat <x> rat",
			"fat <16385> rat",
			"fat <-2> rat",
			"fat <1",
			"'unterminated",
			"fat | | rat",
			"!",
			"fat rat",
		] {
			assert!(check_tsquery(text).is_err(), "{text:?} should be refused");
		}
	}

	#[test]
	fn tsquery_nesting_is_bounded() {
		let nested = |depth: usize| format!("{}a{}", "(".repeat(depth), ")".repeat(depth));
		assert!(check_tsquery(&nested(MAX_QUERY_DEPTH)).is_ok());
		assert!(check_tsquery(&nested(MAX_QUERY_DEPTH + 1)).is_err());
		assert!(check_tsquery(&nested(100_000)).is_err());
		assert!(check_tsquery(&format!("{}a", "!".repeat(100_000))).is_ok());
	}

	#[test]
	fn tsquery_empty_text_is_accepted() {
		assert!(check_tsquery("").is_ok());
		assert!(check_tsquery("   ").is_ok());
	}
}
