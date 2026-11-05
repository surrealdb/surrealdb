//! PostgreSQL lexical analysis and tokenization.

pub mod cursor;
pub mod keyword;
pub mod split;
pub mod token;

use std::borrow::Cow;

use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::cursor::Cursor;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::{Token, TokenKind};
use crate::span::Span;

/// Lexer that scans PostgreSQL SQL input into tokens.
#[derive(Debug, Clone)]
pub struct Lexer<'a> {
	cursor: Cursor<'a>,
}

impl<'a> Lexer<'a> {
	/// Create a new lexer for the given source text.
	#[inline]
	pub const fn new(src: &'a str) -> Self {
		Self {
			cursor: Cursor::new(src),
		}
	}

	/// Return the underlying source text.
	#[inline]
	pub const fn src(&self) -> &'a str {
		self.cursor.src()
	}

	/// Return the current cursor byte position.
	#[inline]
	pub const fn pos(&self) -> u32 {
		self.cursor.pos()
	}

	/// Produce the next token from the input stream.
	pub fn next_token(&mut self) -> ParseResult<Token<'a>> {
		self.skip_whitespace_and_comments()?;

		let start = self.cursor.pos();

		let Some(c) = self.cursor.peek() else {
			return Ok(Token::new(TokenKind::Eof, Span::new(start, start)));
		};

		// 1. Quoted Identifier (`"..."`)
		if c == '"' {
			return self.lex_quoted_identifier(start);
		}

		// 2. String literal (`'...'`)
		if c == '\'' {
			return self.lex_single_quoted_string(start, false);
		}

		// 3. Escape string (`E'...'` or `e'...'`)
		if (c == 'E' || c == 'e') && self.cursor.peek_char_at(1) == Some('\'') {
			self.cursor.bump(); // consume 'E' or 'e'
			return self.lex_single_quoted_string(start, true);
		}

		// 4. Bit-string literal (`B'...'`, `b'...'`, `X'...'`, `x'...'`)
		if (c == 'B' || c == 'b' || c == 'X' || c == 'x')
			&& self.cursor.peek_char_at(1) == Some('\'')
		{
			let is_hex = c == 'X' || c == 'x';
			self.cursor.bump(); // consume prefix
			return self.lex_bit_string(start, is_hex);
		}

		// 5. Delimiters, Punctuation, and Dollar tokens
		match c {
			'$' => {
				if let Some(tok) = self.lex_dollar_token(start)? {
					return Ok(tok);
				}
			}
			',' => {
				self.cursor.bump();
				return Ok(Token::new(TokenKind::Comma, Span::new(start, self.cursor.pos())));
			}
			';' => {
				self.cursor.bump();
				return Ok(Token::new(TokenKind::Semi, Span::new(start, self.cursor.pos())));
			}
			'(' => {
				self.cursor.bump();
				return Ok(Token::new(TokenKind::LParen, Span::new(start, self.cursor.pos())));
			}
			')' => {
				self.cursor.bump();
				return Ok(Token::new(TokenKind::RParen, Span::new(start, self.cursor.pos())));
			}
			'[' => {
				self.cursor.bump();
				return Ok(Token::new(TokenKind::LBracket, Span::new(start, self.cursor.pos())));
			}
			']' => {
				self.cursor.bump();
				return Ok(Token::new(TokenKind::RBracket, Span::new(start, self.cursor.pos())));
			}
			'{' => {
				self.cursor.bump();
				return Ok(Token::new(TokenKind::LBrace, Span::new(start, self.cursor.pos())));
			}
			'}' => {
				self.cursor.bump();
				return Ok(Token::new(TokenKind::RBrace, Span::new(start, self.cursor.pos())));
			}
			':' => {
				self.cursor.bump();
				if self.cursor.eat_char(':') {
					return Ok(Token::new(
						TokenKind::DoubleColon,
						Span::new(start, self.cursor.pos()),
					));
				}
				return Ok(Token::new(TokenKind::Colon, Span::new(start, self.cursor.pos())));
			}
			'.' => {
				// Check for numeric starting with dot (e.g. `.5`)
				if matches!(self.cursor.peek_char_at(1), Some(d) if d.is_ascii_digit()) {
					return self.lex_number(start);
				}
				self.cursor.bump();
				if self.cursor.eat_char('.') {
					return Ok(Token::new(TokenKind::DotDot, Span::new(start, self.cursor.pos())));
				}
				return Ok(Token::new(TokenKind::Dot, Span::new(start, self.cursor.pos())));
			}
			_ => {}
		}

		// 7. Numbers
		if c.is_ascii_digit() {
			return self.lex_number(start);
		}

		// 8. Identifiers and Keywords
		if is_ident_start(c) {
			return self.lex_ident_or_keyword(start);
		}

		// 9. Operators
		if is_operator_char(c) {
			return self.lex_operator(start);
		}

		// Unknown character
		self.cursor.bump();
		Err(ParseError::new(Span::new(start, self.cursor.pos()), ErrorKind::UnexpectedChar(c)))
	}

	/// Skip whitespace and single/block comments.
	fn skip_whitespace_and_comments(&mut self) -> ParseResult<()> {
		loop {
			// Skip whitespace
			while let Some(c) = self.cursor.peek() {
				if c.is_ascii_whitespace() {
					self.cursor.bump();
				} else {
					break;
				}
			}

			// Check for single-line comment `--`
			if self.cursor.starts_with("--") {
				self.cursor.advance_bytes(2);
				while let Some(c) = self.cursor.bump() {
					if c == '\n' {
						break;
					}
				}
				continue;
			}

			// Check for block comment `/* ... */` with nesting
			if self.cursor.starts_with("/*") {
				let start = self.cursor.pos();
				self.cursor.advance_bytes(2);
				let mut nesting: u32 = 1;

				while nesting > 0 {
					if self.cursor.is_eof() {
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos()),
							ErrorKind::UnterminatedBlockComment,
						));
					}

					if self.cursor.starts_with("/*") {
						nesting = nesting.saturating_add(1);
						self.cursor.advance_bytes(2);
					} else if self.cursor.starts_with("*/") {
						nesting = nesting.saturating_sub(1);
						self.cursor.advance_bytes(2);
					} else {
						self.cursor.bump();
					}
				}
				continue;
			}

			break;
		}
		Ok(())
	}

	/// Lex a double-quoted identifier (`"foo""bar"`).
	fn lex_quoted_identifier(&mut self, start: u32) -> ParseResult<Token<'a>> {
		self.cursor.bump(); // consume leading quote
		let mut unescaped = String::new();

		loop {
			let Some(c) = self.cursor.bump() else {
				return Err(ParseError::new(
					Span::new(start, self.cursor.pos()),
					ErrorKind::UnterminatedQuotedIdent,
				));
			};

			if c == '"' {
				if self.cursor.eat_char('"') {
					// Doubled quote -> literal quote
					unescaped.push('"');
				} else {
					// End of quoted identifier
					break;
				}
			} else {
				unescaped.push(c);
			}
		}

		let span = Span::new(start, self.cursor.pos());
		Ok(Token::new(TokenKind::QuotedIdent(unescaped), span))
	}

	/// Lex a single-quoted string literal (`'...'` or `E'...'`).
	fn lex_single_quoted_string(&mut self, start: u32, is_escape: bool) -> ParseResult<Token<'a>> {
		self.cursor.bump(); // consume leading single quote
		let mut content = String::new();

		loop {
			let Some(c) = self.cursor.bump() else {
				return Err(ParseError::new(
					Span::new(start, self.cursor.pos()),
					ErrorKind::UnterminatedString,
				));
			};

			if c == '\'' {
				if self.cursor.eat_char('\'') {
					// Doubled quote
					content.push('\'');
				} else {
					// String completed
					break;
				}
			} else if is_escape && c == '\\' {
				// Handle escape sequence
				if let Some(escaped_char) = self.lex_escape_sequence(start)? {
					content.push(escaped_char);
				}
			} else {
				content.push(c);
			}
		}

		let span = Span::new(start, self.cursor.pos());
		Ok(Token::new(TokenKind::StringConstant(content), span))
	}

	/// Helper for C-style escape sequences in `E'...'`.
	fn lex_escape_sequence(&mut self, start: u32) -> ParseResult<Option<char>> {
		let Some(c) = self.cursor.bump() else {
			return Ok(None);
		};

		match c {
			'b' => Ok(Some('\x08')),
			'f' => Ok(Some('\x0C')),
			'n' => Ok(Some('\n')),
			'r' => Ok(Some('\r')),
			't' => Ok(Some('\t')),
			'\\' => Ok(Some('\\')),
			'\'' => Ok(Some('\'')),
			'"' => Ok(Some('"')),
			'0' if self.cursor.peek().is_none()
				|| !matches!(self.cursor.peek(), Some('0'..='7')) =>
			{
				// Single \0 -> null character
				Err(ParseError::new(
					Span::new(start, self.cursor.pos()),
					ErrorKind::InvalidCharacter("null character not permitted in string literal"),
				))
			}
			'x' => {
				// Hex byte: \xh or \xhh
				let mut val: u32 = 0;
				let mut digits = 0;
				while digits < 2 {
					if let Some(digit) = self.cursor.peek().and_then(|ch| ch.to_digit(16)) {
						self.cursor.bump();
						val = (val * 16) + digit;
						digits += 1;
					} else {
						break;
					}
				}
				if digits == 0 {
					return Err(ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter(
							"invalid hex escape sequence in string literal",
						),
					));
				}
				if val == 0 {
					return Err(ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter(
							"null character not permitted in string literal",
						),
					));
				}
				Ok(char::from_u32(val))
			}
			'0'..='7' => {
				// Octal byte: up to 3 octal digits
				let mut val = c.to_digit(8).unwrap_or(0);
				let mut digits = 1;
				while digits < 3 {
					if let Some(digit) = self.cursor.peek().and_then(|ch| ch.to_digit(8)) {
						self.cursor.bump();
						val = (val * 8) + digit;
						digits += 1;
					} else {
						break;
					}
				}
				if val == 0 {
					return Err(ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter(
							"null character not permitted in string literal",
						),
					));
				}
				Ok(char::from_u32(val))
			}
			'u' => {
				// 4-digit hex codepoint: \uXXXX
				let mut val: u32 = 0;
				for _ in 0..4 {
					if let Some(digit) = self.cursor.bump().and_then(|ch| ch.to_digit(16)) {
						val = (val * 16) + digit;
					} else {
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos()),
							ErrorKind::InvalidCharacter(
								"invalid unicode escape sequence in string literal",
							),
						));
					}
				}
				if val == 0 {
					return Err(ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter(
							"null character not permitted in string literal",
						),
					));
				}
				if (0xD800..=0xDFFF).contains(&val) {
					return Err(ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter(
							"Unicode surrogate not permitted in string literal",
						),
					));
				}
				char::from_u32(val).map(Some).ok_or_else(|| {
					ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter("invalid Unicode code point in string literal"),
					)
				})
			}
			'U' => {
				// 8-digit hex codepoint: \UXXXXXXXX
				let mut val: u32 = 0;
				for _ in 0..8 {
					if let Some(digit) = self.cursor.bump().and_then(|ch| ch.to_digit(16)) {
						val = (val * 16) + digit;
					} else {
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos()),
							ErrorKind::InvalidCharacter(
								"invalid unicode escape sequence in string literal",
							),
						));
					}
				}
				if val == 0 {
					return Err(ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter(
							"null character not permitted in string literal",
						),
					));
				}
				if (0xD800..=0xDFFF).contains(&val) {
					return Err(ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter(
							"Unicode surrogate not permitted in string literal",
						),
					));
				}
				char::from_u32(val).map(Some).ok_or_else(|| {
					ParseError::new(
						Span::new(start, self.cursor.pos()),
						ErrorKind::InvalidCharacter("invalid Unicode code point in string literal"),
					)
				})
			}
			other => Ok(Some(other)),
		}
	}

	/// Lex a bit-string literal (`B'1010'` or `X'FF'`).
	fn lex_bit_string(&mut self, start: u32, is_hex: bool) -> ParseResult<Token<'a>> {
		self.cursor.bump(); // consume opening quote `'`
		let content_start = self.cursor.pos();

		while let Some(c) = self.cursor.peek() {
			if c == '\'' {
				let content = self.cursor.slice_from(content_start).unwrap_or("");
				self.cursor.bump(); // consume closing quote
				let span = Span::new(start, self.cursor.pos());
				return Ok(Token::new(
					TokenKind::BitString {
						is_hex,
						content,
					},
					span,
				));
			}

			// Validate bit digits
			if is_hex {
				if !c.is_ascii_hexdigit() && !c.is_ascii_whitespace() {
					return Err(ParseError::new(
						Span::new(self.cursor.pos(), self.cursor.pos() + 1),
						ErrorKind::InvalidDigit,
					));
				}
			} else if c != '0' && c != '1' && !c.is_ascii_whitespace() {
				return Err(ParseError::new(
					Span::new(self.cursor.pos(), self.cursor.pos() + 1),
					ErrorKind::InvalidDigit,
				));
			}

			self.cursor.bump();
		}

		Err(ParseError::new(Span::new(start, self.cursor.pos()), ErrorKind::UnterminatedBitString))
	}

	/// Lex a dollar token: either parameter (`$1`) or dollar string (`$$...$$` or `$tag$...$tag$`).
	fn lex_dollar_token(&mut self, start: u32) -> ParseResult<Option<Token<'a>>> {
		self.cursor.bump(); // consume initial '$'

		// Case A: Positional parameter `$1`, `$2`, etc.
		if matches!(self.cursor.peek(), Some(c) if c.is_ascii_digit()) {
			let num_start = self.cursor.pos();
			while let Some(d) = self.cursor.peek() {
				if d.is_ascii_digit() {
					self.cursor.bump();
				} else {
					break;
				}
			}
			let s = self.cursor.slice_from(num_start).unwrap_or("");
			if let Ok(param_id) = s.parse::<u32>() {
				let span = Span::new(start, self.cursor.pos());
				return Ok(Some(Token::new(TokenKind::Param(param_id), span)));
			}
		}

		// Case B: Dollar-quoted string `$$` or `$tag$`
		let tag_start = self.cursor.pos();
		while let Some(c) = self.cursor.peek() {
			if c == '$' {
				break;
			}
			if is_tag_char(c) {
				self.cursor.bump();
			} else {
				// Not a valid dollar quote tag
				return Ok(None);
			}
		}

		if !self.cursor.eat_char('$') {
			return Ok(None);
		}

		let tag = self.cursor.slice_from(tag_start).unwrap_or("");
		// The tag slice currently includes the closing '$', so take up to length - 1
		let tag_name = tag.strip_suffix('$').unwrap_or(tag);
		let delimiter = format!("${}$", tag_name);

		let content_start = self.cursor.pos();
		loop {
			if self.cursor.is_eof() {
				return Err(ParseError::new(
					Span::new(start, self.cursor.pos()),
					ErrorKind::UnterminatedDollarString(tag_name.to_string()),
				));
			}

			if self.cursor.starts_with(&delimiter) {
				let content = self.cursor.slice_from(content_start).unwrap_or("");
				self.cursor.advance_bytes(delimiter.len());
				let span = Span::new(start, self.cursor.pos());
				return Ok(Some(Token::new(
					TokenKind::DollarQuotedString {
						tag: tag_name,
						content,
					},
					span,
				)));
			}

			self.cursor.bump();
		}
	}

	/// Lex a numeric literal (integer or floating point).
	fn lex_number(&mut self, start: u32) -> ParseResult<Token<'a>> {
		// Check for base prefix: 0x, 0o, 0b
		if self.cursor.peek() == Some('0') {
			match self.cursor.peek_char_at(1) {
				Some('x' | 'X') => {
					self.cursor.advance_bytes(2);
					let digits_start = self.cursor.pos();
					while let Some(c) = self.cursor.peek() {
						if c.is_ascii_hexdigit() || c == '_' {
							self.cursor.bump();
						} else {
							break;
						}
					}
					if self.cursor.pos() == digits_start {
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos() + 1),
							ErrorKind::InvalidDigit,
						));
					}
					if let Some(c) = self.cursor.peek()
						&& (c.is_alphabetic() || c == '$')
					{
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos() + 1),
							ErrorKind::InvalidDigit,
						));
					}
					let raw = self.cursor.slice_from(start).unwrap_or("");
					let span = Span::new(start, self.cursor.pos());
					return Ok(Token::new(TokenKind::IntegerRaw(raw), span));
				}
				Some('o' | 'O') => {
					self.cursor.advance_bytes(2);
					let digits_start = self.cursor.pos();
					while let Some(c) = self.cursor.peek() {
						if matches!(c, '0'..='7' | '_') {
							self.cursor.bump();
						} else {
							break;
						}
					}
					if self.cursor.pos() == digits_start {
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos() + 1),
							ErrorKind::InvalidDigit,
						));
					}
					if let Some(c) = self.cursor.peek()
						&& (c.is_alphabetic() || c == '$')
					{
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos() + 1),
							ErrorKind::InvalidDigit,
						));
					}
					let raw = self.cursor.slice_from(start).unwrap_or("");
					let span = Span::new(start, self.cursor.pos());
					return Ok(Token::new(TokenKind::IntegerRaw(raw), span));
				}
				Some('b' | 'B') => {
					self.cursor.advance_bytes(2);
					let digits_start = self.cursor.pos();
					while let Some(c) = self.cursor.peek() {
						if matches!(c, '0' | '1' | '_') {
							self.cursor.bump();
						} else {
							break;
						}
					}
					if self.cursor.pos() == digits_start {
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos() + 1),
							ErrorKind::InvalidDigit,
						));
					}
					if let Some(c) = self.cursor.peek()
						&& (c.is_alphabetic() || c == '$')
					{
						return Err(ParseError::new(
							Span::new(start, self.cursor.pos() + 1),
							ErrorKind::InvalidDigit,
						));
					}
					let raw = self.cursor.slice_from(start).unwrap_or("");
					let span = Span::new(start, self.cursor.pos());
					return Ok(Token::new(TokenKind::IntegerRaw(raw), span));
				}
				_ => {}
			}
		}

		// Decimal digits
		while let Some(c) = self.cursor.peek() {
			if c.is_ascii_digit() || c == '_' {
				self.cursor.bump();
			} else {
				break;
			}
		}

		let mut is_float = false;

		// Decimal point (check not followed by another dot like `..`)
		if self.cursor.peek() == Some('.') && self.cursor.peek_char_at(1) != Some('.') {
			is_float = true;
			self.cursor.bump(); // consume '.'
			while let Some(c) = self.cursor.peek() {
				if c.is_ascii_digit() || c == '_' {
					self.cursor.bump();
				} else {
					break;
				}
			}
		}

		// Exponent part (e.g. `e+10`, `E-3`)
		if matches!(self.cursor.peek(), Some('e' | 'E')) {
			is_float = true;
			self.cursor.bump();
			if matches!(self.cursor.peek(), Some('+' | '-')) {
				self.cursor.bump();
			}
			while let Some(d) = self.cursor.peek() {
				if d.is_ascii_digit() || d == '_' {
					self.cursor.bump();
				} else {
					break;
				}
			}
		}

		if let Some(c) = self.cursor.peek()
			&& (c.is_alphabetic() || c == '$')
		{
			return Err(ParseError::new(
				Span::new(start, self.cursor.pos() + 1),
				ErrorKind::InvalidDigit,
			));
		}

		let raw = self.cursor.slice_from(start).unwrap_or("");
		let span = Span::new(start, self.cursor.pos());

		if is_float {
			let clean = raw.replace('_', "");
			if let Ok(val) = clean.parse::<f64>() {
				Ok(Token::new(TokenKind::Float(val), span))
			} else {
				Ok(Token::new(TokenKind::FloatRaw(raw), span))
			}
		} else {
			let clean = raw.replace('_', "");
			if let Ok(val) = clean.parse::<i64>() {
				Ok(Token::new(TokenKind::Integer(val), span))
			} else {
				Ok(Token::new(TokenKind::IntegerRaw(raw), span))
			}
		}
	}

	/// Lex an identifier or keyword.
	fn lex_ident_or_keyword(&mut self, start: u32) -> ParseResult<Token<'a>> {
		while let Some(c) = self.cursor.peek() {
			if is_ident_continue(c) {
				self.cursor.bump();
			} else {
				break;
			}
		}

		let word = self.cursor.slice_from(start).unwrap_or("");
		let span = Span::new(start, self.cursor.pos());

		// Check if it is a keyword
		if let Some(kw) = Keyword::from_str_ignore_ascii_case(word) {
			return Ok(Token::new(TokenKind::Keyword(kw), span));
		}

		// PostgreSQL unquoted identifiers are case-folded to lowercase
		let is_all_lowercase = word.chars().all(|ch| !ch.is_uppercase());
		let ident = if is_all_lowercase {
			Cow::Borrowed(word)
		} else {
			Cow::Owned(word.to_lowercase())
		};

		Ok(Token::new(TokenKind::Ident(ident), span))
	}

	/// Lex an operator symbol.
	///
	/// A multi-character operator does not end in `+` or `-` unless it holds a character that is
	/// not in an SQL operator (one of `~ ! @ # % ^ & | ` ?`), so `a=-1` is `=` and `-1`, and
	/// `a->>-1` is `->>` and `-1`. The exception is `<-`, the arrow of a graph pattern edge that
	/// points left.
	fn lex_operator(&mut self, start: u32) -> ParseResult<Token<'a>> {
		let rest = self.cursor.rest().as_bytes();
		let mut len = 0;
		while let Some(&byte) = rest.get(len) {
			// Avoid eating comment starts
			if byte == b'-' && rest.get(len + 1) == Some(&b'-') {
				break;
			}
			if byte == b'/' && rest.get(len + 1) == Some(&b'*') {
				break;
			}
			if !byte.is_ascii() || !is_operator_char(char::from(byte)) {
				break;
			}
			len += 1;
		}
		let mut op = rest.get(..len).unwrap_or_default();
		let is_sign = |byte: &u8| matches!(byte, b'+' | b'-');
		let is_not_sql = |byte: &u8| {
			matches!(byte, b'~' | b'!' | b'@' | b'#' | b'%' | b'^' | b'&' | b'|' | b'`' | b'?')
		};
		if let [head @ .., last] = op
			&& is_sign(last)
			&& !head.is_empty()
			&& op != b"<-"
			&& !head.iter().any(is_not_sql)
		{
			while let [head @ .., last] = op
				&& is_sign(last)
				&& !head.is_empty()
			{
				op = head;
			}
		}
		self.cursor.advance_bytes(op.len());

		let op = self.cursor.slice_from(start).unwrap_or("");
		let span = Span::new(start, self.cursor.pos());
		Ok(Token::new(TokenKind::Operator(op), span))
	}
}

/// Helper: valid first character of an unquoted identifier.
#[inline]
fn is_ident_start(c: char) -> bool {
	c.is_ascii_alphabetic() || c == '_' || (!c.is_ascii() && c.is_alphabetic())
}

/// Helper: valid continuation character of an unquoted identifier.
#[inline]
fn is_ident_continue(c: char) -> bool {
	c.is_ascii_alphanumeric()
		|| c == '_'
		|| c == '$'
		|| (!c.is_ascii() && (c.is_alphabetic() || c.is_numeric()))
}

/// Helper: valid characters inside dollar quote tag (`$[tag]$`).
#[inline]
fn is_tag_char(c: char) -> bool {
	c.is_ascii_alphanumeric() || c == '_'
}

/// Helper: characters allowed in PostgreSQL operator names.
#[inline]
fn is_operator_char(c: char) -> bool {
	matches!(
		c,
		'+' | '-'
			| '*' | '/'
			| '%' | '^'
			| '<' | '>'
			| '=' | '~'
			| '!' | '@'
			| '#' | '&'
			| '|' | '`'
			| '?'
	)
}
