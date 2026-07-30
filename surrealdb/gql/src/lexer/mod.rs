//! The GQL lexer.
//!
//! Implements the lexical rules of `doc/gql/REFERENCE.md` section (a),
//! transcribed from the vendored grammar `doc/gql/GQL.g4` (sections
//! 21.1-21.4). The lexer mirrors the design of the SurrealQL lexer in
//! [`crate::syn::lexer`] and reuses its [`BytesReader`].
//!
//! Notable lexical properties, all per the grammar:
//!
//! - `--` introduces a line comment, never an edge: `(a)--(b)` lexes as `(`, `a`, `)` followed by a
//!   comment. The any-direction edge is a single `-`.
//! - The compound edge tokens (`-[`, `]->`, `<~[`, …) are single tokens, lexed longest-match; the
//!   parser never assembles them from pieces.
//! - Tokens carry only a [`Span`]; the parser retrieves text via [`Lexer::span_str`] and decodes
//!   quoted values and parameter names with [`Lexer::unescape_quoted_span`] /
//!   [`Lexer::parameter_name_span`].
//! - Maximal munch is followed where the grammar splits adjacent tokens (`0X1` is `0` then `X1`,
//!   `123.` is a float even in `123..456`), but misplaced underscore digit separators are lexical
//!   errors instead of token splits, for better error messages.

mod ident;
pub mod keywords;
mod number;
mod strings;

#[cfg(test)]
mod test;

use crate::gql::token::{Token, TokenKind, t};
use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::BytesReader;
use crate::syn::token::Span;

/// The GQL lexer.
///
/// Takes a string slice and turns it into tokens. The lexer generates tokens
/// lazily: whenever [`Lexer::next_token`] is called it lexes the next bytes
/// of the source as a token. The lexer always returns a token; at the end of
/// the source it returns [`TokenKind::Eof`] and on a lexical error it returns
/// [`TokenKind::Invalid`] with the error stored in [`Lexer::error`].
pub struct Lexer<'a> {
	/// The reader for reading the source bytes.
	reader: BytesReader<'a>,
	/// The one past the last byte of the previous token.
	last_offset: u32,
	/// The error of the last lexed [`TokenKind::Invalid`] token.
	pub error: Option<SyntaxError>,
}

impl<'a> Lexer<'a> {
	/// Create a new lexer.
	///
	/// # Panic
	/// This function will panic if the source is longer than `u32::MAX`
	/// bytes. Callers exposing untrusted input must check the length first,
	/// like the parse entry points do.
	pub fn new(source: &'a str) -> Lexer<'a> {
		assert!(source.len() <= u32::MAX as usize, "source code exceeded maximum size");
		Lexer {
			reader: BytesReader::new(source.as_bytes()),
			last_offset: 0,
			error: None,
		}
	}

	/// Returns the next token, driving the lexer forward.
	///
	/// Whitespace and comments are hidden: they are skipped, not returned as
	/// tokens. If the lexer is at the end of the source it will always return
	/// the Eof token.
	pub fn next_token(&mut self) -> Token {
		// Iterative rather than recursive over hidden tokens so that inputs
		// consisting of many consecutive comments cannot overflow the stack.
		loop {
			self.last_offset = self.reader.offset();
			let Some(byte) = self.reader.next() else {
				return self.eof_token();
			};
			let token = if byte.is_ascii() {
				self.lex_ascii(byte)
			} else {
				self.lex_char(byte)
			};
			if let Some(token) = token {
				return token;
			}
		}
	}

	/// Lex a single token starting with the given, already consumed, ascii
	/// byte. Returns `None` if the byte started a hidden token (whitespace or
	/// a comment).
	fn lex_ascii(&mut self, byte: u8) -> Option<Token> {
		let kind = match byte {
			// Whitespace per `WHITESPACE` (GQL.g4:3715-3744): space, tab,
			// LF, VT, FF, CR and the C0 separators FS/GS/RS/US. The non-ascii
			// whitespace set is handled in `lex_char`.
			b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r' | 0x1C..=0x1F => {
				self.eat_whitespace();
				return None;
			}
			b'(' => t!("("),
			b')' => t!(")"),
			b'[' => t!("["),
			b']' => match self.reader.peek() {
				Some(b'-') => {
					self.reader.next();
					if self.eat(b'>') {
						t!("]->")
					} else {
						t!("]-")
					}
				}
				Some(b'~') => {
					self.reader.next();
					if self.eat(b'>') {
						t!("]~>")
					} else {
						t!("]~")
					}
				}
				_ => t!("]"),
			},
			b'{' => t!("{"),
			b'}' => t!("}"),
			b',' => t!(","),
			b'&' => t!("&"),
			b'%' => t!("%"),
			b'!' => t!("!"),
			b'?' => t!("?"),
			b'*' => t!("*"),
			b'+' => t!("+"),
			b':' => {
				if self.eat(b':') {
					t!("::")
				} else {
					t!(":")
				}
			}
			b'=' => {
				if self.eat(b'>') {
					t!("=>")
				} else {
					t!("=")
				}
			}
			b'>' => {
				if self.eat(b'=') {
					t!(">=")
				} else {
					t!(">")
				}
			}
			b'.' => {
				if self.reader.peek().is_some_and(|x| x.is_ascii_digit()) {
					return Some(self.lex_number_starting_period());
				}
				if self.eat(b'.') {
					t!("..")
				} else {
					t!(".")
				}
			}
			b'|' => {
				if self.reader.peek() == Some(b'+') && self.reader.peek1() == Some(b'|') {
					self.reader.next();
					self.reader.next();
					t!("|+|")
				} else if self.eat(b'|') {
					t!("||")
				} else {
					t!("|")
				}
			}
			// The openCypher trap: `--` is a line comment, never an edge.
			// Longest-match order: `-[`, `->`, `-/` compounds, then the `--`
			// comment, then a single `-`.
			b'-' => match self.reader.peek() {
				Some(b'-') => {
					self.reader.next();
					self.eat_line_comment();
					return None;
				}
				Some(b'[') => {
					self.reader.next();
					t!("-[")
				}
				Some(b'>') => {
					self.reader.next();
					t!("->")
				}
				Some(b'/') => {
					self.reader.next();
					t!("-/")
				}
				_ => t!("-"),
			},
			b'<' => match self.reader.peek() {
				Some(b'-') => {
					self.reader.next();
					match self.reader.peek() {
						Some(b'[') => {
							self.reader.next();
							t!("<-[")
						}
						Some(b'>') => {
							self.reader.next();
							t!("<->")
						}
						Some(b'/') => {
							self.reader.next();
							t!("<-/")
						}
						_ => t!("<-"),
					}
				}
				Some(b'~') => {
					self.reader.next();
					match self.reader.peek() {
						Some(b'[') => {
							self.reader.next();
							t!("<~[")
						}
						Some(b'/') => {
							self.reader.next();
							t!("<~/")
						}
						_ => t!("<~"),
					}
				}
				Some(b'=') => {
					self.reader.next();
					t!("<=")
				}
				Some(b'>') => {
					self.reader.next();
					t!("<>")
				}
				_ => t!("<"),
			},
			b'~' => match self.reader.peek() {
				Some(b'[') => {
					self.reader.next();
					t!("~[")
				}
				Some(b'>') => {
					self.reader.next();
					t!("~>")
				}
				Some(b'/') => {
					self.reader.next();
					t!("~/")
				}
				_ => t!("~"),
			},
			b'/' => match self.reader.peek() {
				Some(b'/') => {
					self.reader.next();
					self.eat_line_comment();
					return None;
				}
				Some(b'*') => {
					self.reader.next();
					return match self.eat_bracketed_comment() {
						Ok(()) => None,
						Err(e) => Some(self.invalid_token(e)),
					};
				}
				Some(b'-') => {
					self.reader.next();
					if self.eat(b'>') {
						t!("/->")
					} else {
						t!("/-")
					}
				}
				Some(b'~') => {
					self.reader.next();
					if self.eat(b'>') {
						t!("/~>")
					} else {
						t!("/~")
					}
				}
				_ => t!("/"),
			},
			// `@` before a quote is the `NO_ESCAPE` prefix (GQL.g4:3129),
			// disabling escape processing for the quoted sequence.
			b'@' => match self.reader.peek() {
				Some(quote @ (b'\'' | b'"' | b'`')) => {
					self.reader.next();
					return Some(self.lex_quoted_token(quote, true));
				}
				_ => t!("@"),
			},
			quote @ (b'\'' | b'"' | b'`') => return Some(self.lex_quoted_token(quote, false)),
			b'$' => return Some(self.lex_param()),
			b'0'..=b'9' => return Some(self.lex_number(byte)),
			b'a'..=b'z' | b'A'..=b'Z' | b'_' => return Some(self.lex_ident()),
			x => {
				let error = syntax_error!(
					"Unexpected character `{}`",
					(x as char).escape_debug(),
					@self.current_span()
				);
				return Some(self.invalid_token(error));
			}
		};
		Some(self.finish_token(kind))
	}

	/// Lex a single token starting with the given, already consumed,
	/// non-ascii byte. Returns `None` if the byte started a hidden token
	/// (whitespace).
	fn lex_char(&mut self, byte: u8) -> Option<Token> {
		let char = match self.reader.complete_char(byte) {
			Ok(x) => x,
			// The source is a string slice, so invalid utf-8 cannot occur,
			// but fail gracefully if it somehow does.
			Err(e) => return Some(self.invalid_token(e.into())),
		};
		if is_whitespace_char(char) {
			self.eat_whitespace();
			return None;
		}
		if ident::is_ident_start(char) {
			return Some(self.lex_ident());
		}
		let error = syntax_error!(
			"Unexpected character `{}`",
			char.escape_debug(),
			@self.current_span()
		);
		Some(self.invalid_token(error))
	}

	/// Eat a run of whitespace characters.
	fn eat_whitespace(&mut self) {
		loop {
			let Some(byte) = self.reader.peek() else {
				return;
			};
			match byte {
				b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r' | 0x1C..=0x1F => {
					self.reader.next();
				}
				x if !x.is_ascii() => {
					let backup = self.reader.offset();
					self.reader.next();
					match self.reader.complete_char(x) {
						Ok(c) if is_whitespace_char(c) => {}
						_ => {
							self.reader.backup(backup);
							return;
						}
					}
				}
				_ => return,
			}
		}
	}

	/// Eat a `//` or `--` line comment. Expects the two introducer bytes to
	/// already be consumed.
	///
	/// Per `SIMPLE_COMMENT_SOLIDUS`/`SIMPLE_COMMENT_MINUS` (GQL.g4:3748-3750)
	/// only `\r` and `\n` terminate a line comment; the terminator itself is
	/// left to be consumed as whitespace.
	fn eat_line_comment(&mut self) {
		// `\r` and `\n` cannot be part of a multi-byte utf-8 character, so
		// scanning bytes always leaves the reader on a character boundary.
		while let Some(byte) = self.reader.peek() {
			if matches!(byte, b'\r' | b'\n') {
				return;
			}
			self.reader.next();
		}
	}

	/// Eat a `/* … */` bracketed comment, erroring if `*/` is missing.
	/// Expects the `/*` introducer to already be consumed.
	///
	/// Per `BRACKETED_COMMENT` (GQL.g4:3746) the comment ends at the first
	/// `*/`: bracketed comments do not nest.
	fn eat_bracketed_comment(&mut self) -> Result<(), SyntaxError> {
		let start_span = self.current_span();
		loop {
			let Some(byte) = self.reader.next() else {
				bail!(
					"Unexpected end of file, expected the comment to be closed with `*/`",
					@start_span => "Comment starting here"
				);
			};
			// `*` and `/` cannot be part of a multi-byte utf-8 character, so
			// scanning bytes always leaves the reader on a character boundary.
			if byte == b'*' && self.eat(b'/') {
				return Ok(());
			}
		}
	}

	/// Creates the eof token.
	///
	/// An eof token has token kind Eof and a span which points to the last
	/// character of the source.
	fn eof_token(&mut self) -> Token {
		Token {
			kind: TokenKind::Eof,
			span: Span {
				offset: self.last_offset,
				len: 0,
			},
		}
	}

	/// Return an invalid token, storing the error for the parser to pick up.
	fn invalid_token(&mut self, error: SyntaxError) -> Token {
		self.error = Some(error);
		self.finish_token(TokenKind::Invalid)
	}

	/// Returns the span for the current token being lexed.
	pub fn current_span(&self) -> Span {
		// The source is no longer than u32::MAX so this can't overflow.
		Span {
			offset: self.last_offset,
			len: self.reader.offset() - self.last_offset,
		}
	}

	fn advance_span(&mut self) -> Span {
		let span = self.current_span();
		self.last_offset = self.reader.offset();
		span
	}

	/// Builds a token from a TokenKind, attaching the current span.
	fn finish_token(&mut self, kind: TokenKind) -> Token {
		Token {
			kind,
			span: self.advance_span(),
		}
	}

	/// Checks if the next byte is the given byte, if it is it consumes the
	/// byte and returns true. Otherwise returns false.
	fn eat(&mut self, byte: u8) -> bool {
		if self.reader.peek() == Some(byte) {
			self.reader.next();
			true
		} else {
			false
		}
	}

	/// Returns the string for a given span of the source.
	///
	/// Will panic if the given span was not valid for the source.
	pub fn span_str(&self, span: Span) -> &'a str {
		std::str::from_utf8(self.reader.span(span)).expect("invalid span segment for source")
	}
}

/// Returns if the character is GQL whitespace, per `WHITESPACE`
/// (GQL.g4:3715-3744). Note that the no-break spaces U+00A0, U+2007 and
/// U+202F *are* whitespace in GQL.
fn is_whitespace_char(char: char) -> bool {
	matches!(
		char,
		' ' | '\t'
			| '\n' | '\u{000B}'
			| '\u{000C}'
			| '\r' | '\u{001C}'..='\u{001F}'
			| '\u{00A0}'
			| '\u{1680}'
			| '\u{180E}'
			| '\u{2000}'..='\u{200A}'
			| '\u{2028}'
			| '\u{2029}'
			| '\u{202F}'
			| '\u{205F}'
			| '\u{3000}'
	)
}
