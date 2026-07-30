//! Lexing and decoding of quoted character sequences.
//!
//! From `SINGLE_QUOTED_/DOUBLE_QUOTED_/ACCENT_QUOTED_CHARACTER_SEQUENCE` and
//! `ESCAPED_CHARACTER` (GQL.g4:3117-3185). All three quoted forms share the
//! same rules:
//!
//! - Raw newlines (`\r`, `\n`) are never allowed inside quotes.
//! - Doubling the quote character escapes it (`''`, `""`, ` `` `), in both normal and no-escape
//!   mode.
//! - Backslash escapes: `\\`, `\'`, `\"`, `` \` ``, `\t`, `\b`, `\n`, `\r`, `\f`, `\uXXXX` (4 hex
//!   digits), `\UXXXXXX` (6 hex digits). The escape letters are case-sensitive; the hex digits are
//!   not.
//! - The `@` prefix (`NO_ESCAPE`) disables backslash escape processing: the backslash is an
//!   ordinary character and quote doubling is the only way to include the quote character.
//!
//! Like the SurrealQL lexer, lexing only validates the token shape
//! (termination, no raw newlines); escape sequences are validated and decoded
//! when the parser requests the value via [`Lexer::unescape_quoted_span`].

use std::str::CharIndices;

use crate::gql::lexer::Lexer;
use crate::gql::token::{Token, TokenKind};
use crate::syn::error::{SyntaxError, bail};
use crate::syn::token::Span;

impl Lexer<'_> {
	/// Lex a quoted token. The opening quote, and the `@` prefix if any, must
	/// already be consumed.
	pub(super) fn lex_quoted_token(&mut self, quote: u8, no_escape: bool) -> Token {
		if let Err(e) = self.lex_quoted(quote, no_escape) {
			return self.invalid_token(e);
		}
		let kind = match quote {
			b'\'' => TokenKind::SingleQuoted {
				no_escape,
			},
			b'"' => TokenKind::DoubleQuoted {
				no_escape,
			},
			_ => TokenKind::AccentQuoted {
				no_escape,
			},
		};
		self.finish_token(kind)
	}

	/// Scan a quoted character sequence up to and including the closing
	/// quote, validating only termination and the absence of raw newlines.
	pub(super) fn lex_quoted(&mut self, quote: u8, no_escape: bool) -> Result<(), SyntaxError> {
		let start_span = self.current_span();
		loop {
			let Some(byte) = self.reader.next() else {
				bail!(
					"Unexpected end of file, expected the quoted sequence to end",
					@start_span => "Quoted sequence starting here"
				);
			};
			match byte {
				b'\r' | b'\n' => {
					let span = Span {
						offset: self.reader.offset() - 1,
						len: 1,
					};
					bail!(
						"Quoted strings and identifiers may not contain raw newline characters, use the `\\n` escape sequence instead",
						@span,
						@start_span => "Quoted sequence starting here"
					);
				}
				x if x == quote => {
					// A doubled quote character escapes the quote.
					if self.eat(quote) {
						continue;
					}
					return Ok(());
				}
				b'\\' if !no_escape => {
					// Skip whatever character follows so an escaped quote
					// does not end the sequence. Escape sequences are
					// validated when the value is decoded.
					let Some(next) = self.reader.next() else {
						bail!(
							"Unexpected end of file, expected the quoted sequence to end",
							@start_span => "Quoted sequence starting here"
						);
					};
					match next {
						b'\r' | b'\n' => {
							let span = Span {
								offset: self.reader.offset() - 1,
								len: 1,
							};
							bail!(
								"Quoted strings and identifiers may not contain raw newline characters, use the `\\n` escape sequence instead",
								@span,
								@start_span => "Quoted sequence starting here"
							);
						}
						x if !x.is_ascii() => {
							self.reader.complete_char(x)?;
						}
						_ => {}
					}
				}
				x if !x.is_ascii() => {
					self.reader.complete_char(x)?;
				}
				_ => {}
			}
		}
	}

	/// Decodes the value of a quoted token: a string literal or a delimited
	/// identifier, in normal or no-escape (`@`) mode.
	///
	/// `text` must be the full token text as returned by [`Lexer::span_str`]
	/// for the token's span — the optional `@` prefix, the quotes and the
	/// content — and `span` must be that token's span within the source, used
	/// to attach precise sub-spans to escape sequence errors.
	pub fn unescape_quoted_span(text: &str, span: Span) -> Result<String, SyntaxError> {
		let mut chars = text.char_indices();
		let (no_escape, quote) = match chars.next() {
			Some((_, '@')) => match chars.next() {
				Some((_, quote @ ('\'' | '"' | '`'))) => (true, quote),
				_ => bail!("Expected a quoted token", @span),
			},
			Some((_, quote @ ('\'' | '"' | '`'))) => (false, quote),
			_ => bail!("Expected a quoted token", @span),
		};
		let mut result = String::with_capacity(text.len());
		while let Some((at, char)) = chars.next() {
			if char == quote {
				match chars.next() {
					Some((_, x)) if x == quote => result.push(quote),
					// The closing quote; the lexer guarantees it is last.
					_ => break,
				}
			} else if char == '\\' && !no_escape {
				Self::unescape_escape_sequence(&mut chars, span, at, &mut result)?;
			} else {
				result.push(char);
			}
		}
		Ok(result)
	}

	/// Decodes the name of a parameter token, stripping the `$`/`$$`
	/// introducer and unquoting a delimited name.
	///
	/// `text` must be the full token text as returned by [`Lexer::span_str`]
	/// for the token's span and `span` must be that token's span within the
	/// source.
	pub fn parameter_name_span(text: &str, span: Span) -> Result<String, SyntaxError> {
		let Some(name) = text.strip_prefix("$$").or_else(|| text.strip_prefix('$')) else {
			bail!("Expected a parameter token", @span);
		};
		if name.starts_with(['"', '`', '@']) {
			let prefix_len = (text.len() - name.len()) as u32;
			let span = Span {
				offset: span.offset + prefix_len,
				len: span.len - prefix_len,
			};
			return Self::unescape_quoted_span(name, span);
		}
		Ok(name.to_owned())
	}

	/// Decodes a single escape sequence; the introducing `\` was already
	/// consumed at byte position `at` within the token text.
	fn unescape_escape_sequence(
		chars: &mut CharIndices,
		span: Span,
		at: usize,
		result: &mut String,
	) -> Result<(), SyntaxError> {
		let Some((_, char)) = chars.next() else {
			let span = Span {
				offset: span.offset + at as u32,
				len: 1,
			};
			bail!("Invalid escape sequence, missing the escape character", @span);
		};
		// The escape letters are case-sensitive (GQL.g4:3171-3184): `\N` is
		// not a newline escape.
		match char {
			'\\' => result.push('\\'),
			'\'' => result.push('\''),
			'"' => result.push('"'),
			'`' => result.push('`'),
			't' => result.push('\t'),
			'b' => result.push('\u{0008}'),
			'n' => result.push('\n'),
			'r' => result.push('\r'),
			'f' => result.push('\u{000C}'),
			'u' => result.push(Self::unescape_unicode_sequence(chars, span, at, 4)?),
			'U' => result.push(Self::unescape_unicode_sequence(chars, span, at, 6)?),
			x => {
				let span = Span {
					offset: span.offset + at as u32,
					len: 1 + x.len_utf8() as u32,
				};
				bail!(
					"Invalid escape sequence `\\{}`",
					x.escape_debug(),
					@span => "Expected one of `\\`, `'`, `\"`, `` ` ``, `t`, `b`, `n`, `r`, `f`, `uXXXX` or `UXXXXXX`; escape letters are case-sensitive"
				);
			}
		}
		Ok(())
	}

	/// Decodes a `\uXXXX` or `\UXXXXXX` escape sequence; `\u`/`\U` were
	/// already consumed, starting at byte position `at` within the token
	/// text.
	fn unescape_unicode_sequence(
		chars: &mut CharIndices,
		span: Span,
		at: usize,
		digits: u32,
	) -> Result<char, SyntaxError> {
		let mut value: u32 = 0;
		for i in 0..digits {
			// The hex digits, unlike the escape letters, are
			// case-insensitive.
			let digit = chars.next().and_then(|(_, x)| x.to_digit(16));
			let Some(digit) = digit else {
				let span = Span {
					offset: span.offset + at as u32,
					// The `\\`, the escape letter, and the valid digits.
					len: 2 + i,
				};
				bail!(
					"Invalid unicode escape sequence, expected {digits} hexadecimal digits",
					@span
				);
			};
			value = (value << 4) | digit;
		}
		let Some(char) = char::from_u32(value) else {
			let span = Span {
				offset: span.offset + at as u32,
				len: 2 + digits,
			};
			bail!(
				"Invalid unicode escape sequence, `{value:#x}` is not a valid unicode code point",
				@span
			);
		};
		Ok(char)
	}
}
