use ast::NodeId;
use common::source_error::{AnnotationKind, Level, Snippet};
use common::span::Span;
use logos::Logos;
use token::{BaseTokenKind, EscapeTokenKind, Token};

use crate::parse::{ParseError, ParseResult, Parser};

impl<'source, 'ast> Parser<'source, 'ast> {
	pub fn unescape_ident(&mut self, token: Token) -> ParseResult<&str> {
		assert!(token.token.is_identifier());
		let slice = self.slice(token.span);
		if slice.starts_with('`') {
			self.unescape_backtick_ident(token.span, slice)
		} else if slice.starts_with('⟨') {
			self.unescape_bracket_ident(token.span, slice)
		} else {
			// Already a valid identifier.
			Ok(slice)
		}
	}

	pub fn unescape_param(&mut self, token: Token) -> ParseResult<&str> {
		assert_eq!(token.token, BaseTokenKind::Param);
		let slice = self.slice(token.span);
		if slice.starts_with("$`") {
			let mut span = token.span;
			span.start += 1;
			self.unescape_backtick_ident(span, &slice[1..])
		} else if slice.starts_with("$⟨") {
			let mut span = token.span;
			span.start += 1;
			self.unescape_bracket_ident(span, &slice[1..])
		} else {
			// Already a valid identifier.
			Ok(&slice[1..])
		}
	}

	pub fn unescape_common<'a>(
		slice_span: Span,
		unescape_source: &'a str,
		full_source: &'a str,
		buffer: &'a mut String,
	) -> ParseResult<&'a str> {
		buffer.clear();

		let mut lexer = EscapeTokenKind::lexer(unescape_source);
		let mut pending_span = 0..0;
		loop {
			let Some(next) = lexer.next() else {
				// Fast path for if there are no escape sequences.
				if pending_span.len() == unescape_source.len() {
					return Ok(unescape_source);
				}
				buffer.push_str(&unescape_source[pending_span]);
				break;
			};

			let span = lexer.span();

			let next =
				match next {
					Ok(x) => x,
					Err(()) => {
						let span = Span::from_usize_range(span)
							.expect("Source to be shorter the u32::MAX");
						return Err(ParseError::diagnostic(
							Level::Error
								.title("Invalid escape sequence")
								.snippet(Snippet::source(full_source).annotate(
									AnnotationKind::Primary.span(slice_span.sub_span(span)),
								))
								.to_diagnostic()
								.to_owned(),
						));
					}
				};

			match next {
				EscapeTokenKind::Chars => {
					pending_span.end = span.end;
				}
				x => {
					buffer.push_str(&unescape_source[pending_span]);
					pending_span = span.end..span.end;
					if !Self::handle_escape(buffer, &unescape_source[span.clone()], x) {
						let span = Span::from_usize_range(span)
							.expect("Source to be shorter the u32::MAX");
						return Err(ParseError::diagnostic(
							Level::Error
								.title("Invalid escape sequence")
								.snippet(Snippet::source(full_source).annotate(
									AnnotationKind::Primary.span(slice_span.sub_span(span)),
								))
								.to_diagnostic()
								.to_owned(),
						));
					}
				}
			}
		}

		Ok(buffer.as_str())
	}

	fn handle_escape(buffer: &mut String, slice: &str, token: EscapeTokenKind) -> bool {
		match token {
			EscapeTokenKind::EscNewline => {
				buffer.push('\n');
				true
			}
			EscapeTokenKind::EscCarriageReturn => {
				buffer.push('\r');
				true
			}
			EscapeTokenKind::EscTab => {
				buffer.push('\t');
				true
			}
			EscapeTokenKind::EscZeroByte => {
				buffer.push('\0');
				true
			}
			EscapeTokenKind::EscBackSlash => {
				buffer.push('\\');
				true
			}
			EscapeTokenKind::EscBackSpace => {
				buffer.push('\x08');
				true
			}
			EscapeTokenKind::EscFormFeed => {
				buffer.push('\x0C');
				true
			}
			EscapeTokenKind::EscQuote => {
				buffer.push('\'');
				true
			}
			EscapeTokenKind::EscDoubleQuote => {
				buffer.push('\"');
				true
			}
			EscapeTokenKind::EscBackTick => {
				buffer.push('`');
				true
			}
			EscapeTokenKind::EscBracketClose => {
				buffer.push('⟩');
				true
			}
			EscapeTokenKind::EscUnicode => {
				let bytes = &slice.as_bytes()[b"\\u{".len()..slice.len() - 1];
				let mut char = 0u32;
				for b in bytes {
					char <<= 4;
					match *b {
						x @ b'0'..=b'9' => {
							char += (x - b'0') as u32;
						}
						x @ b'A'..=b'F' => {
							char += (x - b'A' + 10) as u32;
						}
						x @ b'a'..=b'f' => {
							char += (x - b'a' + 10) as u32;
						}
						_ => unreachable!(),
					}
				}
				if let Some(x) = char::from_u32(char) {
					buffer.push(x);
					true
				} else {
					false
				}
			}
			EscapeTokenKind::Chars => unreachable!(),
		}
	}

	fn unescape_bracket_ident<'a>(
		&'a mut self,
		mut slice_span: Span,
		slice: &'a str,
	) -> ParseResult<&'a str> {
		let start_offset = const { '⟨'.len_utf8() };
		let end_offset = const { '⟩'.len_utf8() };
		let slice = &slice[start_offset..(slice.len() - end_offset)];
		slice_span.start += start_offset as u32;
		slice_span.end -= end_offset as u32;

		Self::unescape_common(slice_span, slice, self.source(), &mut self.unescape_buffer)
	}

	fn unescape_backtick_ident<'a>(
		&'a mut self,
		mut slice_span: Span,
		slice: &'a str,
	) -> ParseResult<&'a str> {
		let start_offset = const { '`'.len_utf8() };
		let end_offset = const { '`'.len_utf8() };
		let slice = &slice[start_offset..(slice.len() - end_offset)];
		slice_span.start += start_offset as u32;
		slice_span.end -= end_offset as u32;

		Self::unescape_common(slice_span, slice, self.source(), &mut self.unescape_buffer)
	}

	pub fn unescape_str<'a>(&'a mut self, token: Token) -> ParseResult<&'a str> {
		let start_offset = match token.token {
			BaseTokenKind::String => 1,
			BaseTokenKind::RecordIdString
			| BaseTokenKind::UuidString
			| BaseTokenKind::DateTimeString => 2,
			_ => panic!("unescape_str should only be called with string like tokens"),
		};
		let slice = self.slice(token.span);
		let mut slice_span = token.span;
		let end_offset = 1;
		let slice = &slice[start_offset..(slice.len() - end_offset)];
		slice_span.start += start_offset as u32;
		slice_span.end -= end_offset as u32;

		Self::unescape_common(slice_span, slice, self.source(), &mut self.unescape_buffer)
	}

	pub fn unescape_str_push<'a>(&'a mut self, token: Token) -> ParseResult<NodeId<String>> {
		let str = self.unescape_str(token)?;
		let s = str.to_string();
		Ok(self.ast.push_set(s))
	}

	/// Returns the offset in the unescaped_str for an offset derived from the escaped version of
	/// the unescaped string.
	///
	/// For example `escape_str_offset("\\u{21}a",1)` will return `6` because `\u{21}` results in a
	/// single escaped character.
	pub fn escape_str_offset(unescaped_str: &str, offset: u32) -> u32 {
		let mut lexer = EscapeTokenKind::lexer(unescaped_str);

		let mut offset_idx = 0;
		loop {
			if offset_idx >= offset {
				return lexer.span().end as u32;
			}

			let Some(t) = lexer.next() else {
				break;
			};

			let t = t.expect("string should have already been checked to be correct");
			match t {
				EscapeTokenKind::EscNewline
				| EscapeTokenKind::EscCarriageReturn
				| EscapeTokenKind::EscTab
				| EscapeTokenKind::EscZeroByte
				| EscapeTokenKind::EscBackSlash
				| EscapeTokenKind::EscBackSpace
				| EscapeTokenKind::EscFormFeed
				| EscapeTokenKind::EscQuote
				| EscapeTokenKind::EscDoubleQuote
				| EscapeTokenKind::EscBackTick => {
					offset_idx += 1;
				}
				EscapeTokenKind::EscBracketClose => {
					offset_idx += const { '⟩'.len_utf8() } as u32;
				}
				EscapeTokenKind::EscUnicode => {
					let mut char = 0u32;
					let slice = lexer.slice();
					let slice = &slice["\\u{".len()..(slice.len() - 1)];
					for c in slice.as_bytes() {
						match c {
							b'0'..=b'9' => {
								char *= 10;
								char += (c - b'0') as u32
							}
							b'a'..=b'f' => {
								char *= 10;
								char += (c - b'a' + 10) as u32
							}
							b'A'..=b'F' => {
								char *= 10;
								char += (c - b'a' + 10) as u32
							}
							_ => unreachable!(),
						}
					}

					offset_idx += char::from_u32(char).unwrap().len_utf8() as u32;
				}
				EscapeTokenKind::Chars => {
					let slice = lexer.span();
					if offset_idx + slice.len() as u32 >= offset {
						return ((slice.start as u32) + (offset - offset_idx)) as u32;
					}
					offset_idx += slice.len() as u32;
				}
			}
		}

		lexer.span().end as u32
	}
}
