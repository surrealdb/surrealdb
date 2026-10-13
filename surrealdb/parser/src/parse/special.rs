//! Module implementing special tokens which require additional parsing or require a separate lexer
//! from the normal lexer because of token conflicts.

use std::ops::Range;

use ast::DateTime;
use common::source_error::{AnnotationKind, Level, Snippet};
use common::span::Span;
use logos::Lexer;
use token::{BaseTokenKind, JsFunctionTemplateToken, JsFunctionToken, RegexToken, T, VersionToken};
use uuid::Uuid;

use crate::parse::{ParseError, ParseResult};
use crate::{ParseSync, Parser};

/// Create an unexpected token error for productions parsed from an escaped string.
///
/// This function maps the span from the escaped string to the span in the orignal source.
fn unexpected_error(
	full_source: &str,
	unescape_source: &str,
	unescape_source_offset: u32,
	span: Range<usize>,
	message: String,
) -> ParseError {
	let start =
		Parser::escape_str_offset(unescape_source, span.start as u32) + unescape_source_offset;
	let end = Parser::escape_str_offset(unescape_source, span.end as u32) + unescape_source_offset;
	let span = Span::from_range(start..end);
	ParseError::diagnostic(
		Level::Error
			.title(message)
			.snippet(Snippet::source(full_source).annotate(AnnotationKind::Primary.span(span)))
			.to_diagnostic()
			.to_owned(),
	)
}

/// Parse a version `1.2.3`, token conflicts with a float so needs special consideration.
impl ParseSync for ast::Version {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		fn eat_version(partial: bool, lexer: &mut Lexer<'_, VersionToken>) -> ParseResult<u64> {
			match lexer.next() {
				Some(Ok(VersionToken::Digits)) => {}
				None => {
					if partial {
						return Err(ParseError::missing_data());
					}
				}
				_ => {
					let span = Span::from_usize_range(lexer.span())
						.expect("source should not be larger the u32::MAX");
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Unexpected version token, expected version digits")
							.snippet(
								Snippet::source(lexer.source())
									.annotate(AnnotationKind::Primary.span(span)),
							)
							.to_diagnostic()
							.to_owned(),
					));
				}
			}

			lexer.slice().parse().map_err(|_| {
				let span = Span::from_usize_range(lexer.span())
					.expect("source should not be larger the u32::MAX");
				ParseError::diagnostic(
					Level::Error
						.title("Invalid version token, version number exceeds maximum value")
						.snippet(
							Snippet::source(lexer.source())
								.annotate(AnnotationKind::Primary.span(span)),
						)
						.to_diagnostic()
						.to_owned(),
				)
			})
		}

		let partial = parser.partial();
		parser.lex(|lexer, _| {
			let mut lexer = lexer.morph::<VersionToken>();

			let start = lexer.span().end;

			let major = eat_version(partial, &mut lexer)?;
			match lexer.next() {
				Some(Ok(VersionToken::Dot)) => {}
				x => {
					if x.is_none() && partial {
						return Err(ParseError::missing_data());
					}
					let span = Span::from_usize_range(lexer.span())
						.expect("source should not be larger the u32::MAX");
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Unexpected version token, expected `.`")
							.snippet(
								Snippet::source(lexer.source())
									.annotate(AnnotationKind::Primary.span(span)),
							)
							.to_diagnostic()
							.to_owned(),
					));
				}
			}
			let minor = eat_version(partial, &mut lexer)?;
			match lexer.next() {
				Some(Ok(VersionToken::Dot)) => {}
				x => {
					if x.is_none() && partial {
						return Err(ParseError::missing_data());
					}
					let span = Span::from_usize_range(lexer.span())
						.expect("source should not be larger the u32::MAX");
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Unexpected version token, expected `.`")
							.snippet(
								Snippet::source(lexer.source())
									.annotate(AnnotationKind::Primary.span(span)),
							)
							.to_diagnostic()
							.to_owned(),
					));
				}
			}
			let patch = eat_version(partial, &mut lexer)?;

			let end = lexer.span().end;
			let span = Span::from_usize_range(start..end)
				.expect("source should not be larger the u32::MAX");
			Ok((
				lexer.morph(),
				ast::Version {
					major,
					minor,
					patch,
					span,
				},
			))
		})
	}
}

/// Parse a UUID string.
///
/// String first needs to be unescaped and then parsed for correct UUID format.
impl ParseSync for Uuid {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::UuidString)?;
		let mut span = token.span;
		span.start += 2;
		span.end -= 1;
		let full_source = parser.source();
		let unescape_source = parser.slice(span);
		let str = Parser::unescape_common(
			span,
			unescape_source,
			parser.source(),
			&mut parser.unescape_buffer,
		)?;

		parse_common::uuid(str).map_err(|e| {
			unexpected_error(full_source, unescape_source, span.start, e.span, e.message)
		})
	}
}

/// Parse a DateTime string.
///
/// String first needs to be unescaped and then parsed for correct DateTime format.
impl ParseSync for DateTime {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::DateTimeString)?;
		let mut span = token.span;
		span.start += 2;
		span.end -= 1;
		let full_source = parser.source();
		let unescape_source = parser.slice(span);
		let str = Parser::unescape_common(
			span,
			unescape_source,
			parser.source(),
			&mut parser.unescape_buffer,
		)?;

		parse_common::datetime(str).map_err(|e| {
			unexpected_error(full_source, unescape_source, span.start, e.span, e.message)
		})
	}
}

impl ParseSync for ast::BytesLit {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::ByteString)?;
		let mut span = token.span;
		span.start += 2;
		span.end -= 1;
		let full_source = parser.source();
		let unescape_source = parser.slice(span);
		let str = Parser::unescape_common(
			span,
			unescape_source,
			full_source,
			&mut parser.unescape_buffer,
		)?;

		let text = parse_common::bytes(str).map_err(|e| {
			unexpected_error(full_source, unescape_source, span.start, e.span, e.message)
		})?;

		Ok(ast::BytesLit {
			text,
			span: token.span,
		})
	}
}

/// Parse a regex.
///
/// A regex works almost like a string but its delimiter `/` conflicts with the division operator
/// so it cannot be lexed by the normal lexer.
impl ParseSync for ast::Regex {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = parser.expect(T![/])?;
		let partial = parser.partial();
		let snippet = parser.snippet();
		let source = parser.lex(|lexer, buffer| {
			buffer.clear();
			let mut lexer = lexer.morph::<RegexToken>();
			loop {
				match lexer.next() {
					Some(Ok(RegexToken::End)) => break,
					Some(Ok(RegexToken::Escape)) => {
						buffer.push('/');
					}
					Some(Ok(RegexToken::Source)) => {
						buffer.push_str(lexer.slice());
					}
					// This lexer has no invalid characters, so this should never error.
					Some(Err(_)) => unreachable!(),
					None => {
						if partial {
							return Err(ParseError::missing_data());
						}

						let span = lexer.span();
						let span = Span::from_usize_range(span).expect("span to be in range");
						return Err(ParseError::diagnostic(
							Level::Error
								.title("Unexpected end of query, expected regex to end")
								.snippet(snippet.annotate(AnnotationKind::Primary.span(span)))
								.to_diagnostic()
								.to_owned(),
						));
					}
				}
			}
			Ok((lexer.morph(), buffer.clone()))
		})?;

		let source = parser.push_set(source);
		let span = parser.span_since(start.span);

		Ok(ast::Regex {
			source,
			span,
		})
	}
}

impl ParseSync for ast::JsFunctionBody {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = parser.expect(BaseTokenKind::OpenBrace)?;
		// Lex a javascript function body by keeping track of delimiters: `()`, `{}`, and `[]`.
		// Each of these delimiters should (mostly) be balanced, once all the delimiters have
		// closed to function body has been successfully lexed. We use a stack to keep track of the
		// next delimiter that should be closed, that way we don't have to recurse.
		//
		// Unfortunatly delimiters in a js function body are not always balanced. Instances where
		// we should not track delimiter tokens are:
		// 1. Strings like "]" and '{';
		// 2. Coments like `// ]` and `/* ) */`
		// 3. Template strings like `[`
		// 4. Regexes like /\[/
		//
		// The first and second are handled by the lexer.
		//
		// The third requires some work, because you can have `foo ${ `recursive template ${ string
		// }` } bar` We cannot just snip out anything between two `` but need to restart counting
		// delimiters when entering a template expression (the `${ }` part).
		//
		// The last is the least robust and relies on the fact the `()` `{}` and `[]` are also part
		// of regex syntax, and need to be balanced within the regex except when escaped. The lexer
		// cannot know if a delimiter is inside a regex so instead we just ignore all escaped
		// delimiters everywhere in a javascript body. Escaped delimiters are not valid syntax in
		// normal javascript; `\(`, `\[` is never valid syntax outside of a regex so doing it this
		// way should only ignore those delimiters that happened to be part of a regex.

		#[derive(Clone, Copy, Eq, PartialEq)]
		enum Delimiter {
			Paren,
			Brace,
			Bracket,
			Template,
		}

		impl Delimiter {
			fn as_token(&self) -> &str {
				match self {
					Delimiter::Paren => ")",
					Delimiter::Brace => "}",
					Delimiter::Bracket => "]",
					Delimiter::Template => "}",
				}
			}
		}

		/// Pop a delimiter from the stack and return an error if the delimiter is not the expected
		/// delimiter.
		///
		/// Delimiter::Template should be handled before this function.
		fn expect_delimiter(
			stack: &mut Vec<Delimiter>,
			expected: Delimiter,
			span: Span,
			source: &str,
		) -> ParseResult<()> {
			let got = stack.pop().expect("delimiters to be present");
			if got != expected {
				return Err(ParseError::diagnostic(
					Level::Error
						.title(format!(
							"Unexpected token `{}`, expected `{}",
							got.as_token(),
							expected.as_token()
						))
						.snippet(
							Snippet::source(source).annotate(AnnotationKind::Primary.span(span)),
						)
						.to_diagnostic()
						.to_owned(),
				));
			}
			Ok(())
		}

		/// Handle template strings, possibly pushing a `Delimiter::Template` on the stack when
		/// encounter a template expression `${}`.
		fn lex_template(
			lexer: &mut Lexer<JsFunctionToken>,
			delim_stack: &mut Vec<Delimiter>,
			partial: bool,
		) -> ParseResult<()> {
			let mut templ_lexer = lexer.clone().morph::<JsFunctionTemplateToken>();
			loop {
				match templ_lexer.next() {
					// lexer should have no invalid characters so it should never error.
					Some(Ok(JsFunctionTemplateToken::End)) => break,
					Some(Ok(JsFunctionTemplateToken::Dollar)) => {}
					Some(Ok(JsFunctionTemplateToken::TemplateOpen)) => {
						delim_stack.push(Delimiter::Template);
						break;
					}
					Some(Err(_)) => {
						if partial {
							return Err(ParseError::missing_data());
						}
						let span = Span::from_usize_range(templ_lexer.span())
							.expect("span to be in range");
						return Err(ParseError::diagnostic(
							Level::Error
								.title("Invalid token, expected javascript template string to end")
								.snippet(
									Snippet::source(templ_lexer.source())
										.annotate(AnnotationKind::Primary.span(span)),
								)
								.to_diagnostic()
								.to_owned(),
						));
					}
					None => {
						if partial {
							return Err(ParseError::missing_data());
						}
						let span = Span::from_usize_range(templ_lexer.span())
							.expect("span to be in range");
						return Err(ParseError::diagnostic(
							Level::Error
								.title(
									"Unexpected end of query, expected javascript function to end",
								)
								.snippet(
									Snippet::source(templ_lexer.source())
										.annotate(AnnotationKind::Primary.span(span)),
								)
								.to_diagnostic()
								.to_owned(),
						));
					}
				}
			}
			*lexer = templ_lexer.morph();
			Ok(())
		}

		let partial = parser.partial();
		let body_span = parser.lex(|lexer, _| {
			let start = lexer.span().end;
			let mut lexer = lexer.morph::<JsFunctionToken>();
			let mut delim_stack = vec![Delimiter::Brace];
			while let Some(last) = delim_stack.last() {
				match lexer.next() {
					Some(Ok(JsFunctionToken::BraceOpen)) => delim_stack.push(Delimiter::Brace),
					Some(Ok(JsFunctionToken::BracketOpen)) => delim_stack.push(Delimiter::Bracket),
					Some(Ok(JsFunctionToken::ParenOpen)) => delim_stack.push(Delimiter::Paren),
					Some(Ok(JsFunctionToken::BraceClose)) => {
						if *last == Delimiter::Template {
							// We found that the next `}` re-enters into a template string so we
							// need to lex the template source.
							delim_stack.pop();
							lex_template(&mut lexer, &mut delim_stack, partial)?;
						} else {
							let span_end = lexer.span().end as u32;
							let span = Span::from_range((span_end - 1)..span_end);
							expect_delimiter(
								&mut delim_stack,
								Delimiter::Brace,
								span,
								lexer.source(),
							)?;
						}
					}
					Some(Ok(JsFunctionToken::BracketClose)) => {
						let span_end = lexer.span().end as u32;
						let span = Span::from_range((span_end - 1)..span_end);
						expect_delimiter(
							&mut delim_stack,
							Delimiter::Bracket,
							span,
							lexer.source(),
						)?;
					}
					Some(Ok(JsFunctionToken::ParenClose)) => {
						let span_end = lexer.span().end as u32;
						let span = Span::from_range((span_end - 1)..span_end);
						expect_delimiter(&mut delim_stack, Delimiter::Paren, span, lexer.source())?;
					}
					Some(Ok(JsFunctionToken::TemplateOpen)) => {
						lex_template(&mut lexer, &mut delim_stack, partial)?;
					}
					Some(Ok(JsFunctionToken::Characters)) => {}
					// lexer should have no invalid characters so it should never error.
					Some(Err(_)) => unreachable!("{:?}", lexer.slice()),
					None => {
						if partial {
							return Err(ParseError::missing_data());
						}
						let span =
							Span::from_usize_range(lexer.span()).expect("span to be in range");
						return Err(ParseError::diagnostic(
							Level::Error
								.title(
									"Unexpected end of query, expected javascript function to end",
								)
								.snippet(
									Snippet::source(lexer.source())
										.annotate(AnnotationKind::Primary.span(span)),
								)
								.to_diagnostic()
								.to_owned(),
						));
					}
				}
			}

			// -1 to remove the last `}`
			let span = Span::from_range((start as u32)..((lexer.span().end - 1) as u32));
			Ok((lexer.morph(), span))
		})?;

		let body = parser.slice(body_span);
		let source = parser.push_set_entry::<String, _>(body);

		let span = parser.span_since(start.span);
		Ok(ast::JsFunctionBody {
			source,
			span,
		})
	}
}
