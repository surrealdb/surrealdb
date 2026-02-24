use common::source_error::{AnnotationKind, Level, Snippet};
use common::span::Span;
use logos::{Lexer, Logos};
use token::{BaseTokenKind, UuidToken, VersionToken};
use uuid::Uuid;

use crate::parse::{ParseError, ParseResult};
use crate::{ParseSync, Parser};

impl ParseSync for ast::Version {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		fn eat_version(partial: bool, lexer: &mut Lexer<'_, VersionToken>) -> ParseResult<u64> {
			match lexer.next() {
				Some(Ok(VersionToken::Digits)) => {}
				None => {
					if partial {
						return Err(ParseError::missing_data_error());
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
		parser.lex(|lexer| {
			let mut lexer = lexer.morph::<VersionToken>();

			let start = lexer.span().end;

			let major = eat_version(partial, &mut lexer)?;
			match lexer.next() {
				Some(Ok(VersionToken::Dot)) => {}
				x => {
					if x.is_none() && partial {
						return Err(ParseError::missing_data_error());
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
						return Err(ParseError::missing_data_error());
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

		fn parse_hex_byte(byte: u8) -> u8 {
			match byte {
				b'0'..=b'9' => byte - b'0',
				b'a'..=b'f' => byte - b'a' + 10,
				b'A'..=b'F' => byte - b'A' + 10,
				_ => unreachable!(),
			}
		}

		fn eat_digits(
			full_source: &str,
			unescape_source: &str,
			unescape_source_offset: u32,
			lexer: &mut Lexer<UuidToken>,
			bytes: &mut [u8],
		) -> ParseResult<()> {
			match lexer.next() {
				Some(Ok(UuidToken::Digits)) => {
					let span = lexer.span();
					if span.len() != bytes.len() * 2 {
						let start = Parser::escape_str_offset(unescape_source, span.start as u32)
							+ unescape_source_offset;
						let end = Parser::escape_str_offset(unescape_source, span.end as u32)
							+ unescape_source_offset;
						let span = Span::from_range(start..end);
						return Err(ParseError::diagnostic(
							Level::Error
								.title(format!(
									"Invalid uuid token, invalid number of hex digits, expected {}, found {}",
									bytes.len() * 2,
									span.len()
								))
								.snippet(
									Snippet::source(full_source)
										.annotate(AnnotationKind::Primary.span(span)),
								)
								.to_diagnostic()
								.to_owned(),
						));
					}

					let slice = lexer.slice().as_bytes();
					for (idx, b) in bytes.iter_mut().enumerate() {
						let u = parse_hex_byte(slice[idx * 2]);
						let l = parse_hex_byte(slice[idx * 2 + 1]);

						*b = (u << 4) | l
					}
				}
				_ => {
					let span = lexer.span();
					let start = Parser::escape_str_offset(unescape_source, span.start as u32)
						+ unescape_source_offset;
					let end = Parser::escape_str_offset(unescape_source, span.end as u32)
						+ unescape_source_offset;
					let span = Span::from_range(start..end);
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Invalid uuid token, unexpected character, expected hex digit")
							.snippet(
								Snippet::source(full_source)
									.annotate(AnnotationKind::Primary.span(span)),
							)
							.to_diagnostic()
							.to_owned(),
					));
				}
			}
			Ok(())
		}

		fn eat_dash(
			full_source: &str,
			unescape_source: &str,
			unescape_source_offset: u32,
			lexer: &mut Lexer<UuidToken>,
		) -> ParseResult<()> {
			match lexer.next() {
				Some(Ok(UuidToken::Dash)) => {}
				_ => {
					let span = lexer.span();
					let start = Parser::escape_str_offset(unescape_source, span.start as u32)
						+ unescape_source_offset;
					let end = Parser::escape_str_offset(unescape_source, span.end as u32)
						+ unescape_source_offset;
					let span = Span::from_range(start..end);
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Invalid uuid token, unexpected character, expected `-`")
							.snippet(
								Snippet::source(full_source)
									.annotate(AnnotationKind::Primary.span(span)),
							)
							.to_diagnostic()
							.to_owned(),
					));
				}
			}
			Ok(())
		}

		let mut lexer = UuidToken::lexer(str);

		let mut buffer = [0u8; 16];

		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[0..4])?;
		eat_dash(full_source, unescape_source, span.start, &mut lexer)?;
		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[4..6])?;
		eat_dash(full_source, unescape_source, span.start, &mut lexer)?;
		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[6..8])?;
		eat_dash(full_source, unescape_source, span.start, &mut lexer)?;
		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[8..10])?;
		eat_dash(full_source, unescape_source, span.start, &mut lexer)?;
		eat_digits(full_source, unescape_source, span.start, &mut lexer, &mut buffer[10..16])?;

		Ok(Uuid::from_bytes(buffer))
	}
}
