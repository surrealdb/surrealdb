use common::source_error::{AnnotationKind, Level, Snippet};
use common::span::Span;
use logos::Lexer;
use token::VersionTokenKind;

use crate::parse::{ParseError, ParseResult};
use crate::{ParseSync, Parser};

impl ParseSync for ast::Version {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		fn eat_version(partial: bool, lexer: &mut Lexer<'_, VersionTokenKind>) -> ParseResult<u64> {
			let start = lexer.span().end;
			let mut clone = lexer.clone();
			loop {
				match clone.next() {
					Some(Ok(VersionTokenKind::Digit)) => {
						*lexer = clone.clone();
					}
					None => {
						if partial {
							return Err(ParseError::missing_data_error());
						}
					}
					_ => break,
				}
			}

			let end = lexer.span().end;
			lexer.source()[start..end].parse().map_err(|_| {
				let span = Span::from_usize_range(start..end)
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
			let mut lexer = lexer.morph::<VersionTokenKind>();

			let start = lexer.span().end;

			let major = eat_version(partial, &mut lexer)?;
			match lexer.next() {
				Some(Ok(VersionTokenKind::Dot)) => {}
				x => {
					if x.is_none() && partial {
						return Err(ParseError::missing_data_error());
					}
					let span = Span::from_usize_range(lexer.span())
						.expect("source should not be larger the u32::MAX");
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Unexpected token, expected `.`")
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
				Some(Ok(VersionTokenKind::Dot)) => {}
				x => {
					if x.is_none() && partial {
						return Err(ParseError::missing_data_error());
					}
					let span = Span::from_usize_range(lexer.span())
						.expect("source should not be larger the u32::MAX");
					return Err(ParseError::diagnostic(
						Level::Error
							.title("Unexpected token, expected `.`")
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
