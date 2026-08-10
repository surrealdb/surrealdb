use std::str::FromStr;
use std::time::Duration;

use ast::{NodeId, PathSegment};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use rust_decimal::Decimal;
use token::{BaseTokenKind, T, Token};

use super::{ParseResult, ParseSync, Parser};
use crate::parse::{ParseError, ParserSettings};

impl ParseSync for ast::Ident {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.peek_expect("an identifier")?;
		if !token.token.is_identifier() {
			return Err(parser.unexpected("an identifier"));
		}
		let _ = parser.next();
		let text = parser.unescape_ident(token)?;

		Ok(ast::Ident {
			text,
			span: token.span,
		})
	}
}

impl ParseSync for ast::Param {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Param)?;
		let text = parser.unescape_param(token)?;

		Ok(ast::Param {
			text,
			span: token.span,
		})
	}
}

pub fn ununderscore_slice<'a>(slice: &'a str, buffer: &'a mut String) -> &'a str {
	let Some((a, mut rest)) = slice.split_once('_') else {
		return slice;
	};
	buffer.clear();
	buffer.push_str(a);
	while let Some((head, tail)) = rest.split_once('_') {
		buffer.push_str(head);
		rest = tail
	}
	buffer.push_str(rest);
	buffer
}

impl ParseSync for f64 {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let sign = if parser.eat(T![+])?.is_some() {
			ast::Sign::Plus
		} else if parser.eat(T![-])?.is_some() {
			ast::Sign::Minus
		} else {
			ast::Sign::Plus
		};

		// Some more complicated handling of expecting a float here because we
		// need to handle the case here we require a float but we are doing a partial parse
		// and the cut-off is right after the dot, i.e. `1.`.
		let expect = BaseTokenKind::Float.description();
		let token = match parser.peek_expect(expect)? {
			token @ Token {
				token: BaseTokenKind::Float,
				..
			} => {
				let _ = parser.next();
				token
			}
			Token {
				token: BaseTokenKind::Int,
				..
			} if parser.settings.contains(ParserSettings::PARTIAL) => {
				if let Some(BaseTokenKind::Int) = parser.peek()?.map(|x| x.token)
					&& let Some(T![.]) = parser.peek_joined1()?.map(|x| x.token)
				{
					return Err(ParseError::missing_data());
				}
				return Err(parser.unexpected(expect));
			}
			_ => return Err(parser.unexpected(expect)),
		};

		let slice = parser.slice(token.span);
		let slice = ununderscore_slice(slice, &mut parser.unescape_buffer);
		let float: f64 =
			slice.trim_end_matches("f").parse().expect("lexer should ensure valid floats");
		if let ast::Sign::Minus = sign {
			Ok(-float)
		} else {
			Ok(float)
		}
	}
}

impl ParseSync for Decimal {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Decimal)?;
		let slice =
			parser.slice(token.span).strip_suffix("dec").expect("decimal tokens should end in dec");
		let slice = ununderscore_slice(slice, &mut parser.unescape_buffer);
		let decimal = if slice.contains(['e', 'E']) {
			match Decimal::from_scientific(slice) {
				Ok(x) => x,
				Err(_) => {
					return Err(parser.with_error(|parser| {
						Level::Error
							.title("Decimal literal outside of supported value range")
							.snippet(
								parser.snippet().annotate(AnnotationKind::Primary.span(token.span)),
							)
							.to_diagnostic()
					}));
				}
			}
		} else {
			match Decimal::from_str(slice) {
				Ok(x) => x,
				Err(_) => {
					return Err(parser.with_error(|parser| {
						Level::Error
							.title("Decimal literal outside of supported value range")
							.snippet(
								parser.snippet().annotate(AnnotationKind::Primary.span(token.span)),
							)
							.to_diagnostic()
					}));
				}
			}
		};
		Ok(decimal)
	}
}

impl ParseSync for ast::Path {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let span = parser.peek_span();
		let start = parser.parse_sync::<NodeId<ast::Ident>>()?;

		// Special path for ml::*<version> paths which don't have the ::< and are therefore
		// generally ambiguous,
		if start.index(parser).text.index(parser) == "ml" {
			let ml = parser.speculate_sync(|parser| {
				let _ = parser.expect(T![::])?;
				let name = parser.parse_sync()?;
				let open = parser.expect(T![<])?;
				parser.commit_sync(|parser| {
					let version = parser.parse_sync()?;
					let _ = parser.expect_closing_delimiter(T![>], open.span)?;
					Ok((name, version))
				})
			})?;
			if let Some((name, version)) = ml {
				let mut cur = None;
				let mut parts = None;
				let name = parser.push(name);
				parser.push_list(ast::PathSegment::Ident(name), &mut parts, &mut cur);
				parser.push_list(ast::PathSegment::Version(version), &mut parts, &mut cur);

				return Ok(ast::Path {
					start,
					parts,
					span: parser.span_since(span),
				});
			}
		}

		let mut cur = None;
		let mut parts = None;
		while let Some(token) = parser.peek()?
			&& let T![::] = token.token
		{
			let _ = parser.next();

			let peek = parser.peek_expect("a version or a identifier")?;
			let v = match peek.token {
				T![<] => {
					let _ = parser.next();
					let v = parser.parse_sync()?;
					let _ = parser.expect(T![>])?;
					PathSegment::Version(v)
				}
				x if x.is_identifier() => {
					let ident = parser.parse_sync()?;
					PathSegment::Ident(ident)
				}
				_ => return Err(parser.unexpected("a version or a identifier")),
			};
			parser.push_list(v, &mut parts, &mut cur);
		}

		Ok(ast::Path {
			start,
			parts,
			span: parser.span_since(span),
		})
	}
}

impl ParseSync for ast::Integer {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		fn parse_int_value(slice: &[u8]) -> Option<u64> {
			let mut res: u64 = 0;
			for b in slice.iter().copied() {
				if b == b'_' {
					continue;
				}
				// Lexer guarentees that no other characters then `[0-9_]` are present in the
				// slice.
				let v = (b - b'0') as u64;
				res = res.checked_mul(10u64)?.checked_add(v)?;
			}
			Some(res)
		}

		let sign = if parser.eat(T![+])?.is_some() {
			ast::Sign::Plus
		} else if parser.eat(T![-])?.is_some() {
			ast::Sign::Minus
		} else {
			ast::Sign::Plus
		};

		let token = parser.expect(BaseTokenKind::Int)?;
		let slice = parser.slice(token.span);
		let Some(x) = parse_int_value(slice.as_bytes()) else {
			return Err(parser.with_error(|parser| {
				Level::Error
					.title("Integer too large to fit in target type")
					.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(token.span)))
					.to_diagnostic()
			}));
		};

		Ok(ast::Integer {
			sign,
			value: x,
			span: token.span,
		})
	}
}

impl ParseSync for ast::StringLit {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::String)?;
		let slice = parser.unescape_str_push(token)?;
		Ok(ast::StringLit {
			text: slice,
			span: token.span,
		})
	}
}

impl ParseSync for ast::FileLit {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::FileString)?;
		let slice = parser.unescape_str_push(token)?;
		Ok(ast::FileLit {
			path: slice,
			span: token.span,
		})
	}
}

impl ParseSync for Duration {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Duration)?;
		let slice = parser.slice(token.span);

		parse_common::duration(slice).map_err(|e| {
			parser.with_error(|parser| {
				let span = Span::new(
					token.span.start + e.span.start as u32,
					token.span.start + e.span.end as u32,
				);
				Level::Error
					.title(e.message)
					.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(span)))
					.to_diagnostic()
			})
		})
	}
}
