use std::str::FromStr;

use ast::PathSegment;
use common::source_error::{AnnotationKind, Level};
use rust_decimal::Decimal;
use token::{BaseTokenKind, T};

use super::{ParseResult, ParseSync, Parser};

impl ParseSync for ast::Ident {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.peek_expect("an identifier")?;
		if !token.token.is_identifier() {
			return Err(parser.unexpected("an identifier"));
		}
		let _ = parser.next();
		let str_value = parser.unescape_ident(token)?.to_owned();
		let text = parser.push_set(str_value);

		Ok(ast::Ident {
			text,
			span: token.span,
		})
	}
}

impl ParseSync for ast::Param {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Param)?;
		let str_value = parser.unescape_param(token)?.to_owned();
		let text = parser.push_set(str_value);

		Ok(ast::Param {
			text,
			span: token.span,
		})
	}
}

impl ParseSync for f64 {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Float)?;
		let slice = parser.slice(token.span);
		let float = slice.trim_end_matches("f").parse().expect("lexer should ensure valid floats");
		Ok(float)
	}
}

impl ParseSync for Decimal {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Decimal)?;
		let slice =
			parser.slice(token.span).strip_suffix("dec").expect("decimal tokens should end in dec");
		let decimal = if slice.contains(['e', 'E']) {
			Decimal::from_scientific(slice).expect("lexer should ensure valid decimals").normalize()
		} else {
			Decimal::from_str(slice).expect("lexer should ensure valid decimals").normalize()
		};
		Ok(decimal)
	}
}

impl ParseSync for ast::Path {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let span = parser.peek_span();
		let start = parser.parse_sync_push()?;

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
					let ident = parser.parse_sync_push()?;
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
		let token = parser.expect(BaseTokenKind::Int)?;
		let slice = parser.slice(token.span);
		let Ok(x) = slice.parse() else {
			return Err(parser.with_error(|parser| {
				Level::Error
					.title("Integer too large to fit in target type")
					.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(token.span)))
					.to_diagnostic()
			}));
		};

		Ok(ast::Integer {
			sign: ast::Sign::Plus,
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
