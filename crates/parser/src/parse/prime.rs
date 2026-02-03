use ast::{Expr, Integer, Sign, Spanned};
use common::source_error::{AnnotationKind, Level};
use token::BaseTokenKind;

use crate::parse::{ParseResult, Parser};

pub async fn parse_prime(parser: &mut Parser<'_, '_>) -> ParseResult<Expr> {
	let peek = parser.peek_expect("an expression")?;
	match peek.token {
		BaseTokenKind::Float => {
			let _ = parser.next();
			let slice = parser.slice(peek.span);
			let slice = slice.trim_end_matches("f");
			let Ok(x) = slice.parse() else {
				unreachable!("lexer should ensure that float will parse correctly")
			};
			let float = parser.push(Spanned {
				span: peek.span,
				value: x,
			});
			Ok(Expr::Float(float))
		}
		BaseTokenKind::NaN => {
			let _ = parser.next();
			let float = parser.push(Spanned {
				span: peek.span,
				value: f64::NAN,
			});
			Ok(Expr::Float(float))
		}
		BaseTokenKind::PosInfinity => {
			let _ = parser.next();
			let float = parser.push(Spanned {
				span: peek.span,
				value: f64::INFINITY,
			});
			Ok(Expr::Float(float))
		}
		BaseTokenKind::NegInfinity => {
			let _ = parser.next();
			let float = parser.push(Spanned {
				span: peek.span,
				value: f64::NEG_INFINITY,
			});
			Ok(Expr::Float(float))
		}
		BaseTokenKind::Int => {
			let _ = parser.next();
			let slice = parser.slice(peek.span);
			let Ok(x) = slice.parse() else {
				return Err(parser.with_error(|parser| {
					Level::Error
						.title("Integer too large to fit in target type")
						.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(peek.span)))
						.to_diagnostic()
				}));
			};

			let value = parser.push(Integer {
				sign: Sign::Plus,
				value: x,
				span: peek.span,
			});
			Ok(Expr::Integer(value))
		}
		_ => Err(parser.with_error(|parser| {
			Level::Error
				.title(format!(
					"Unexpected token `{}` expected an expression",
					parser.slice(peek.span)
				))
				.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(peek.span)))
				.to_diagnostic()
		})),
	}
}
