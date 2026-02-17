use ast::{Builtin, Expr, Integer, Point, Sign, Spanned};
use common::source_error::{AnnotationKind, Level};
use token::{BaseTokenKind, T};

use crate::parse::{ParseResult, Parser};

/// Parse a prime expression
///
/// Prime expressions are expression that don't have any operators in them, like `1`, `{ a: 1 }` or
/// `CREATE a`
pub async fn parse_prime(parser: &mut Parser<'_, '_>) -> ParseResult<Expr> {
	let peek = parser.peek_expect("an expression")?;
	match peek.token {
		T![true] => {
			let _ = parser.next();

			let builtin = parser.push(Builtin::True(peek.span));
			Ok(Expr::Builtin(builtin))
		}
		T![false] => {
			let _ = parser.next();

			let builtin = parser.push(Builtin::False(peek.span));
			Ok(Expr::Builtin(builtin))
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
		BaseTokenKind::Float => {
			let float = parser.parse_sync_push()?;
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
		BaseTokenKind::Decimal => {
			let dec = parser.parse_sync_push()?;
			Ok(Expr::Decimal(dec))
		}
		BaseTokenKind::OpenParen => {
			let _ = parser.next();

			// Try parsing a point: (float, float)
			if let Some((x, y, span)) = parser
				.speculate(async |parser| {
					let x: f64 = parser.parse_sync()?;
					let _ = parser.expect(T![,])?;
					// The `,` was accepted so this has to be a point.
					// So commit to parsing `FLOAT )`
					let y: f64 = parser
						.commit(async |parser| {
							let res = parser.parse_sync()?;
							let _ = parser
								.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;
							Ok(res)
						})
						.await?;
					Ok((x, y, parser.span_since(peek.span)))
				})
				.await?
			{
				let point = parser.push(Point {
					x,
					y,
					span,
				});

				return Ok(Expr::Point(point));
			};

			// not a point, so it has to be a partial expression.
			let expr = parser.parse_enter_push().await?;
			let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;
			Ok(Expr::Covered(expr))
		}
		BaseTokenKind::Ident => {
			let path = parser.parse_sync_push()?;
			Ok(Expr::Path(path))
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
