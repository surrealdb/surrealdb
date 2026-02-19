use ast::{Builtin, Expr, Integer, Point, Sign, Spanned};
use common::source_error::{AnnotationKind, Level};
use token::{BaseTokenKind, T};

use crate::Parse;
use crate::parse::{ParseResult, Parser};

impl Parse for ast::Array {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(BaseTokenKind::OpenBracket)?;
		let mut head = None;
		let mut tail = None;
		loop {
			if parser.eat(BaseTokenKind::CloseBracket)?.is_some() {
				break;
			}

			let expr = parser.parse_enter::<ast::Expr>().await?;
			parser.push_list(expr, &mut head, &mut tail);

			if parser.eat(T![,])?.is_none() {
				let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBracket, start.span)?;
				break;
			}
		}

		Ok(ast::Array {
			entries: head,
			span: parser.span_since(start.span),
		})
	}
}

impl Parse for ast::Block {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(BaseTokenKind::OpenBrace)?;
		let mut head = None;
		let mut tail = None;

		// Eat empty statements.
		while parser.eat(T![;])?.is_some() {}

		loop {
			if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
				break;
			}

			let expr = parser.parse_enter::<ast::Expr>().await?;
			parser.push_list(expr, &mut head, &mut tail);

			if parser.eat(T![;])?.is_none() {
				let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, start.span)?;
				break;
			}

			// Eat empty statements.
			while parser.eat(T![;])?.is_some() {}
		}

		Ok(ast::Block {
			exprs: head,
			span: parser.span_since(start.span),
		})
	}
}

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
		T![NONE] => {
			let _ = parser.next();

			let builtin = parser.push(Builtin::None(peek.span));
			Ok(Expr::Builtin(builtin))
		}
		T![NULL] => {
			let _ = parser.next();

			let builtin = parser.push(Builtin::Null(peek.span));
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
		BaseTokenKind::OpenBracket => {
			let p = parser.parse_push().await?;
			Ok(Expr::Array(p))
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
		T![IF] => {
			let expr = parser.parse_push().await?;
			Ok(Expr::If(expr))
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
