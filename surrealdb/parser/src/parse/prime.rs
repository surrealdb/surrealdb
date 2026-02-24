use ast::{Builtin, Expr, Integer, NodeList, ObjectEntry, Point, Sign, Spanned};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, T};

use crate::Parse;
use crate::parse::{ParseError, ParseResult, Parser};

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

impl Parse for ast::Object {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<ast::Object> {
		let start = parser.expect(BaseTokenKind::OpenBrace)?;
		let peek = parser.peek_expect("an object key")?;
		match peek.token {
			BaseTokenKind::String | BaseTokenKind::Ident => {
				let obj = parse_object_continue(parser, start.span).await?;
				Ok(obj)
			}
			_ => return Err(parser.unexpected("an object key")),
		}
	}
}

/// Parse a prime expression that starts with `{`:
pub async fn parse_object_like(parser: &mut Parser<'_, '_>) -> ParseResult<Expr> {
	let start = parser.expect(BaseTokenKind::OpenBrace)?;

	let token = parser.peek_expect("`}`")?;
	let expr = match token.token {
		BaseTokenKind::String | BaseTokenKind::Ident => {
			if let Some(T![:]) = parser.peek1()?.map(|x| x.token) {
				let obj = parse_object_continue(parser, start.span).await?;
				let obj = parser.push(obj);
				return Ok(Expr::Object(obj));
			}
			parser.parse_enter().await?
		}
		BaseTokenKind::CloseBrace => {
			let _ = parser.next();
			let span = parser.span_since(start.span);
			let obj = parser.push(ast::Object {
				entries: None,
				span,
			});
			return Ok(Expr::Object(obj));
		}
		T![;] => {
			while parser.eat(T![;])?.is_some() {}

			if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
				let span = parser.span_since(start.span);
				let obj = parser.push(ast::Block {
					exprs: None,
					span,
				});
				return Ok(Expr::Block(obj));
			} else {
				parser.parse_enter::<Expr>().await?
			}
		}
		T![,] => {
			let _ = parser.next();
			let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, start.span)?;
			let span = parser.span_since(start.span);
			let obj = parser.push(ast::Set {
				entries: None,
				span,
			});
			return Ok(Expr::Set(obj));
		}
		_ => parser.parse_enter::<Expr>().await?,
	};

	let mut head = None;
	let mut tail = None;
	parser.push_list(expr, &mut head, &mut tail);

	let token = parser.peek_expect("`}`")?;
	match token.token {
		T![;] => {
			loop {
				while parser.eat(T![;])?.is_some() {}

				if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
					break;
				}

				let expr = parser.parse_enter().await?;
				parser.push_list(expr, &mut head, &mut tail);

				if parser.eat(T![;])?.is_none() {
					let _ =
						parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, start.span)?;
					break;
				}
			}
			let span = parser.span_since(start.span);
			let obj = parser.push(ast::Block {
				exprs: head,
				span,
			});
			Ok(Expr::Block(obj))
		}
		T![,] => {
			let _ = parser.next();

			loop {
				if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
					break;
				}

				let expr = parser.parse_enter().await?;
				parser.push_list(expr, &mut head, &mut tail);

				if parser.eat(T![,])?.is_none() {
					let _ =
						parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, start.span)?;
					break;
				}
			}
			let span = parser.span_since(start.span);
			let obj = parser.push(ast::Set {
				entries: head,
				span,
			});
			Ok(Expr::Set(obj))
		}
		_ => {
			let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, start.span)?;

			let span = parser.span_since(start.span);
			let obj = parser.push(ast::Block {
				exprs: head,
				span,
			});
			Ok(Expr::Block(obj))
		}
	}
}

async fn parse_object_continue(
	parser: &mut Parser<'_, '_>,
	start_span: Span,
) -> ParseResult<ast::Object> {
	let next = parser.next()?.expect("there should be an object key in this function");

	let key = match next.token {
		BaseTokenKind::Ident => {
			let str = parser.unescape_ident(next)?;
			let str = str.to_owned();
			parser.push_set(str)
		}
		BaseTokenKind::String => {
			let str = parser.unescape_str(next)?;
			let str = str.to_owned();
			parser.push_set(str)
		}
		_ => unreachable!(),
	};

	let _ = parser.expect(T![:])?;

	let expr = parser.parse_enter_push::<Expr>().await?;

	let entry_span = parser.span_since(next.span);

	let mut head = None;
	let mut tail = None;
	parser.push_list(
		ObjectEntry {
			key,
			value: expr,
			span: entry_span,
		},
		&mut head,
		&mut tail,
	);

	if parser.eat(T![,])?.is_some() {
		loop {
			if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
				break;
			}

			let peek = parser.peek_expect("an object key")?;
			let key = match peek.token {
				BaseTokenKind::Ident => {
					let _ = parser.next();
					let str = parser.unescape_ident(peek)?;
					let str = str.to_owned();
					parser.push_set(str)
				}
				BaseTokenKind::String => {
					let _ = parser.next();
					let str = parser.unescape_str(peek)?;
					let str = str.to_owned();
					parser.push_set(str)
				}
				_ => return Err(parser.unexpected("an object key")),
			};

			let _ = parser.expect(T![:])?;

			let expr = parser.parse_enter_push::<Expr>().await?;

			let entry_span = parser.span_since(peek.span);

			parser.push_list(
				ObjectEntry {
					key,
					value: expr,
					span: entry_span,
				},
				&mut head,
				&mut tail,
			);

			if parser.eat(T![,])?.is_none() {
				let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, start_span)?;
				break;
			}
		}
	} else {
		let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, start_span)?;
	}

	let span = parser.span_since(start_span);
	Ok(ast::Object {
		entries: head,
		span,
	})
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
			let value = parser.parse_sync_push()?;
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
		BaseTokenKind::OpenBrace => parse_object_like(parser).await,
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
		BaseTokenKind::String => Ok(Expr::String(parser.parse_sync_push()?)),
		BaseTokenKind::RecordIdString => {
			let _ = parser.next()?;
			// TODO: Remove `to_owned` call.
			let str = parser.unescape_str(peek)?.to_owned();
			match parser.sub_parse::<ast::RecordId>(&str).await {
				Ok(x) => {
					let p = parser.push(x);
					Ok(Expr::RecordId(p))
				}
				Err(mut e) => {
					if let Some(e) = e.as_mut_diagnostic() {
						// remove the first 2 `r"` characters to get the unescaped string that was
						// used for parsing.
						let slice = &parser.slice(peek.span)[2..];
						e.map_source(
							|s| *s = parser.source().to_owned().into(),
							|s| {
								let range = s.to_range();
								// +2 for the `r"` characters
								let start = Parser::escape_str_offset(slice, range.start)
									+ peek.span.start + 2;
								let end = Parser::escape_str_offset(slice, range.end)
									+ peek.span.start + 2;
								*s = Span::from_range(start..end)
							},
						);
					}
					return Err(e);
				}
			}
		}
		BaseTokenKind::UuidString => {
			let uuid = parser.parse_sync_push()?;
			Ok(Expr::Uuid(uuid))
		}
		T![IF] => {
			let expr = parser.parse_push().await?;
			Ok(Expr::If(expr))
		}
		T![LET] => {
			let expr = parser.parse_push().await?;
			Ok(Expr::Let(expr))
		}
		BaseTokenKind::Ident => {
			let peek1 = parser.peek1()?;

			if peek1.map(|x| x.token) == Some(T![:]) {
				let expr = parser.parse_push().await?;
				Ok(Expr::RecordId(expr))
			} else {
				let path = parser.parse_sync_push()?;
				Ok(Expr::Path(path))
			}
		}
		BaseTokenKind::Param => {
			let path = parser.parse_sync_push()?;
			Ok(Expr::Param(path))
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
