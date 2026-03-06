use std::ops::Bound;

use ast::{Builtin, Expr, ObjectEntry, Point, Spanned};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, T};

use crate::parse::utils::parse_delimited_list;
use crate::parse::{ParseResult, Parser};
use crate::{Parse, ParseSync};

impl ParseSync for ast::Mock {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = parser.expect(T![|])?;
		let name = parser.parse_sync()?;
		let _ = parser.expect(T![:])?;
		let peek = parser.peek_expect("an integer or a range")?;
		match peek.token {
			BaseTokenKind::Int => {
				let start_int = parser.parse_sync()?;
				let peek = parser.peek_expect("`|`")?;
				match peek.token {
					T![>] => {
						let peek1 = parser.peek_joined1()?;
						if let Some(x) = peek1
							&& let T![..] = x.token
						{
							let peek2 = parser.peek_joined2()?;
							if let Some(x) = peek2
								&& let T![=] = x.token
							{
								// |a:1>..=2|
								let _ = parser.next();
								let _ = parser.next();
								let _ = parser.next();
								let end = parser.parse_sync()?;
								let _ = parser.expect_closing_delimiter(T![|], start.span)?;

								let span = parser.span_since(start.span);

								Ok(ast::Mock {
									name,
									kind: ast::MockKind::Range {
										start: Bound::Excluded(start_int),
										end: Bound::Included(end),
									},
									span,
								})
							} else if let Some(peek2) = parser.peek2()?
								&& let BaseTokenKind::Int = peek2.token
							{
								// |a:1>..2|
								let _ = parser.next();
								let _ = parser.next();
								let end = parser.parse_sync()?;
								let _ = parser.expect_closing_delimiter(T![|], start.span)?;

								let span = parser.span_since(start.span);
								Ok(ast::Mock {
									name,
									kind: ast::MockKind::Range {
										start: Bound::Excluded(start_int),
										end: Bound::Excluded(end),
									},
									span,
								})
							} else {
								// |a:1>..|
								let _ = parser.next();
								let _ = parser.next();
								let _ = parser.expect_closing_delimiter(T![|], start.span)?;
								let span = parser.span_since(start.span);
								Ok(ast::Mock {
									name,
									kind: ast::MockKind::Range {
										start: Bound::Excluded(start_int),
										end: Bound::Unbounded,
									},
									span,
								})
							}
						} else {
							Err(parser.unexpected("`>..`"))
						}
					}
					T![..] => {
						let peek1 = parser.peek_joined1()?;
						if let Some(x) = peek1
							&& let T![=] = x.token
						{
							// |a:1..=2|
							let _ = parser.next();
							let _ = parser.next();
							let end = parser.parse_sync()?;
							let _ = parser.expect_closing_delimiter(T![|], start.span)?;

							let span = parser.span_since(start.span);

							Ok(ast::Mock {
								name,
								kind: ast::MockKind::Range {
									start: Bound::Included(start_int),
									end: Bound::Included(end),
								},
								span,
							})
						} else if let Some(peek1) = parser.peek1()?
							&& let BaseTokenKind::Int = peek1.token
						{
							// |a:..2|
							let _ = parser.next();
							let end = parser.parse_sync()?;
							let _ = parser.expect_closing_delimiter(T![|], start.span)?;

							let span = parser.span_since(start.span);
							Ok(ast::Mock {
								name,
								kind: ast::MockKind::Range {
									start: Bound::Included(start_int),
									end: Bound::Excluded(end),
								},
								span,
							})
						} else {
							// |a:1..|
							let _ = parser.next();
							let _ = parser.expect_closing_delimiter(T![|], start.span)?;

							let span = parser.span_since(start.span);
							Ok(ast::Mock {
								name,
								kind: ast::MockKind::Range {
									start: Bound::Included(start_int),
									end: Bound::Unbounded,
								},
								span,
							})
						}
					}
					_ => {
						// |a:1|
						let _ = parser.expect_closing_delimiter(T![|], start.span)?;
						Ok(ast::Mock {
							name,
							kind: ast::MockKind::Integer(start_int),
							span: start.span.extend(peek.span),
						})
					}
				}
			}
			T![..] => {
				if let Some(x) = parser.peek_joined1()?
					&& let T![=] = x.token
				{
					// |a:..=2|
					let _ = parser.next();
					let _ = parser.next();
					let end = parser.parse_sync()?;
					let _ = parser.expect_closing_delimiter(T![|], start.span)?;

					let span = parser.span_since(start.span);

					Ok(ast::Mock {
						name,
						kind: ast::MockKind::Range {
							start: Bound::Unbounded,
							end: Bound::Included(end),
						},
						span,
					})
				} else if let Some(x) = parser.peek1()?
					&& let BaseTokenKind::Int = x.token
				{
					// |a:..2|
					let _ = parser.next();
					let end = parser.parse_sync()?;
					let _ = parser.expect_closing_delimiter(T![|], start.span)?;

					let span = parser.span_since(start.span);
					Ok(ast::Mock {
						name,
						kind: ast::MockKind::Range {
							start: Bound::Unbounded,
							end: Bound::Excluded(end),
						},
						span,
					})
				} else {
					// |a:..|
					let _ = parser.next();
					let _ = parser.expect_closing_delimiter(T![|], start.span)?;
					let span = parser.span_since(start.span);
					Ok(ast::Mock {
						name,
						kind: ast::MockKind::Range {
							start: Bound::Unbounded,
							end: Bound::Unbounded,
						},
						span,
					})
				}
			}
			_ => Err(parser.unexpected("an integer or a range")),
		}
	}
}

impl Parse for ast::Array {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let (span, entries) = parse_delimited_list(
			parser,
			BaseTokenKind::OpenBracket,
			BaseTokenKind::CloseBracket,
			T![,],
			async |parser| parser.parse_enter().await,
		)
		.await?;

		Ok(ast::Array {
			entries,
			span,
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
				// Has to be object.
				let obj = parse_object_continue(parser, start.span).await?;
				let obj = parser.push(obj);
				return Ok(Expr::Object(obj));
			}
			parser.parse_enter().await?
		}
		BaseTokenKind::CloseBrace => {
			// empty object.
			let _ = parser.next();
			let span = parser.span_since(start.span);
			let obj = parser.push(ast::Object {
				entries: None,
				span,
			});
			return Ok(Expr::Object(obj));
		}
		T![;] => {
			// block with a starting empty statement.
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
			// empty set.
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
			// Block
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
			// set
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
			// block with a single expression
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

/// Continues parsing an object after ensuring that the production starting with `{` has to be an
/// object.
async fn parse_object_continue(
	parser: &mut Parser<'_, '_>,
	start_span: Span,
) -> ParseResult<ast::Object> {
	let next = parser.next()?.expect("there should be an object key in this function");

	let key = match next.token {
		BaseTokenKind::Ident => parser.unescape_ident(next)?,
		BaseTokenKind::String => parser.unescape_str_push(next)?,
		_ => unreachable!(),
	};

	let _ = parser.expect(T![:])?;

	let expr = parser.parse_enter().await?;

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
					parser.unescape_ident(peek)?
				}
				BaseTokenKind::String => {
					let _ = parser.next();
					parser.unescape_str_push(peek)?
				}
				_ => return Err(parser.unexpected("an object key")),
			};

			let _ = parser.expect(T![:])?;

			let expr = parser.parse_enter().await?;

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

impl Parse for ast::JsFunction {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![FUNCTION])?;
		let (_, args) = parse_delimited_list(
			parser,
			BaseTokenKind::OpenParen,
			BaseTokenKind::CloseParen,
			T![,],
			async |parser| parser.parse_enter().await,
		)
		.await?;
		let body = parser.parse_sync()?;

		let span = parser.span_since(start.span);
		Ok(ast::JsFunction {
			args,
			body,
			span,
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
			let value = parser.parse_sync()?;
			Ok(Expr::Integer(value))
		}
		BaseTokenKind::Float => {
			let float = parser.parse_sync()?;
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
			let dec = parser.parse_sync()?;
			Ok(Expr::Decimal(dec))
		}
		BaseTokenKind::OpenBracket => {
			let p = parser.parse().await?;
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

			// not a point, so it has to be a covered expression.
			let expr = parser.parse_enter().await?;
			let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;
			Ok(Expr::Covered(expr))
		}
		BaseTokenKind::String => Ok(Expr::String(parser.parse_sync()?)),
		BaseTokenKind::RecordIdString => {
			let _ = parser.next();
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
			let uuid = parser.parse_sync()?;
			Ok(Expr::Uuid(uuid))
		}
		BaseTokenKind::DateTimeString => {
			let uuid = parser.parse_sync()?;
			Ok(Expr::DateTime(uuid))
		}
		BaseTokenKind::FileString => {
			let file = parser.parse_sync()?;
			Ok(Expr::File(file))
		}
		BaseTokenKind::Duration => {
			let uuid = parser.parse_sync()?;
			Ok(Expr::Duration(uuid))
		}
		T![|] => {
			let uuid = parser.parse_sync()?;
			Ok(Expr::Mock(uuid))
		}
		T![/] => {
			let regex = parser.parse_sync()?;
			Ok(Expr::Regex(regex))
		}
		T![FUNCTION] => {
			let js_function = parser.parse().await?;
			Ok(Expr::JsFunction(js_function))
		}
		T![IF] => {
			let expr = parser.parse().await?;
			Ok(Expr::If(expr))
		}
		T![LET] => {
			let expr = parser.parse().await?;
			Ok(Expr::Let(expr))
		}
		T![INFO] => {
			let expr = parser.parse().await?;
			Ok(Expr::Info(expr))
		}
		T![THROW] => {
			let _ = parser.next();
			let expr = parser.parse_enter().await?;
			Ok(Expr::Throw(expr))
		}
		T![DELETE] => {
			let expr = parser.parse().await?;
			Ok(Expr::Delete(expr))
		}
		T![CREATE] => {
			let expr = parser.parse().await?;
			Ok(Expr::Create(expr))
		}
		T![UPDATE] => {
			let expr = parser.parse().await?;
			Ok(Expr::Update(expr))
		}
		T![UPSERT] => {
			let expr = parser.parse().await?;
			Ok(Expr::Upsert(expr))
		}
		T![RELATE] => {
			let expr = parser.parse().await?;
			Ok(Expr::Relate(expr))
		}
		T![SELECT] => {
			let expr = parser.parse().await?;
			Ok(Expr::Select(expr))
		}
		T![DEFINE] => {
			let expected = "a resource type to define";
			let Some(peek) = parser.peek1()? else {
				let _ = parser.next();
				return Err(parser.unexpected(expected));
			};
			match peek.token {
				T![NAMESPACE] => parser.parse().await.map(Expr::DefineNamespace),
				T![DATABASE] => parser.parse().await.map(Expr::DefineDatabase),
				T![TABLE] => parser.parse().await.map(Expr::DefineTable),
				T![FUNCTION] => parser.parse().await.map(Expr::DefineFunction),
				T![MODULE] => parser.parse().await.map(Expr::DefineModule),
				T![PARAM] => parser.parse().await.map(Expr::DefineParam),
				T![API] => parser.parse().await.map(Expr::DefineApi),
				T![EVENT] => parser.parse().await.map(Expr::DefineEvent),
				T![FIELD] => parser.parse().await.map(Expr::DefineField),
				T![INDEX] => parser.parse().await.map(Expr::DefineIndex),
				T![ANALYZER] => parser.parse().await.map(Expr::DefineAnalyzer),
				T![BUCKET] => parser.parse().await.map(Expr::DefineBucket),
				T![SEQUENCE] => parser.parse().await.map(Expr::DefineSequence),
				T![CONFIG] => parser.parse().await.map(Expr::DefineConfig),
				T![ACCESS] => parser.parse().await.map(Expr::DefineAccess),
				_ => {
					let _ = parser.next();
					return Err(parser.unexpected(expected));
				}
			}
		}
		BaseTokenKind::Param => {
			let path = parser.parse_sync()?;
			Ok(Expr::Param(path))
		}
		x if x.is_identifier() => {
			let peek1 = parser.peek1()?;

			if peek1.map(|x| x.token) == Some(T![:]) {
				let expr = parser.parse().await?;
				Ok(Expr::RecordId(expr))
			} else {
				let path = parser.parse_sync()?;
				Ok(Expr::Path(path))
			}
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
