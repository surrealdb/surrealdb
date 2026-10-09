use std::ops::Bound;

use ast::{AstSpan, Builtin, Expr, ObjectEntry, Point, Spanned};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, T};

use crate::parse::misc::parse_closure_parameter;
use crate::parse::range::{TryRange, parse_prefix_range_sync, try_parse_infix_range_sync};
use crate::parse::utils::parse_delimited_list;
use crate::parse::{ParseError, ParseResult, Parser, ParserSettings, ParserState};
use crate::{Parse, ParseSync};

impl ParseSync for ast::Mock {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = parser.expect(T![|])?;
		let name = parser.parse_sync()?;
		let _ = parser.expect(T![:])?;
		let peek = parser.peek_expect("an integer or a range")?;

		fn peek_has_int(parser: &mut Parser) -> ParseResult<bool> {
			parser.peek().map(|x| {
				x.map(|x| matches!(x.token, BaseTokenKind::Int | T![+] | T![-])).unwrap_or(false)
			})
		}

		match peek.token {
			BaseTokenKind::Int => {
				let start_int = parser.parse_sync()?;
				let try_range = try_parse_infix_range_sync(parser, start_int, peek_has_int)?;
				let kind = match try_range {
					TryRange::None(x) => ast::MockKind::Integer(x),
					TryRange::Some {
						start,
						end,
					} => ast::MockKind::Range {
						start,
						end,
					},
				};

				let _ = parser.expect_closing_delimiter(T![|], start.span)?;

				let span = parser.span_since(start.span);
				Ok(ast::Mock {
					name,
					kind,
					span,
				})
			}
			T![..] => {
				let bound = parse_prefix_range_sync(parser, peek_has_int)?;
				let _ = parser.expect_closing_delimiter(T![|], start.span)?;
				let span = parser.span_since(start.span);
				Ok(ast::Mock {
					name,
					kind: ast::MockKind::Range {
						start: Bound::Unbounded,
						end: bound,
					},
					span,
				})
			}
			_ => Err(parser.unexpected("an integer or a range")),
		}
	}
}

impl Parse for ast::Closure {
	fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let (args_span, parameters) = if let Some(peek) = parser.eat(T![||])? {
			(peek.span, None)
		} else {
			parse_delimited_list(parser, T![|], T![|], T![,], parse_closure_parameter)?
		};

		let output_ty = if parser.eat(T![->])?.is_some() {
			Some(parser.parse()?)
		} else {
			None
		};

		let body = parser.parse_enter()?;

		let span = parser.span_since(args_span);
		Ok(ast::Closure {
			parameters,
			output_ty,
			body,
			span,
		})
	}
}

impl Parse for ast::Array {
	fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let (span, entries) = parse_delimited_list(
			parser,
			BaseTokenKind::OpenBracket,
			BaseTokenKind::CloseBracket,
			T![,],
			|parser| parser.parse_enter(),
		)?;

		Ok(ast::Array {
			entries,
			span,
		})
	}
}

impl Parse for ast::Object {
	fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<ast::Object> {
		let start = parser.expect(BaseTokenKind::OpenBrace)?;
		let peek = parser.peek_expect("an object key")?;
		match peek.token {
			BaseTokenKind::String => {
				let obj = parse_object_continue(parser, start.span)?;
				Ok(obj)
			}
			x if x.is_identifier() => {
				let obj = parse_object_continue(parser, start.span)?;
				Ok(obj)
			}
			_ => Err(parser.unexpected("an object key")),
		}
	}
}

fn expect_close_block(
	parser: &mut Parser<'_, '_>,
	open: Span,
	last_statement: Span,
) -> ParseResult<()> {
	if parser.eat(BaseTokenKind::CloseBrace)?.is_none() {
		return Err(parser.with_error(|p| {
			if p.eof() {
				Level::Error
					.title("Unexpected end of query, expected closing delimiter }")
					.snippet(
						p.snippet()
							.annotate(
								AnnotationKind::Primary.span(p.eof_span()).label("Missing } here"),
							)
							.annotate(
								AnnotationKind::Context
									.span(open)
									.label("Expected this delimiter to close"),
							),
					)
					.to_diagnostic()
			} else {
				let span = p.peek_span();
				Level::Error
					.title(format!(
						"Unexpected token `{}`, expected closing delimiter }}",
						p.slice(span),
					))
					.snippet(
						p.snippet()
							.annotate(AnnotationKind::Primary.span(span).label("Missing } here"))
							.annotate(
								AnnotationKind::Context
									.span(open)
									.label("Expected this delimiter to close"),
							)
							.annotate(
								AnnotationKind::Context
									.span(last_statement)
									.label("Maybe forgot a `;` after the last expression?"),
							),
					)
					.to_diagnostic()
			}
		}));
	}
	Ok(())
}

/// Parse a prime expression that starts with `{`:
pub fn parse_object_like(parser: &mut Parser<'_, '_>) -> ParseResult<Expr> {
	let start = parser.expect(BaseTokenKind::OpenBrace)?;

	let token = parser.peek_expect("`}`")?;
	let expr = match token.token {
		BaseTokenKind::String | BaseTokenKind::Int => {
			if let Some(T![:]) = parser.peek1()?.map(|x| x.token) {
				// Has to be object.
				let obj = parse_object_continue(parser, start.span)?;
				let obj = parser.push(obj);
				return Ok(Expr::Object(obj));
			}
			parser.parse_enter()?
		}
		x if x.is_identifier() => {
			if let Some(T![:]) = parser.peek1()?.map(|x| x.token) {
				// Has to be object.
				let obj = parse_object_continue(parser, start.span)?;
				let obj = parser.push(obj);
				return Ok(Expr::Object(obj));
			}
			parser.parse_enter()?
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
				parser.parse_enter::<Expr>()?
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
		_ => parser.parse_enter::<Expr>()?,
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

				let expr = parser.parse_enter()?;
				parser.push_list(expr, &mut head, &mut tail);

				if parser.eat(T![;])?.is_none() {
					expect_close_block(
						parser,
						start.span,
						tail.expect("we should have already parsed a value").ast_span(parser),
					)?;
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

				let expr = parser.parse_enter()?;
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
			if parser.settings.contains(ParserSettings::QUIRK_FIRST_SEMICOLON_BLOCK) {
				loop {
					while parser.eat(T![;])?.is_some() {}

					if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
						break;
					}

					let expr = parser.parse_enter()?;
					parser.push_list(expr, &mut head, &mut tail);

					if parser.eat(T![;])?.is_none() {
						let _ = parser
							.expect_closing_delimiter(BaseTokenKind::CloseBrace, start.span)?;
						break;
					}
				}
			} else {
				// block with a single expression
				expect_close_block(
					parser,
					start.span,
					tail.expect("we should have already parsed a value").ast_span(parser),
				)?;
			}

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
fn parse_object_continue(
	parser: &mut Parser<'_, '_>,
	start_span: Span,
) -> ParseResult<ast::Object> {
	let next = parser.peek()?.expect("there should be an object key in this function");

	let key = match next.token {
		BaseTokenKind::String => {
			let _ = parser.next();
			parser.unescape_str_push(next)?
		}
		BaseTokenKind::Int => {
			let _ = parser.next();
			let p = parser.slice(next.span).to_owned();
			parser.push_set(p)
		}
		x if x.is_identifier() => {
			let _ = parser.next();
			parser.unescape_ident(next)?
		}
		_ => unreachable!(),
	};

	let _ = parser.expect(T![:])?;

	let expr = parser.parse_enter()?;

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
				BaseTokenKind::String => {
					let _ = parser.next();
					parser.unescape_str_push(peek)?
				}
				x if x.is_identifier() => {
					let _ = parser.next();
					parser.unescape_ident(peek)?
				}
				_ => return Err(parser.unexpected("an object key")),
			};

			let _ = parser.expect(T![:])?;

			let expr = parser.parse_enter()?;

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
	fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(BaseTokenKind::OpenBrace)?;
		let mut head = None;
		let mut tail = None;

		// Eat empty statements.
		while parser.eat(T![;])?.is_some() {}

		loop {
			if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
				break;
			}

			let expr = parser.parse_enter::<ast::Expr>()?;
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
	fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![FUNCTION])?;
		let (_, args) = parse_delimited_list(
			parser,
			BaseTokenKind::OpenParen,
			BaseTokenKind::CloseParen,
			T![,],
			|parser| parser.parse_enter(),
		)?;
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
pub fn parse_prime(parser: &mut Parser<'_, '_>) -> ParseResult<Expr> {
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
		BaseTokenKind::PosInfinity | BaseTokenKind::Infinity => {
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
			let p = parser.parse()?;
			Ok(Expr::Array(p))
		}
		BaseTokenKind::OpenBrace => parse_object_like(parser),
		BaseTokenKind::OpenParen => {
			let _ = parser.next();

			// Try parsing a point: (float|int, float|int)
			if let Some((x, y, span)) = parser.speculate(|parser| {
				let expect = "an integer or a float";
				let peek = parser.peek_expect(expect)?;
				let x = match peek.token {
					T![+] | T![-] => match parser.peek1()?.map(|x| x.token) {
						Some(BaseTokenKind::Int) => parser.parse_sync::<ast::Integer>()?.into_f64(),
						Some(BaseTokenKind::Float) => parser.parse_sync::<f64>()?,
						_ => return Err(ParseError::speculate()),
					},
					BaseTokenKind::Float => parser.parse_sync::<f64>()?,
					BaseTokenKind::Int => parser.parse_sync::<ast::Integer>()?.into_f64(),
					_ => return Err(ParseError::speculate()),
				};

				let _ = parser.expect(T![,])?;
				// The `,` was accepted so this has to be a point.
				// So commit to parsing `FLOAT )`
				let y: f64 = parser.commit(|parser| {
					let peek_float = parser.peek_expect(expect)?;
					let y = match peek_float.token {
						T![+] | T![-] => match parser.peek1()?.map(|x| x.token) {
							Some(BaseTokenKind::Int) => {
								parser.parse_sync::<ast::Integer>()?.into_f64()
							}
							Some(BaseTokenKind::Float) => parser.parse_sync::<f64>()?,
							_ => return Err(parser.unexpected(expect)),
						},
						BaseTokenKind::Float => parser.parse_sync::<f64>()?,
						BaseTokenKind::Int => parser.parse_sync::<ast::Integer>()?.into_f64(),
						_ => return Err(parser.unexpected(expect)),
					};
					let _ =
						parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;
					Ok(y)
				})?;
				Ok((x, y, parser.span_since(peek.span)))
			})? {
				let point = parser.push(Point {
					x,
					y,
					span,
				});

				return Ok(Expr::Point(point));
			};

			// not a point, so it has to be a covered expression.
			let expr = parser.parse_enter()?;
			let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;
			Ok(Expr::Covered(expr))
		}
		BaseTokenKind::String => Ok(Expr::String(parser.parse_sync()?)),
		BaseTokenKind::RecordIdString => {
			let _ = parser.next();
			let mut unescape_buffer = String::new();
			let str = parser.unescape_str(peek, &mut unescape_buffer)?;
			match parser.sub_parse::<ast::RecordId>(str) {
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
					Err(e)
				}
			}
		}
		BaseTokenKind::UuidString => Ok(Expr::Uuid(parser.parse_sync()?)),
		BaseTokenKind::DateTimeString => Ok(Expr::DateTime(parser.parse_sync()?)),
		BaseTokenKind::FileString => Ok(Expr::File(parser.parse_sync()?)),
		BaseTokenKind::Duration => Ok(Expr::Duration(parser.parse_sync()?)),
		BaseTokenKind::ByteString => Ok(Expr::Bytes(parser.parse_sync()?)),
		T![||] => Ok(Expr::Closure(parser.parse()?)),
		T![|] => {
			if let Some(peek1) = parser.peek1()?
				&& let BaseTokenKind::Param = peek1.token
			{
				Ok(Expr::Closure(parser.parse()?))
			} else {
				Ok(Expr::Mock(parser.parse_sync()?))
			}
		}
		T![/] => Ok(Expr::Regex(parser.parse_sync()?)),
		T![FUNCTION] => Ok(Expr::JsFunction(parser.parse()?)),
		T![IF] => Ok(Expr::If(parser.parse()?)),
		T![FOR] => Ok(Expr::For(parser.parse()?)),
		T![LET] => Ok(Expr::Let(parser.parse()?)),
		T![RETURN] => Ok(Expr::Return(parser.parse()?)),
		T![INFO] => Ok(Expr::Info(parser.parse()?)),
		T![THROW] => {
			let _ = parser.next();
			let expr = parser.parse_enter()?;
			Ok(Expr::Throw(expr))
		}
		T![CONTINUE] => {
			if !parser.settings.contains(ParserSettings::QUIRK_NO_CONTROLFLOW_CHECK)
				&& !parser.state.contains(ParserState::LOOP)
			{
				return Err(parser.error(
					"Invalid use of control flow statement outside of loop body",
					peek.span,
				));
			}
			let _ = parser.next();
			Ok(Expr::Continue(parser.push(peek.span)))
		}
		T![BREAK] => {
			if !parser.settings.contains(ParserSettings::QUIRK_NO_CONTROLFLOW_CHECK)
				&& !parser.state.contains(ParserState::LOOP)
			{
				return Err(parser.error(
					"Invalid use of control flow statement outside of loop body",
					peek.span,
				));
			}
			let _ = parser.next();
			Ok(Expr::Break(parser.push(peek.span)))
		}
		T![SLEEP] => {
			if let Some(peek1) = parser.peek1()?
				&& let BaseTokenKind::OpenParen = peek1.token
			{
				let path = parser.parse_sync()?;
				Ok(Expr::Path(path))
			} else {
				let _ = parser.next();
				Ok(Expr::Sleep(parser.parse_sync()?))
			}
		}
		T![DELETE] => Ok(Expr::Delete(parser.parse()?)),
		T![CREATE] => Ok(Expr::Create(parser.parse()?)),
		T![UPDATE] => Ok(Expr::Update(parser.parse()?)),
		T![UPSERT] => Ok(Expr::Upsert(parser.parse()?)),
		T![RELATE] => Ok(Expr::Relate(parser.parse()?)),
		T![SELECT] => Ok(Expr::Select(parser.parse()?)),
		T![INSERT] => Ok(Expr::Insert(parser.parse()?)),
		T![REBUILD] => Ok(Expr::Rebuild(parser.parse()?)),
		T![ACCESS] => Ok(Expr::Access(parser.parse()?)),
		T![DEFINE] => {
			let expected = "a resource type to define";
			let Some(peek) = parser.peek1()? else {
				let _ = parser.next();
				return Err(parser.unexpected(expected));
			};
			match peek.token {
				T![NAMESPACE] => parser.parse().map(Expr::DefineNamespace),
				T![DATABASE] => parser.parse().map(Expr::DefineDatabase),
				T![TABLE] | T![TB] => parser.parse().map(Expr::DefineTable),
				T![FUNCTION] => parser.parse().map(Expr::DefineFunction),
				T![MODULE] => parser.parse().map(Expr::DefineModule),
				T![PARAM] => parser.parse().map(Expr::DefineParam),
				T![API] => parser.parse().map(Expr::DefineApi),
				T![EVENT] => parser.parse().map(Expr::DefineEvent),
				T![FIELD] => parser.parse().map(Expr::DefineField),
				T![INDEX] => parser.parse().map(Expr::DefineIndex),
				T![ANALYZER] => parser.parse().map(Expr::DefineAnalyzer),
				T![BUCKET] => parser.parse().map(Expr::DefineBucket),
				T![SEQUENCE] => parser.parse().map(Expr::DefineSequence),
				T![CONFIG] => parser.parse().map(Expr::DefineConfig),
				T![USER] => parser.parse().map(Expr::DefineUser),
				T![ACCESS] => parser.parse().map(Expr::DefineAccess),
				_ => {
					let _ = parser.next();
					Err(parser.unexpected(expected))
				}
			}
		}
		T![REMOVE] => {
			let expected = "a resource type to define";
			let Some(peek) = parser.peek1()? else {
				let _ = parser.next();
				return Err(parser.unexpected(expected));
			};
			match peek.token {
				T![NAMESPACE] => parser.parse().map(Expr::RemoveNamespace),
				T![DATABASE] => parser.parse().map(Expr::RemoveDatabase),
				T![TABLE] | T![TB] => parser.parse().map(Expr::RemoveTable),
				T![FUNCTION] => parser.parse().map(Expr::RemoveFunction),
				T![MODULE] => parser.parse().map(Expr::RemoveModule),
				T![PARAM] => parser.parse().map(Expr::RemoveParam),
				T![API] => parser.parse().map(Expr::RemoveApi),
				T![EVENT] => parser.parse().map(Expr::RemoveEvent),
				T![FIELD] => parser.parse().map(Expr::RemoveField),
				T![INDEX] => parser.parse().map(Expr::RemoveIndex),
				T![ANALYZER] => parser.parse().map(Expr::RemoveAnalyzer),
				T![BUCKET] => parser.parse().map(Expr::RemoveBucket),
				T![SEQUENCE] => parser.parse().map(Expr::RemoveSequence),
				T![USER] => parser.parse().map(Expr::RemoveUser),
				T![ACCESS] => parser.parse().map(Expr::RemoveAccess),
				T![CONFIG] => parser.parse().map(Expr::RemoveConfig),
				_ => {
					let _ = parser.next();
					Err(parser.unexpected(expected))
				}
			}
		}
		T![ALTER] => {
			let expected = "a resource type to alter";
			let Some(peek) = parser.peek1()? else {
				let _ = parser.next();
				return Err(parser.unexpected(expected));
			};
			match peek.token {
				T![SYSTEM] => parser.parse().map(Expr::AlterSystem),
				T![NAMESPACE] => parser.parse().map(Expr::AlterNamespace),
				T![DATABASE] => parser.parse().map(Expr::AlterDatabase),
				T![TABLE] | T![TB] => parser.parse().map(Expr::AlterTable),
				T![EVENT] => parser.parse().map(Expr::AlterEvent),
				T![PARAM] => parser.parse().map(Expr::AlterParam),
				T![FIELD] => parser.parse().map(Expr::AlterField),
				T![INDEX] => parser.parse().map(Expr::AlterIndex),
				T![SEQUENCE] => parser.parse().map(Expr::AlterSequence),
				T![BUCKET] => parser.parse().map(Expr::AlterBucket),
				T![ANALYZER] => parser.parse().map(Expr::AlterAnalyzer),
				T![FUNCTION] => parser.parse().map(Expr::AlterFunction),
				T![USER] => parser.parse().map(Expr::AlterUser),
				T![ACCESS] => parser.parse().map(Expr::AlterAccess),
				T![API] => parser.parse().map(Expr::AlterApi),
				T![CONFIG] => parser.parse().map(Expr::AlterConfig),
				_ => {
					let _ = parser.next();
					Err(parser.unexpected(expected))
				}
			}
		}
		T![EXPLAIN] => Ok(Expr::Explain(parser.parse()?)),
		T![@] => {
			let _ = parser.next();

			Ok(Expr::Document(parser.push(peek.span)))
		}
		T![<] => {
			if let Some(peek1) = parser.peek_joined1()?
				&& let T![->] | T![-] = peek1.token
			{
				Ok(Expr::Document(parser.push(peek.span.extend(peek1.span))))
			} else {
				Err(parser.with_error(|parser| {
					Level::Error
						.title(format!(
							"Unexpected token `{}` expected an expression",
							parser.slice(peek.span)
						))
						.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(peek.span)))
						.to_diagnostic()
				}))
			}
		}
		T![->] => Ok(Expr::Document(parser.push(peek.span))),
		T![<~] => Ok(Expr::Document(parser.push(peek.span))),
		BaseTokenKind::Param => {
			let path = parser.parse_sync()?;
			Ok(Expr::Param(path))
		}
		x if x.is_identifier() => {
			let peek1 = parser.peek1()?;

			if peek1.map(|x| x.token) == Some(T![:]) {
				let expr = parser.parse()?;
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
