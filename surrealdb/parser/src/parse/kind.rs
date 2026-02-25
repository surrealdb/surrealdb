use ast::{GeometrySubType, IdentListType, NodeList, PrimeType, Spanned};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, T};

use super::Parser;
use crate::Parse;
use crate::parse::ParseResult;
use crate::parse::utils::{parse_delimited_list, parse_seperated_list, parse_seperated_list_sync};

fn parse_ident_list_type(span: Span, parser: &mut Parser) -> ParseResult<IdentListType> {
	let tables = if let Some(x) = parser.eat(T![<])? {
		let (_, head) = parse_seperated_list_sync(parser, T![|], |parser| parser.parse_sync())?;
		let _ = parser.expect_closing_delimiter(T![>], x.span)?;

		Some(head)
	} else {
		None
	};

	let span = parser.span_since(span);
	Ok(ast::IdentListType {
		idents: tables,
		span,
	})
}

fn parse_geometry_sub_type(parser: &mut Parser<'_, '_>) -> ParseResult<GeometrySubType> {
	let peek = parser.peek_expect("a geometry sub type")?;
	let res = match peek.token {
		T![POINT] => GeometrySubType::Point(peek.span),
		T![LINE] => GeometrySubType::Line(peek.span),
		T![POLYGON] => GeometrySubType::Polygon(peek.span),
		T![MULTIPOINT] => GeometrySubType::MultiPoint(peek.span),
		T![MULTILINE] => GeometrySubType::MultiLine(peek.span),
		T![MULTIPOLYGON] => GeometrySubType::MultiPolygon(peek.span),
		T![COLLECTION] => GeometrySubType::Collection(peek.span),
		_ => return Err(parser.unexpected("a geometry sub type")),
	};

	let _ = parser.next();
	Ok(res)
}

async fn parse_prime_type(parser: &mut Parser<'_, '_>) -> ParseResult<PrimeType> {
	let peek = parser.peek_expect("a kind")?;
	match peek.token {
		T![ANY] => Err(parser.with_error(|parser| {
			Level::Error
				.title(format!("Unexpected token `{}`", parser.slice(peek.span)))
				.snippet(
					parser.snippet().annotate(
						AnnotationKind::Primary
							.span(peek.span)
							.label("Cannot create a union with kind `any`"),
					),
				)
				.to_diagnostic()
		})),
		T![NONE] => {
			let _ = parser.next();
			let builtin = parser.push(ast::Builtin::None(peek.span));
			Ok(ast::PrimeType::LitBuiltin(builtin))
		}
		T![NULL] => {
			let _ = parser.next();
			let builtin = parser.push(ast::Builtin::Null(peek.span));
			Ok(ast::PrimeType::LitBuiltin(builtin))
		}
		T![true] => {
			let _ = parser.next();
			let builtin = parser.push(ast::Builtin::True(peek.span));
			Ok(ast::PrimeType::LitBuiltin(builtin))
		}
		T![false] => {
			let _ = parser.next();
			let builtin = parser.push(ast::Builtin::False(peek.span));
			Ok(ast::PrimeType::LitBuiltin(builtin))
		}
		BaseTokenKind::NaN => {
			let _ = parser.next();
			let builtin = parser.push(Spanned {
				value: f64::NAN,
				span: peek.span,
			});
			Ok(ast::PrimeType::LitFloat(builtin))
		}
		BaseTokenKind::PosInfinity => {
			let _ = parser.next();
			let builtin = parser.push(Spanned {
				value: f64::INFINITY,
				span: peek.span,
			});
			Ok(ast::PrimeType::LitFloat(builtin))
		}
		BaseTokenKind::NegInfinity => {
			let _ = parser.next();
			let builtin = parser.push(Spanned {
				value: f64::NEG_INFINITY,
				span: peek.span,
			});
			Ok(ast::PrimeType::LitFloat(builtin))
		}
		BaseTokenKind::Int => {
			let int = parser.parse_sync_push()?;
			Ok(ast::PrimeType::LitInteger(int))
		}
		BaseTokenKind::Float => {
			let int = parser.parse_sync_push()?;
			Ok(ast::PrimeType::LitFloat(int))
		}
		BaseTokenKind::Decimal => {
			let int = parser.parse_sync_push()?;
			Ok(ast::PrimeType::LitDecimal(int))
		}
		BaseTokenKind::OpenBrace => {
			let (span, entries) = parse_delimited_list(
				parser,
				BaseTokenKind::OpenBrace,
				BaseTokenKind::CloseBrace,
				T![,],
				async |parser| {
					let key_peek = parser.peek_expect("an object key")?;
					let key = match key_peek.token {
						BaseTokenKind::String => {
							let _ = parser.next();
							let text = parser.unescape_str_push(key_peek)?;
							parser.push(ast::Ident {
								text,
								span: key_peek.span,
							})
						}
						x if x.is_identifier() => parser.parse_sync_push()?,
						_ => return Err(parser.unexpected("an object key")),
					};
					let _ = parser.expect(T![:])?;
					let ty = parser.parse_enter_push().await?;

					let span = parser.span_since(key_peek.span);

					Ok(ast::LitObjectTypeEntry {
						name: key,
						ty,
						span,
					})
				},
			)
			.await?;

			let ty = parser.push(ast::LitObjectType {
				entries,
				span,
			});

			Ok(ast::PrimeType::LitObject(ty))
		}
		BaseTokenKind::OpenBracket => {
			let (span, entries) = parse_delimited_list(
				parser,
				BaseTokenKind::OpenBracket,
				BaseTokenKind::CloseBracket,
				T![,],
				async |parser| parser.parse_enter().await,
			)
			.await?;

			let ty = parser.push(ast::LitArrayType {
				entries,
				span,
			});

			Ok(ast::PrimeType::LitArray(ty))
		}
		T![INT] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Integer(peek.span))
		}
		T![FLOAT] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Float(peek.span))
		}
		T![DECIMAL] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Decimal(peek.span))
		}
		T![NUMBER] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Number(peek.span))
		}
		T![OBJECT] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Object(peek.span))
		}
		T![DURATION] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Duration(peek.span))
		}
		T![DATETIME] => {
			let _ = parser.next();
			Ok(ast::PrimeType::DateTime(peek.span))
		}
		T![BYTES] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Bytes(peek.span))
		}
		T![BOOL] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Bool(peek.span))
		}
		T![REGEX] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Regex(peek.span))
		}
		T![UUID] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Uuid(peek.span))
		}
		T![STRING] => {
			let _ = parser.next();
			Ok(ast::PrimeType::String(peek.span))
		}
		T![FUNCTION] => {
			let _ = parser.next();
			Ok(ast::PrimeType::Function(peek.span))
		}
		T![RECORD] => {
			let _ = parser.next();

			let ty = parse_ident_list_type(peek.span, parser)?;
			let ty = parser.push(ty);

			Ok(ast::PrimeType::Record(ty))
		}
		T![TABLE] => {
			let _ = parser.next();

			let ty = parse_ident_list_type(peek.span, parser)?;
			let ty = parser.push(ty);

			Ok(ast::PrimeType::Table(ty))
		}
		T![FILE] => {
			let _ = parser.next();

			let ty = parse_ident_list_type(peek.span, parser)?;
			let ty = parser.push(ty);

			Ok(ast::PrimeType::File(ty))
		}
		T![ARRAY] => {
			let _ = parser.next();

			let (ty, size) = if let Some(x) = parser.eat(T![<])? {
				let ty = parser.parse_enter_push().await?;
				let size = if parser.eat(T![,])?.is_some() {
					Some(parser.parse_sync_push()?)
				} else {
					None
				};
				let _ = parser.expect_closing_delimiter(T![>], x.span)?;
				(Some(ty), size)
			} else {
				(None, None)
			};

			let span = parser.span_since(peek.span);
			let ty = parser.push(ast::ArrayLikeType {
				ty,
				size,
				span,
			});

			Ok(ast::PrimeType::Array(ty))
		}
		T![SET] => {
			let _ = parser.next();

			let (ty, size) = if let Some(x) = parser.eat(T![<])? {
				let ty = parser.parse_enter_push().await?;
				let size = if parser.eat(T![,])?.is_some() {
					Some(parser.parse_sync_push()?)
				} else {
					None
				};
				let _ = parser.expect_closing_delimiter(T![>], x.span)?;
				(Some(ty), size)
			} else {
				(None, None)
			};

			let span = parser.span_since(peek.span);
			let ty = parser.push(ast::ArrayLikeType {
				ty,
				size,
				span,
			});

			Ok(ast::PrimeType::Set(ty))
		}
		T![GEOMETRY] => {
			let _ = parser.next();

			let types = if let Some(x) = parser.eat(T![<])? {
				let (_, head) = parse_seperated_list_sync(parser, T![|], parse_geometry_sub_type)?;
				let _ = parser.expect_closing_delimiter(T![>], x.span)?;

				Some(head)
			} else {
				None
			};

			let span = parser.span_since(peek.span);
			let ty = parser.push(ast::GeometryType {
				types,
				span,
			});

			Ok(ast::PrimeType::Geometry(ty))
		}

		_ => Err(parser.unexpected("a kind")),
	}
}

fn no_either_type(parser: &mut Parser<'_, '_>, cause: Span) -> ParseResult<()> {
	if let Some(x) = parser.peek()?
		&& let T![|] = x.token
	{
		return Err(parser.with_error(|parser| {
			Level::Error
				.title("Unexpected token `|`")
				.snippet(
					parser.snippet().annotate(AnnotationKind::Primary.span(x.span)).annotate(
						AnnotationKind::Context
							.span(cause)
							.label("Cannot union kinds with kind `any` or `option<..>`"),
					),
				)
				.to_diagnostic()
		}));
	}
	Ok(())
}

impl Parse for ast::Type {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let peek = parser.peek_expect("a kind")?;
		match peek.token {
			T![ANY] => {
				let _ = parser.next()?;
				let ty = ast::Type::Any(peek.span);

				no_either_type(parser, peek.span)?;

				return Ok(ty);
			}
			T![OPTION] => {
				let _ = parser.next()?;
				let start = parser.expect(T![<])?;
				let (_, head) = parse_seperated_list(parser, T![|], parse_prime_type).await?;
				let _ = parser.expect_closing_delimiter(T![>], start.span)?;

				let none = parser.push(ast::PrimeType::None(peek.span));
				let head = parser.push_list_item(NodeList {
					cur: none,
					next: Some(head),
				});

				no_either_type(parser, peek.span)?;

				return Ok(ast::Type::Prime(head));
			}
			_ => {}
		}

		let (_, head) = parse_seperated_list(parser, T![|], parse_prime_type).await?;
		Ok(ast::Type::Prime(head))
	}
}
