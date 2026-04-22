use std::ops::Bound;

use ast::Spanned;
use common::span::Span;
use token::{BaseTokenKind, Joined, RecordIdKeyToken, T, Token};

use crate::parse::ParseResult;
use crate::parse::peek::peek_joined_starts_record_id_key;
use crate::parse::range::TryRange;
use crate::{Parse, Parser};

fn parse_int_id(
	parser: &mut Parser<'_, '_>,
	sign: Option<ast::Sign>,
	int_span: Span,
) -> ParseResult<ast::RecordIdKey> {
	if let Some(x) = parser.peek_joined()?
		&& (x.token.is_identifier()
			|| matches!(x.token, BaseTokenKind::NaN | BaseTokenKind::Infinity))
	{
		let _ = parser.next();
		let mut span = int_span.extend(x.span);
		if sign.is_some() {
			span.start -= 1;
		}
		let slice = parser.slice(span);
		let text = parser.push_set_entry(slice);
		let lit = parser.push(ast::StringLit {
			text,
			span,
		});
		Ok(ast::RecordIdKey::String(lit))
	} else {
		let speculate = parser.speculate_sync(|parser| parser.parse_sync::<ast::Integer>())?;
		if let Some(int) = speculate {
			if let Some(i) = int.into_i64() {
				Ok(ast::RecordIdKey::Number(Spanned {
					value: i,
					span: int.span,
				}))
			} else {
				let slice = parser.slice(int.span);
				let text = parser.push_set_entry(slice);
				let lit = parser.push(ast::StringLit {
					text,
					span: int.span,
				});
				Ok(ast::RecordIdKey::String(lit))
			}
		} else {
			let _ = parser.next();
			let mut span = int_span;
			if sign.is_some() {
				span.start -= 1;
			}
			let slice = parser.slice(span);
			let text = parser.push_set_entry(slice);
			let lit = parser.push(ast::StringLit {
				text,
				span,
			});
			Ok(ast::RecordIdKey::String(lit))
		}
	}
}

pub async fn parse_peeked_record_id_key(
	parser: &mut Parser<'_, '_>,
) -> ParseResult<ast::RecordIdKey> {
	let peek = parser.peek_expect("a record id key")?;
	let key = match peek.token {
		BaseTokenKind::OpenBrace => ast::RecordIdKey::Object(parser.parse().await?),
		BaseTokenKind::OpenBracket => ast::RecordIdKey::Array(parser.parse().await?),
		BaseTokenKind::String => ast::RecordIdKey::String(parser.parse_sync()?),
		T![+] => {
			if let Some(peek1) = parser.peek_joined1()?
				&& let BaseTokenKind::Int = peek1.token
			{
				let _ = parser.next();
				parse_int_id(parser, Some(ast::Sign::Plus), peek1.span)?
			} else {
				return Err(parser.unexpected("a record id key"));
			}
		}
		T![-] => {
			if let Some(peek1) = parser.peek_joined1()?
				&& let BaseTokenKind::Int = peek1.token
			{
				let _ = parser.next();
				parse_int_id(parser, Some(ast::Sign::Minus), peek1.span)?
			} else {
				return Err(parser.unexpected("a record id key"));
			}
		}
		BaseTokenKind::Int => parse_int_id(parser, None, peek.span)?,
		BaseTokenKind::UuidString => {
			let uuid = parser.parse_sync()?;
			ast::RecordIdKey::Uuid(uuid)
		}
		x if x.is_identifier() => {
			let _ = parser.next();
			let text = parser.unescape_ident(peek)?;
			let i = parser.push(ast::StringLit {
				text,
				span: peek.span,
			});
			ast::RecordIdKey::String(i)
		}
		BaseTokenKind::NaN | BaseTokenKind::Infinity => {
			let _ = parser.next();
			let slice = parser.slice(peek.span);
			let text = parser.push_set_entry(slice);
			let i = parser.push(ast::StringLit {
				text,
				span: peek.span,
			});
			ast::RecordIdKey::String(i)
		}
		_ => return Err(parser.unexpected("a record id key")),
	};
	Ok(key)
}

pub fn peek_record_id_token(parser: &mut Parser<'_, '_>) -> ParseResult<Option<Token>> {
	let token = parser.lex(|lexer, _| {
		let lexer = lexer.morph::<RecordIdKeyToken>();
		let mut lexer_clone = lexer.clone();
		match lexer_clone.next() {
			Some(Ok(x)) => {
				let span = lexer_clone.span();
				Ok((lexer_clone.morph(), Some((x, span))))
			}
			_ => Ok((lexer.morph(), None)),
		}
	})?;

	let Some((token, span)) = token else {
		return Ok(None);
	};

	let span = Span::from_usize_range(span).expect("span to be in range");

	let token = match token {
		RecordIdKeyToken::Range => T![..],
		RecordIdKeyToken::Rand => T![RAND],
		RecordIdKeyToken::Uuid => T![UUID],
		RecordIdKeyToken::Ulid => T![ULID],
		RecordIdKeyToken::OpenBrace => BaseTokenKind::OpenBrace,
		RecordIdKeyToken::OpenBracket => BaseTokenKind::OpenBracket,
		RecordIdKeyToken::UuidString => BaseTokenKind::UuidString,
		RecordIdKeyToken::String => BaseTokenKind::String,
		RecordIdKeyToken::Number => {
			if parser.slice(span).starts_with('+') {
				let token = Token {
					token: T![+],
					joined: Joined::Joined,
					span: Span {
						start: span.start,
						end: span.start + 1,
					},
				};
				parser.lex.push_token(token);
				parser.lex.push_token(Token {
					token: BaseTokenKind::Int,
					joined: Joined::Joined,
					span: Span {
						start: span.start + 1,
						end: span.end,
					},
				});
				return Ok(Some(token));
			} else if parser.slice(span).starts_with('-') {
				let token = Token {
					token: T![-],
					joined: Joined::Joined,
					span: Span {
						start: span.start,
						end: span.start + 1,
					},
				};
				parser.lex.push_token(token);
				parser.lex.push_token(Token {
					token: BaseTokenKind::Int,
					joined: Joined::Joined,
					span: Span {
						start: span.start + 1,
						end: span.end,
					},
				});
				return Ok(Some(token));
			}
			BaseTokenKind::Int
		}
		RecordIdKeyToken::Identifier => BaseTokenKind::Ident,
	};

	let token = Token {
		token,
		joined: Joined::Joined,
		span,
	};
	parser.lex.push_token(token);
	Ok(Some(token))
}

pub async fn parse_record_id_headless_range(
	parser: &mut Parser<'_, '_>,
) -> ParseResult<Bound<ast::RecordIdKey>> {
	let end = if peek_joined_starts_record_id_key(parser)? {
		let item = parse_peeked_record_id_key(parser).await?;
		Bound::Excluded(item)
	} else if let Some(T![=]) = parser.peek_joined()?.map(|x| x.token) {
		let _ = parser.next()?;

		peek_record_id_token(parser)?;
		let item = parse_peeked_record_id_key(parser).await?;
		Bound::Included(item)
	} else {
		Bound::Unbounded
	};
	Ok(end)
}

pub async fn try_parse_record_id_range(
	parser: &mut Parser<'_, '_>,
	start: ast::RecordIdKey,
) -> ParseResult<TryRange<ast::RecordIdKey>> {
	let peek = parser.peek()?;
	let res = match peek.map(|x| x.token) {
		Some(T![..]) => {
			let _ = parser.next();
			if peek_joined_starts_record_id_key(parser)? {
				let end = parse_peeked_record_id_key(parser).await?;

				TryRange::Some {
					start: Bound::Included(start),
					end: Bound::Excluded(end),
				}
			} else if let Some(T![=]) = parser.peek_joined()?.map(|x| x.token) {
				let _ = parser.next();

				peek_record_id_token(parser)?;
				let end = parse_peeked_record_id_key(parser).await?;

				TryRange::Some {
					start: Bound::Included(start),
					end: Bound::Included(end),
				}
			} else {
				TryRange::Some {
					start: Bound::Included(start),
					end: Bound::Unbounded,
				}
			}
		}
		Some(T![>]) => {
			let _ = parser.next();
			if !matches!(parser.peek_joined()?.map(|x| x.token), Some(T![..])) {
				let Some(peek) = peek else {
					unreachable!()
				};
				return Err(parser.unexpected_token("a range operator", peek));
			}
			let _ = parser.next();
			if peek_joined_starts_record_id_key(parser)? {
				let end = parse_peeked_record_id_key(parser).await?;

				TryRange::Some {
					start: Bound::Excluded(start),
					end: Bound::Excluded(end),
				}
			} else if let Some(T![=]) = parser.peek_joined()?.map(|x| x.token) {
				let _ = parser.next();
				peek_record_id_token(parser)?;
				let end = parse_peeked_record_id_key(parser).await?;

				TryRange::Some {
					start: Bound::Excluded(start),
					end: Bound::Included(end),
				}
			} else {
				TryRange::Some {
					start: Bound::Excluded(start),
					end: Bound::Unbounded,
				}
			}
		}
		_ => TryRange::None(start),
	};
	Ok(res)
}

impl Parse for ast::RecordId {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start_span = parser.peek_span();
		let name = parser.parse_sync()?;
		let name = parser.push(name);
		let _ = parser.expect(T![:])?;

		// Record id keys need a special implementation to properly support.
		//
		// All the code below is to correctly support record id key identifiers which can start
		// with a digit.
		// This leads to multiple issues, first we need a second lexer as the old one will not
		// parse identifiers starting with a digit. Second we need to fallback if integers do not
		// fit in i64 as 9999999999999999 is also a valid identifier.
		//
		// We first run the special lexer and the push that token as the next peeked token so that
		// we can reuse existing code.
		let Some(token) = peek_record_id_token(parser)? else {
			return Err(parser.unexpected("a record id key"));
		};
		let key = match token.token {
			T![..] => {
				let _ = parser.next();
				let end = parse_record_id_headless_range(parser).await?;
				let end = end.map(|x| parser.push(x));
				let span = parser.span_since(token.span);
				let range = ast::RecordIdKeyRange {
					start: Bound::Unbounded,
					end,
					span,
				};
				ast::RecordIdKey::Range(parser.push(range))
			}
			T![RAND] => {
				let _ = parser.next();
				if let Some(x) = parser.peek()?
					&& let BaseTokenKind::OpenParen = x.token
				{
					let _ = parser.next();
					let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, x.span)?;
					let span = parser.span_since(token.span);
					ast::RecordIdKey::Generate(Spanned {
						value: ast::RecordIdKeyGenerate::Rand,
						span,
					})
				} else {
					let slice = parser.slice(token.span);
					let text = parser.push_set_entry(slice);
					let ident = parser.push(ast::StringLit {
						text,
						span: token.span,
					});
					ast::RecordIdKey::String(ident)
				}
			}
			T![UUID] => {
				let _ = parser.next();
				if let Some(x) = parser.peek()?
					&& let BaseTokenKind::OpenParen = x.token
				{
					let _ = parser.next();
					let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, x.span)?;
					let span = parser.span_since(token.span);
					ast::RecordIdKey::Generate(Spanned {
						value: ast::RecordIdKeyGenerate::Uuid,
						span,
					})
				} else {
					let slice = parser.slice(token.span);
					let text = parser.push_set_entry(slice);
					let ident = parser.push(ast::StringLit {
						text,
						span: token.span,
					});
					ast::RecordIdKey::String(ident)
				}
			}
			T![ULID] => {
				let _ = parser.next();
				if let Some(x) = parser.peek()?
					&& let BaseTokenKind::OpenParen = x.token
				{
					let _ = parser.next();
					let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, x.span)?;
					let span = parser.span_since(token.span);
					ast::RecordIdKey::Generate(Spanned {
						value: ast::RecordIdKeyGenerate::Ulid,
						span,
					})
				} else {
					let slice = parser.slice(token.span);
					let text = parser.push_set_entry(slice);
					let ident = parser.push(ast::StringLit {
						text,
						span: token.span,
					});
					ast::RecordIdKey::String(ident)
				}
			}
			_ => {
				let start = parse_peeked_record_id_key(parser).await?;

				let range = try_parse_record_id_range(parser, start).await?;

				match range {
					TryRange::None(key) => {
						let key = parser.push(key);

						return Ok(ast::RecordId {
							name,
							key,
							span: parser.span_since(start_span),
						});
					}
					TryRange::Some {
						start,
						end,
					} => {
						let span = parser.span_since(token.span);
						let range = ast::RecordIdKeyRange {
							start: start.map(|x| parser.push(x)),
							end: end.map(|x| parser.push(x)),
							span,
						};
						ast::RecordIdKey::Range(parser.push(range))
					}
				}
			}
		};

		let key = parser.push(key);

		Ok(ast::RecordId {
			name,
			key,
			span: parser.span_since(start_span),
		})
	}
}
