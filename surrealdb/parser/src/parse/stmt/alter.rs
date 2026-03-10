use ast::{AlterKind, ChangeFeed, Permission, Schema};
use token::T;

use crate::parse::ParseResult;
use crate::parse::utils::{parse_unordered_clause, parse_unordered_clause_sync};
use crate::{Parse, Parser};

fn parse_if_not_exists(parser: &mut Parser<'_, '_>) -> ParseResult<bool> {
	if parser.eat(T![IF])?.is_some() {
		let _ = parser.expect(T![NOT])?;
		let _ = parser.expect(T![EXISTS])?;
		Ok(true)
	} else {
		Ok(false)
	}
}

impl Parse for ast::AlterSystem {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![SYSTEM])?;

		let mut query_timeout = None;
		let mut compact = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = parser.expect(T![QUERY_TIMEOUT])?;
					parse_unordered_clause_sync(
						parser,
						&mut query_timeout,
						peek.span.extend(expect.span),
						|_| Ok(AlterKind::Drop(expect.span)),
					)?;
				}
				T![QUERY_TIMEOUT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut query_timeout, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				T![COMPACT] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut compact, peek.span, |_| Ok(()))?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterSystem {
			query_timeout: query_timeout.map(|x| x.0),
			compact: compact.is_some(),
			span,
		})
	}
}

impl Parse for ast::AlterNamespace {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![NAMESPACE])?;

		let if_exists = parse_if_not_exists(parser)?;

		let name = parser.parse_enter().await?;

		let compact = parser.eat(T![COMPACT])?.is_some();

		let span = parser.span_since(alter.span);
		Ok(ast::AlterNamespace {
			if_exists,
			name,
			compact,
			span,
		})
	}
}

impl Parse for ast::AlterDatabase {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![DATABASE])?;

		let if_exists = parse_if_not_exists(parser)?;

		let name = parser.parse_enter().await?;

		let compact = parser.eat(T![COMPACT])?.is_some();

		let span = parser.span_since(alter.span);
		Ok(ast::AlterDatabase {
			if_exists,
			name,
			compact,
			span,
		})
	}
}

impl Parse for ast::AlterTable {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![TABLE])?;

		let if_exists = parse_if_not_exists(parser)?;

		let name = parser.parse_enter().await?;

		let mut changefeed = None;
		let mut comment = None;
		let mut compact = None;
		let mut permissions = None;
		let mut schema = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				_ => break,
				T![DROP] => {
					let expect = "`CHANGEFEED` or `COMMENT`";
					let drop = parser.peek_expect(expect)?;
					match drop.token {
						T![COMMENT] => {
							let _ = parser.next();
							parse_unordered_clause_sync(
								parser,
								&mut comment,
								peek.span.extend(drop.span),
								|_| Ok(AlterKind::Drop(drop.span)),
							)?;
						}
						T![CHANGEFEED] => {
							let _ = parser.next();
							parse_unordered_clause_sync(
								parser,
								&mut comment,
								peek.span.extend(drop.span),
								|_| Ok(AlterKind::Drop(drop.span)),
							)?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				T![CHANGEFEED] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut changefeed, peek.span, |parser| {
						parser.parse_sync().map(AlterKind::Set)
					})?;
				}
				T![COMPACT] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut compact, peek.span, |_| Ok(()))?;
				}
				T![PERMISSIONS] => {
					parse_unordered_clause(parser, &mut permissions, peek.span, Parser::parse)
						.await?;
				}
				T![SCHEMAFULL] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut schema, peek.span, |_| {
						Ok(Schema::Full)
					})?;
				}
				T![SCHEMALESS] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut schema, peek.span, |_| {
						Ok(Schema::Less)
					})?;
				}
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterTable {
			if_exists,
			name,
			comment: comment.map(|x| x.0),
			changefeed: changefeed.map(|x| x.0),
			compact: compact.is_some(),
			permissions: permissions.map(|x| x.0),
			schema: schema.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterIndex {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![INDEX])?;

		let if_exists = parse_if_not_exists(parser)?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let mut comment = None;
		let mut prepare_remove = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "`COMMENT`";
					let drop = parser.peek_expect(expect)?;
					match drop.token {
						T![COMMENT] => {
							let _ = parser.next();
							parse_unordered_clause_sync(
								parser,
								&mut comment,
								peek.span.extend(drop.span),
								|_| Ok(AlterKind::Drop(drop.span)),
							)?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![PREPARE] => {
					let _ = parser.next();
					let expect = parser.expect(T![REMOVE])?;
					parse_unordered_clause_sync(
						parser,
						&mut prepare_remove,
						peek.span.extend(expect.span),
						|_| Ok(()),
					)?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterIndex {
			if_exists,
			name,
			table,
			comment: comment.map(|x| x.0),
			prepare_remove: prepare_remove.is_some(),
			span,
		})
	}
}
