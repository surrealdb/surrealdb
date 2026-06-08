use ast::{AlterKind, NodeId, Schema};
use common::span::Span;
use token::{BaseTokenKind, T};

use crate::parse::utils::{
	parse_delimited_list, parse_seperated_list, parse_seperated_list_sync, parse_unordered_clause,
	parse_unordered_clause_sync, redefined_error,
};
use crate::parse::{ParseResult, ParserSettings};
use crate::{Parse, Parser};

fn parse_if_exists(parser: &mut Parser<'_, '_>) -> ParseResult<bool> {
	if parser.eat(T![IF])?.is_some() {
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

		let if_exists = parse_if_exists(parser)?;

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

		let if_exists = parse_if_exists(parser)?;

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

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;

		let mut changefeed = None;
		let mut comment = None;
		let mut compact = None;
		let mut permissions = None;
		let mut schema = None;
		let mut table_kind = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "`CHANGEFEED` or `COMMENT`";
					let drop = parser.peek_expect(expect)?;
					match drop.token {
						T![COMMENT] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut comment, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![CHANGEFEED] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut changefeed, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
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
				T![TYPE] => {
					let _ = parser.next();
					parse_unordered_clause_sync(
						parser,
						&mut table_kind,
						peek.span,
						Parser::parse_sync,
					)?;
				}
				_ => break,
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
			table_kind: table_kind.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterEvent {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![EVENT])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let mut condition = None;
		let mut then = None;
		let mut async_ = None;
		let mut comment = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "`WHEN`, `THEN`, `ASYNC` or `COMMENT`";
					let drop = parser.peek_expect(expect)?;
					match drop.token {
						T![WHEN] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut condition, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![THEN] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut then, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![ASYNC] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut async_, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![COMMENT] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut comment, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![WHEN] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut condition, peek.span, async |x| {
						Ok(AlterKind::Set(x.parse_enter().await?))
					})
					.await?;
				}
				T![THEN] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut then, peek.span, async |x| {
						Ok(AlterKind::Set(
							parse_seperated_list(x, T![,], Parser::parse_enter).await?.1,
						))
					})
					.await?;
				}
				T![ASYNC] => {
					parse_unordered_clause_sync(parser, &mut async_, peek.span, |x| {
						Ok(AlterKind::Set(x.parse_sync()?))
					})?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, async |x| {
						Ok(AlterKind::Set(x.parse_enter().await?))
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterEvent {
			if_exists,
			name,
			table,
			condition: condition.map(|x| x.0),
			then: then.map(|x| x.0),
			async_: async_.map(|x| x.0),
			comment: comment.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterParam {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![PARAM])?;
		let if_exists = parse_if_exists(parser)?;

		let param = parser.parse_sync()?;

		let mut value = None;
		let mut comment = None;
		let mut permissions = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let _ = parser.expect(T![COMMENT])?;
					let span = parser.span_since(peek.span);
					parse_unordered_clause_sync(parser, &mut comment, span, |_| {
						Ok(AlterKind::Drop(span))
					})?;
				}
				T![VALUE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut value, peek.span, Parser::parse_enter)
						.await?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, async |x| {
						Ok(AlterKind::Set(x.parse_enter().await?))
					})
					.await?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(
						parser,
						&mut permissions,
						peek.span,
						Parser::parse_enter,
					)
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterParam {
			if_exists,
			param,
			value: value.map(|x| x.0),
			comment: comment.map(|x| x.0),
			permissions: permissions.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterBucket {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![BUCKET])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;

		let mut backend = None;
		let mut readonly = None;
		let mut permissions = None;
		let mut comment = None;

		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "`READONLY`, `BACKEND`, or `COMMENT`";
					let token = parser.peek_expect(expect)?;
					match token.token {
						T![READONLY] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut readonly, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![BACKEND] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut backend, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![COMMENT] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut comment, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![BACKEND] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut backend, peek.span, |p| {
						Ok(AlterKind::Set(p.parse_sync()?))
					})?;
				}
				T![READONLY] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut readonly, peek.span, |_| {
						Ok(AlterKind::Set(()))
					})?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permissions, peek.span, Parser::parse)
						.await?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, async |p| {
						Ok(AlterKind::Set(p.parse_enter().await?))
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterBucket {
			if_exists,
			name,
			backend: backend.map(|x| x.0),
			readonly: readonly.map(|x| x.0),
			permissions: permissions.map(|x| x.0),
			comment: comment.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterAnalyzer {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![ANALYZER])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;

		let mut function = None;
		let mut tokenizer = None;
		let mut filter = None;
		let mut comment = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "`FUNCTION`, `TOKENIZERS`, `FILTERS`, or `COMMENT`";
					let token = parser.peek_expect(expect)?;
					match token.token {
						T![FUNCTION] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut function, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![TOKENIZERS] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut tokenizer, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![FILTERS] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut filter, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![COMMENT] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut comment, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![FUNCTION] => {
					let _ = parser.next();
					let span = parser.span_since(peek.span);
					parse_unordered_clause_sync(parser, &mut function, span, |p| {
						Ok(AlterKind::Set(p.parse_sync()?))
					})?;
				}
				T![TOKENIZERS] => {
					let _ = parser.next();
					let span = parser.span_since(peek.span);
					parse_unordered_clause_sync(parser, &mut tokenizer, span, |p| {
						Ok(AlterKind::Set(
							parse_seperated_list_sync(p, T![,], Parser::parse_sync)?.1,
						))
					})?;
				}
				T![FILTERS] => {
					let _ = parser.next();
					let span = parser.span_since(peek.span);
					parse_unordered_clause_sync(parser, &mut filter, span, |p| {
						Ok(AlterKind::Set(
							parse_seperated_list_sync(p, T![,], Parser::parse_sync)?.1,
						))
					})?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					let span = parser.span_since(peek.span);
					parse_unordered_clause(parser, &mut comment, span, async |p| {
						Ok(AlterKind::Set(p.parse_enter().await?))
					})
					.await?;
				}

				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterAnalyzer {
			if_exists,
			name,
			function: function.map(|x| x.0),
			tokenizer: tokenizer.map(|x| x.0),
			filter: filter.map(|x| x.0),
			comment: comment.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterField {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![FIELD])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let mut ty = None;
		let mut flexible = None;
		let mut readonly = None;
		let mut value = None;
		let mut assert = None;
		let mut default = None;
		let mut comment = None;
		let mut on_delete = None;
		let mut permissions = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "`TYPE`, `FLEXIBLE`, `READONLY`, `VALUE`, `ASSERT`, `DEFAULT`, `COMMENT`, or `REFERENCE`";
					let drop = parser.peek_expect(expect)?;
					match drop.token {
						T![TYPE] => {
							let _ = parser.next();
							let span = parser.span_since(drop.span);
							parse_unordered_clause_sync(parser, &mut ty, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![FLEXIBLE] => {
							let _ = parser.next();
							let span = parser.span_since(drop.span);
							parse_unordered_clause_sync(parser, &mut flexible, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![READONLY] => {
							let _ = parser.next();
							let span = parser.span_since(drop.span);
							parse_unordered_clause_sync(parser, &mut readonly, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![VALUE] => {
							let _ = parser.next();
							let span = parser.span_since(drop.span);
							parse_unordered_clause_sync(parser, &mut value, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![ASSERT] => {
							let _ = parser.next();
							let span = parser.span_since(drop.span);
							parse_unordered_clause_sync(parser, &mut assert, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![DEFAULT] => {
							let _ = parser.next();
							let span = parser.span_since(drop.span);
							parse_unordered_clause_sync(parser, &mut default, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![COMMENT] => {
							let _ = parser.next();
							let span = parser.span_since(drop.span);
							parse_unordered_clause_sync(parser, &mut comment, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![REFERENCE] => {
							let _ = parser.next();
							let span = parser.span_since(drop.span);
							parse_unordered_clause_sync(parser, &mut on_delete, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![TYPE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut ty, peek.span, async |parser| {
						parser.parse().await.map(AlterKind::Set)
					})
					.await?;
				}

				T![FLEXIBLE] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut flexible, peek.span, |_| {
						Ok(AlterKind::Set(()))
					})?;
				}
				T![READONLY] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut readonly, peek.span, |_| {
						Ok(AlterKind::Set(()))
					})?;
				}
				T![VALUE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut value, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				T![ASSERT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut assert, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				T![DEFAULT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut default, peek.span, async |parser| {
						if parser.eat(T![ALWAYS])?.is_some() {
							parser
								.parse_enter()
								.await
								.map(ast::FieldDefault::Always)
								.map(AlterKind::Set)
						} else {
							parser
								.parse_enter()
								.await
								.map(ast::FieldDefault::Some)
								.map(AlterKind::Set)
						}
					})
					.await?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				T![REFERENCE] => {
					parse_unordered_clause(parser, &mut on_delete, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				T![PERMISSIONS] => {
					parse_unordered_clause(parser, &mut permissions, peek.span, Parser::parse)
						.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterField {
			if_exists,
			name,
			table,
			ty: ty.map(|x| x.0),
			value: value.map(|x| x.0),
			assert: assert.map(|x| x.0),
			default: default.map(|x| x.0),
			flexible: flexible.map(|x| x.0),
			readonly: readonly.map(|x| x.0),
			comment: comment.map(|x| x.0),
			on_delete: on_delete.map(|x| x.0),
			permissions: permissions.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterIndex {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![INDEX])?;

		let if_exists = parse_if_exists(parser)?;

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
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut comment, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
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

impl Parse for ast::AlterSequence {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![SEQUENCE])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;

		let mut timeout = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "`TIMEOUT`";
					let drop = parser.peek_expect(expect)?;
					match drop.token {
						T![TIMEOUT] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(
								parser,
								&mut timeout,
								peek.span.extend(span),
								|_| Ok(AlterKind::Drop(span)),
							)?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![TIMEOUT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut timeout, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterSequence {
			if_exists,
			name,
			timeout: timeout.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterFunction {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![FUNCTION])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_sync::<NodeId<ast::Path>>()?;

		let mut comment = None;
		let mut body = None;
		let mut parameters = None;
		let mut return_ty = None;
		let mut permission = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let _ = parser.expect(T![COMMENT])?;
					let span = parser.span_since(peek.span);
					parse_unordered_clause_sync(
						parser,
						&mut comment,
						peek.span.extend(span),
						|_| Ok(AlterKind::Drop(span)),
					)?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				BaseTokenKind::OpenParen => {
					parse_unordered_clause(parser, &mut body, peek.span, async |parser| {
						parameters = Some(
							parse_delimited_list(
								parser,
								BaseTokenKind::OpenParen,
								BaseTokenKind::CloseParen,
								T![,],
								Parser::parse,
							)
							.await?
							.1,
						);

						if parser.eat(T![->])?.is_some() {
							return_ty = Some(parser.parse().await?)
						} else {
							return_ty = None
						}

						parser.parse().await
					})
					.await?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permission, peek.span, Parser::parse)
						.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterFunction {
			if_exists,
			name,
			comment: comment.map(|x| x.0),
			parameters,
			return_ty,
			body: body.map(|x| x.0),
			permission: permission.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterUser {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![USER])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let base = parser.parse_sync()?;

		let mut secret = None;
		let mut comment = None;
		let mut roles = None;
		let mut session_duration = None;
		let mut token_duration = None;

		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "COMMENT";
					let drop = parser.peek_expect(expect)?;
					match drop.token {
						T![COMMENT] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut comment, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![PASSHASH] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut secret, peek.span, |parser| {
						parser.parse_sync().map(ast::UserSecret::PassHash)
					})?;
				}
				T![PASSWORD] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut secret, peek.span, |parser| {
						parser.parse_sync().map(ast::UserSecret::PassWord)
					})?;
				}
				T![ROLES] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut roles, peek.span, |parser| {
						Ok(parse_seperated_list_sync(parser, T![,], Parser::parse_sync)?.1)
					})?;
				}
				T![DURATION] => {
					let _ = parser.next();
					let _ = parser.expect(T![FOR])?;
					loop {
						let expect = "TOKEN, or SESSION";
						let peek = parser.peek_expect(expect)?;
						match peek.token {
							T![TOKEN] => {
								let _ = parser.next();
								if parser.eat(T![NONE])?.is_some() {
									parse_unordered_clause_sync(
										parser,
										&mut token_duration,
										peek.span,
										|_| Ok(AlterKind::Drop(peek.span)),
									)?;
								} else {
									parse_unordered_clause_sync(
										parser,
										&mut token_duration,
										peek.span,
										|p| Ok(AlterKind::Set(p.parse_sync()?)),
									)?;
								}
							}
							T![SESSION] => {
								let _ = parser.next();
								if parser.eat(T![NONE])?.is_some() {
									parse_unordered_clause_sync(
										parser,
										&mut session_duration,
										peek.span,
										|_| Ok(AlterKind::Drop(peek.span)),
									)?;
								} else {
									parse_unordered_clause_sync(
										parser,
										&mut session_duration,
										peek.span,
										|p| Ok(AlterKind::Set(p.parse_sync()?)),
									)?;
								}
							}
							_ => return Err(parser.unexpected(expect)),
						}

						let _ = parser.eat(T![,])?;
						if parser.eat(T![FOR])?.is_none() {
							break;
						}
					}
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
		Ok(ast::AlterUser {
			if_exists,
			name,
			base,
			span,
			secret: secret.map(|x| x.0),
			roles: roles.map(|x| x.0),
			token_duration: token_duration.map(|x| x.0),
			session_duration: session_duration.map(|x| x.0),
			comment: comment.map(|x| x.0),
		})
	}
}

impl Parse for ast::AlterAccess {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![ACCESS])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let base = parser.parse_sync()?;

		let mut comment = None;
		let mut authenticate = None;
		let mut grant_duration = None;
		let mut session_duration = None;
		let mut token_duration = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let expect = "`AUTHENTICATE` or `COMMENT`";
					let drop = parser.peek_expect(expect)?;
					match drop.token {
						T![AUTHENTICATE] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut authenticate, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						T![COMMENT] => {
							let _ = parser.next();
							let span = parser.span_since(peek.span);
							parse_unordered_clause_sync(parser, &mut comment, span, |_| {
								Ok(AlterKind::Drop(span))
							})?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				T![DURATION] => {
					let _ = parser.next();
					let _ = parser.expect(T![FOR])?;
					loop {
						let expect = "`TOKEN`, `GRANT`, or `SESSION`";
						let peek = parser.peek_expect(expect)?;
						match peek.token {
							T![TOKEN] => {
								let _ = parser.next();
								if parser.eat(T![NONE])?.is_some() {
									parse_unordered_clause_sync(
										parser,
										&mut token_duration,
										peek.span,
										|_| Ok(AlterKind::Drop(peek.span)),
									)?;
								} else {
									parse_unordered_clause_sync(
										parser,
										&mut token_duration,
										peek.span,
										|p| Ok(AlterKind::Set(p.parse_sync()?)),
									)?;
								}
							}
							T![SESSION] => {
								let _ = parser.next();
								if parser.eat(T![NONE])?.is_some() {
									parse_unordered_clause_sync(
										parser,
										&mut session_duration,
										peek.span,
										|_| Ok(AlterKind::Drop(peek.span)),
									)?;
								} else {
									parse_unordered_clause_sync(
										parser,
										&mut session_duration,
										peek.span,
										|p| Ok(AlterKind::Set(p.parse_sync()?)),
									)?;
								}
							}
							T![GRANT] => {
								let _ = parser.next();
								if parser.eat(T![NONE])?.is_some() {
									parse_unordered_clause_sync(
										parser,
										&mut grant_duration,
										peek.span,
										|_| Ok(AlterKind::Drop(peek.span)),
									)?;
								} else {
									parse_unordered_clause_sync(
										parser,
										&mut grant_duration,
										peek.span,
										|p| Ok(AlterKind::Set(p.parse_sync()?)),
									)?;
								}
							}
							_ => return Err(parser.unexpected(expect)),
						}

						let _ = parser.eat(T![,])?;
						if parser.eat(T![FOR])?.is_none() {
							break;
						}
					}
				}
				T![AUTHENTICATE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut authenticate, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
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
		Ok(ast::AlterAccess {
			if_exists,
			name,
			base,
			authenticate: authenticate.map(|x| x.0),
			grant_duration: grant_duration.map(|x| x.0),
			token_duration: token_duration.map(|x| x.0),
			session_duration: session_duration.map(|x| x.0),
			comment: comment.map(|x| x.0),
			span,
		})
	}
}

macro_rules! impl_method_matching {
	(($parser:expr) => {$($pat:pat => ($store:ident, $new_span:ident)),*}) => {
		$(let mut $new_span = None;)*
		loop{
			let peek = $parser
				.peek_expect("`DELETE`, `GET`, `PATCH`, `POST`, `PUT`, or `TRACE`")?;
			match peek.token {
				$($pat => {
					let _ = $parser.next();
					if let Some(span) = $store.map(|x: (_, Span)| x.1).or($new_span) && !$parser.settings.contains(ParserSettings::QUIRK_REDEFINE) {
						return Err(redefined_error($parser, peek.span, span));
					}
					$new_span = Some(peek.span)
				})*
				_ => {
					return Err($parser.unexpected(
							"`DELETE`, `GET`, `PATCH`, `POST`, `PUT`, or `TRACE`",
					));
				}
			}

			if $parser.eat(T![,])?.is_none(){
				break
			}
		}

		let action = if let Some(x) = $parser.eat(T![DROP])? {
			let _ = $parser.expect(T![THEN])?;
			let span = $parser.span_since(x.span);
			AlterKind::Drop(span)
		} else {
			AlterKind::Set($parser.parse::<ast::NodeId<ast::ApiAction>>().await?)
		};

		$(
			if let Some($new_span) = $new_span{
				$store = Some((action, $new_span));
			}
		)*
	};
}

impl Parse for ast::AlterApi {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![API])?;

		let if_exists = parse_if_exists(parser)?;

		let name = parser.parse_enter().await?;

		let mut comment = None;
		let mut fallback = None;
		let mut get = None;
		let mut delete = None;
		let mut patch = None;
		let mut post = None;
		let mut put = None;
		let mut trace = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![DROP] => {
					let _ = parser.next();
					let _ = parser.expect(T![COMMENT])?;
					let span = parser.span_since(peek.span);
					parse_unordered_clause_sync(parser, &mut comment, span, |_| {
						Ok(AlterKind::Drop(span))
					})?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, async |parser| {
						parser.parse_enter().await.map(AlterKind::Set)
					})
					.await?;
				}
				T![FOR] => {
					let _ = parser.next();
					let expect = "`ANY`, `DELETE`, `GET`, `PATCH`, `PUT`, or `TRACE`";
					let next = parser.peek_expect(expect)?;
					match next.token {
						T![ANY] => {
							let _ = parser.next();
							if let Some(d) = parser.eat(T![DROP])? {
								let _ = parser.expect(T![THEN])?;
								let span = parser.span_since(d.span);
								parse_unordered_clause_sync(parser, &mut fallback, span, |_| {
									Ok(AlterKind::Drop(span))
								})?;
							} else {
								let _ = parser.expect(T![THEN])?;
								parse_unordered_clause(
									parser,
									&mut fallback,
									next.span,
									async |parser| parser.parse_enter().await.map(AlterKind::Set),
								)
								.await?;
							}
						}
						T![DELETE] | T![GET] | T![POST] | T![PUT] | T![PATCH] | T![TRACE] => {
							// macro for some very repetitive code
							// Don't forget to update the expectation strings inside the macro if
							// you ever add new methods.
							//
							// Matches any number of methods, checks if the method was already
							// defined somewhere, if so, throw an error, otherwise parse a
							// ApiAction and set the methods to the parsed action
							impl_method_matching! {
								(parser) => {
									T![DELETE] => (delete,delete_span),
									T![GET] => (get,get_span),
									T![PATCH] => (patch,patch_span),
									T![POST] => (post,post_span),
									T![PUT] => (put,put_span),
									T![TRACE] => (trace,trace_span)
								}
							}
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				_ => break,
			}
		}

		let span = parser.span_since(alter.span);
		Ok(ast::AlterApi {
			if_exists,
			name,
			fallback: fallback.map(|x| x.0),
			methods: ast::AlterMethodApiActions {
				get: get.map(|x| x.0),
				delete: delete.map(|x| x.0),
				patch: patch.map(|x| x.0),
				post: post.map(|x| x.0),
				put: put.map(|x| x.0),
				trace: trace.map(|x| x.0),
			},
			comment: comment.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::AlterConfig {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let alter = parser.expect(T![ALTER])?;
		let _ = parser.expect(T![CONFIG])?;

		let if_exists = parse_if_exists(parser)?;

		let expect = "`API`, `GRAPHQL`, or `DEFAULT`";
		let peek = parser.peek_expect(expect)?;
		let kind = match peek.token {
			T![API] => ast::DefineConfigKind::Api(parser.parse().await?),
			T![GRAPHQL] => ast::DefineConfigKind::Graphql(parser.parse().await?),
			T![DEFAULT] => ast::DefineConfigKind::Default(parser.parse().await?),
			_ => return Err(parser.unexpected(expect)),
		};

		let span = parser.span_since(alter.span);
		Ok(ast::AlterConfig {
			if_exists,
			kind,
			span,
		})
	}
}
