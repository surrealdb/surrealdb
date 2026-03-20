mod alter;
mod define;
mod remove;

use ast::{
	AstSpan, Expr, InsertData, InsertInto, InsertTuples, NodeId, OrderBy, RecordData, WithIndex,
};
use common::source_error::{AnnotationKind, Level};
use token::{BaseTokenKind, T};

use super::Parser;
use crate::Parse;
use crate::parse::utils::{parse_delimited_list, parse_seperated_list, parse_seperated_list_sync};
use crate::parse::{ParseError, ParseResult};

impl Parse for ast::If {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![IF])?;
		let condition = parser.parse_enter().await?;

		if parser.eat(T![THEN])?.is_some() {
			let then = parser.parse_enter().await?;
			parser.eat(T![;])?;

			let otherwise = if parser.eat(T![ELSE])?.is_some() {
				let peek = parser.peek_expect("a `else` body")?;
				if let T![IF] = peek.token {
					let otherwise = parser.parse_enter().await?;
					Some(parser.push(Expr::If(otherwise)))
				} else {
					let res = parser.parse_enter().await?;
					parser.eat(T![;])?;
					let _ = parser.expect(T![END])?;
					Some(res)
				}
			} else {
				let _ = parser.expect(T![END])?;
				None
			};

			let span = parser.span_since(start.span);
			Ok(ast::If {
				condition,
				then,
				otherwise,
				span,
			})
		} else {
			let then = parser.parse().await?;
			let then = parser.push(Expr::Block(then));

			let otherwise = if parser.eat(T![ELSE])?.is_some() {
				let peek = parser.peek_expect("a `else` body")?;
				if let T![IF] = peek.token {
					let otherwise = parser.parse_enter().await?;
					Some(parser.push(Expr::If(otherwise)))
				} else {
					let block = parser.parse().await?;
					Some(parser.push(Expr::Block(block)))
				}
			} else {
				None
			};

			let span = parser.span_since(start.span);
			Ok(ast::If {
				condition,
				then,
				otherwise,
				span,
			})
		}
	}
}

impl Parse for ast::For {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![FOR])?;

		let param = parser.parse_sync()?;
		let _ = parser.expect(T![IN])?;
		let expr = parser.parse_enter().await?;

		let body = parser.parse().await?;

		let span = parser.span_since(start.span);
		Ok(ast::For {
			param,
			range: expr,
			body,
			span,
		})
	}
}

impl Parse for ast::Let {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![LET])?;

		let param = parser.parse_sync()?;

		let ty = if parser.eat(T![:])?.is_some() {
			Some(parser.parse().await?)
		} else {
			None
		};

		let _ = parser.expect(T![=])?;

		let expr = parser.parse_enter().await?;
		let span = parser.span_since(start.span);

		Ok(ast::Let {
			param,
			ty,
			expr,
			span,
		})
	}
}

impl Parse for ast::Return {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![RETURN])?;

		let expr = parser.parse_enter().await?;

		let fetch = if parser.eat(T![FETCH])?.is_some() {
			Some(
				parse_seperated_list(parser, T![,], async |parser| {
					// Check for param or identifier,
					// This check could be removed but keeping it in place can allow us to
					// constrain the allowed syntax in the future without cause backwards
					// incompatible changes
					if let Some(t) = parser.peek()?
						&& (t.token.is_identifier() || t.token == BaseTokenKind::Param)
					{
						parser.parse_enter().await
					} else {
						Err(parser.unexpected("a fetch expression"))
					}
				})
				.await?
				.1,
			)
		} else {
			None
		};

		let span = parser.span_since(start.span);
		Ok(ast::Return {
			expr,
			fetch,
			span,
		})
	}
}

impl Parse for ast::Info {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![INFO])?;
		let _ = parser.expect(T![FOR])?;

		let peek = parser.peek_expect("a resource to get information for")?;
		let kind = match peek.token {
			T![ROOT] => {
				let _ = parser.next();
				ast::InfoKind::Root
			}
			T![NAMESPACE] => {
				let _ = parser.next();
				ast::InfoKind::Namespace
			}
			T![DATABASE] => {
				let _ = parser.next();
				let version = if parser.eat(T![VERSION])?.is_some() {
					Some(parser.parse_enter().await?)
				} else {
					None
				};
				ast::InfoKind::Database {
					version,
				}
			}
			T![TABLE] => {
				let _ = parser.next();
				let name = parser.parse_enter().await?;
				let version = if parser.eat(T![VERSION])?.is_some() {
					Some(parser.parse_enter().await?)
				} else {
					None
				};
				ast::InfoKind::Table {
					name,
					version,
				}
			}
			T![USER] => {
				let _ = parser.next();
				let name = parser.parse_enter().await?;

				let base = if parser.eat(T![ON])?.is_some() {
					Some(parser.parse_sync()?)
				} else {
					None
				};
				ast::InfoKind::User {
					name,
					base,
				}
			}
			T![INDEX] => {
				let _ = parser.next();
				let name = parser.parse_enter().await?;
				let _ = parser.expect(T![ON])?;
				parser.eat(T![TABLE])?;
				let table = parser.parse_enter().await?;
				ast::InfoKind::Index {
					name,
					table,
				}
			}
			_ => return Err(parser.unexpected("")),
		};

		let structure = parser.eat(T![STRUCTURE])?.is_some();

		Ok(ast::Info {
			kind,

			span: parser.span_since(start.span),
			structure,
		})
	}
}

async fn try_parse_clause_expr(
	parser: &mut Parser<'_, '_>,
	token: BaseTokenKind,
) -> ParseResult<Option<NodeId<ast::Expr>>> {
	if parser.eat(token)?.is_some() {
		Ok(Some(parser.parse_enter().await?))
	} else {
		Ok(None)
	}
}

async fn try_parse_with(parser: &mut Parser<'_, '_>) -> ParseResult<Option<WithIndex>> {
	if parser.eat(T![WITH])?.is_some() {
		let peek = parser.peek_expect("`INDEX` `NO`, or `NOINDEX`")?;
		let idx = match peek.token {
			T![NO] => {
				let _ = parser.next();
				let _ = parser.expect(T![INDEX])?;
				ast::WithIndex::None(peek.span)
			}
			T![NOINDEX] => {
				let _ = parser.next();
				ast::WithIndex::None(peek.span)
			}
			T![INDEX] => {
				let _ = parser.next();
				let (_, indecies) =
					parse_seperated_list_sync(parser, T![,], |parser| parser.parse_sync())?;
				ast::WithIndex::Some(indecies)
			}
			_ => return Err(parser.unexpected("`INDEX` or `NOINDEX`")),
		};
		Ok(Some(idx))
	} else {
		Ok(None)
	}
}

async fn try_parse_explain(parser: &mut Parser<'_, '_>) -> ParseResult<Option<ast::ExplainClause>> {
	let res = if let Some(explain) = parser.eat(T![EXPLAIN])? {
		if let Some(full) = parser.eat(T![FULL])? {
			Some(ast::ExplainClause::Full(explain.span.extend(full.span)))
		} else {
			Some(ast::ExplainClause::Base(explain.span))
		}
	} else {
		None
	};
	Ok(res)
}

async fn try_parse_record_data(parser: &mut Parser<'_, '_>) -> ParseResult<Option<RecordData>> {
	let peek = parser.peek()?;
	let data = match peek.map(|x| x.token) {
		Some(T![SET]) => {
			let _ = parser.next();
			let (_, list) =
				parse_seperated_list(parser, T![,], async |parser| parser.parse().await).await?;
			Some(ast::RecordData::Set(list))
		}
		Some(T![UNSET]) => {
			let _ = parser.next();
			let (_, list) =
				parse_seperated_list(parser, T![,], async |parser| parser.parse().await).await?;
			Some(ast::RecordData::Unset(list))
		}
		Some(T![CONTENT]) => {
			let _ = parser.next();
			Some(ast::RecordData::Content(parser.parse_enter().await?))
		}
		Some(T![PATCH]) => {
			let _ = parser.next();
			Some(ast::RecordData::Patch(parser.parse_enter().await?))
		}
		Some(T![MERGE]) => {
			let _ = parser.next();
			Some(ast::RecordData::Merge(parser.parse_enter().await?))
		}
		Some(T![REPLACE]) => {
			let _ = parser.next();
			Some(ast::RecordData::Replace(parser.parse_enter().await?))
		}
		_ => None,
	};
	Ok(data)
}

impl Parse for ast::Selector {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let span = parser.peek_span();
		let expr = parser.parse_enter().await?;
		let alias = if parser.eat(T![AS])?.is_some() {
			Some(parser.parse().await?)
		} else {
			None
		};
		let span = parser.span_since(span);
		Ok(ast::Selector {
			expr,
			alias,
			span,
		})
	}
}

impl Parse for ast::ListSelector {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		if let Some(x) = parser.eat(T![*])? {
			Ok(ast::ListSelector::All(x.span))
		} else {
			Ok(ast::ListSelector::Selector(parser.parse().await?))
		}
	}
}

impl Parse for ast::Fields {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		if parser.eat(T![VALUE])?.is_some() {
			let selector = parser.parse().await?;
			Ok(ast::Fields::Value(selector))
		} else {
			let (_, list) =
				parse_seperated_list(parser, T![,], async |parser| parser.parse().await).await?;
			Ok(ast::Fields::List(list))
		}
	}
}

impl Parse for ast::Output {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let _ = parser.expect(T![RETURN])?;
		let peek =
			parser.peek_expect("`NONE`, `NULL`, `DIFF`, `AFTER`, `BEFORE`, or an expression")?;
		let res = match peek.token {
			T![NONE] => {
				let _ = parser.next();
				ast::Output::None(peek.span)
			}
			T![NULL] => {
				let _ = parser.next();
				ast::Output::Null(peek.span)
			}
			T![DIFF] => {
				let _ = parser.next();
				ast::Output::Diff(peek.span)
			}
			T![AFTER] => {
				let _ = parser.next();
				ast::Output::After(peek.span)
			}
			T![BEFORE] => {
				let _ = parser.next();
				ast::Output::Before(peek.span)
			}
			_ => {
				let fields = parser.parse().await?;
				ast::Output::Fields(fields)
			}
		};
		Ok(res)
	}
}

impl Parse for ast::Delete {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![DELETE])?;
		let _ = parser.eat(T![FROM])?;
		let only = parser.eat(T![ONLY])?.is_some();
		let (_, targets) =
			parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await).await?;

		let with_index = try_parse_with(parser).await?;

		let condition = try_parse_clause_expr(parser, T![WHERE]).await?;
		let output = if let Some(x) = parser.peek()?
			&& let T![RETURN] = x.token
		{
			Some(parser.parse().await?)
		} else {
			None
		};

		let timeout = try_parse_clause_expr(parser, T![TIMEOUT]).await?;

		let explain = try_parse_explain(parser).await?;

		let span = parser.span_since(start.span);
		Ok(ast::Delete {
			only,
			targets,
			with_index,
			condition,
			output,
			timeout,
			explain,
			span,
		})
	}
}

impl Parse for ast::Assignment {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();
		let place = parser.parse().await?;

		let peek = parser.peek_expect("`=`, `+=`, `-=`, or `+?=`")?;
		let op = match peek.token {
			T![=] => ast::AssignmentOp::Assign(peek.span),
			T![+=] => ast::AssignmentOp::Add(peek.span),
			T![-=] => ast::AssignmentOp::Subtract(peek.span),
			T![+?=] => ast::AssignmentOp::Extend(peek.span),
			_ => return Err(parser.unexpected("`=`, `+=`, `-=`, or `+?=`")),
		};
		let _ = parser.next();

		let value = parser.parse_enter().await?;
		let span = parser.span_since(start);

		Ok(ast::Assignment {
			place,
			op,
			value,
			span,
		})
	}
}

impl Parse for ast::Create {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![CREATE])?;
		let only = parser.eat(T![ONLY])?.is_some();
		let (_, targets) =
			parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await).await?;

		let data = try_parse_record_data(parser).await?;

		let output = if let Some(x) = parser.peek()?
			&& let T![RETURN] = x.token
		{
			Some(parser.parse().await?)
		} else {
			None
		};
		let version = try_parse_clause_expr(parser, T![VERSION]).await?;
		let timeout = try_parse_clause_expr(parser, T![TIMEOUT]).await?;

		let span = parser.span_since(start.span);
		Ok(ast::Create {
			only,
			targets,
			data,
			output,
			version,
			timeout,
			span,
		})
	}
}

impl Parse for ast::Update {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![UPDATE])?;
		let only = parser.eat(T![ONLY])?.is_some();
		let (_, targets) =
			parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await).await?;

		let with_index = try_parse_with(parser).await?;

		let data = try_parse_record_data(parser).await?;

		let condition = try_parse_clause_expr(parser, T![WHERE]).await?;

		let output = if let Some(x) = parser.peek()?
			&& let T![RETURN] = x.token
		{
			Some(parser.parse().await?)
		} else {
			None
		};

		let timeout = try_parse_clause_expr(parser, T![TIMEOUT]).await?;

		let explain = try_parse_explain(parser).await?;

		let span = parser.span_since(start.span);
		Ok(ast::Update {
			only,
			targets,
			with_index,
			data,
			condition,
			output,
			timeout,
			explain,
			span,
		})
	}
}

impl Parse for ast::Upsert {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![UPSERT])?;
		let only = parser.eat(T![ONLY])?.is_some();
		let (_, targets) =
			parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await).await?;

		let with_index = try_parse_with(parser).await?;

		let data = try_parse_record_data(parser).await?;

		let condition = try_parse_clause_expr(parser, T![WHERE]).await?;

		let output = if let Some(x) = parser.peek()?
			&& let T![RETURN] = x.token
		{
			Some(parser.parse().await?)
		} else {
			None
		};

		let timeout = try_parse_clause_expr(parser, T![TIMEOUT]).await?;

		let explain = try_parse_explain(parser).await?;

		let span = parser.span_since(start.span);
		Ok(ast::Upsert {
			only,
			targets,
			with_index,
			data,
			condition,
			output,
			timeout,
			explain,
			span,
		})
	}
}

async fn parse_relate_expr(parser: &mut Parser<'_, '_>) -> ParseResult<NodeId<Expr>> {
	let peek = parser.peek_expect("an expression")?;
	match peek.token {
		BaseTokenKind::Param => {
			let expr = parser.parse_sync().map(Expr::Param)?;
			Ok(parser.push(expr))
		}
		BaseTokenKind::OpenBracket => {
			let expr = parser.parse().await.map(Expr::Array)?;
			Ok(parser.push(expr))
		}
		BaseTokenKind::OpenParen => parser.parse_enter().await,
		_ => {
			let expr = parser.parse().await.map(Expr::RecordId)?;
			Ok(parser.push(expr))
		}
	}
}

impl Parse for ast::Relate {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![RELATE])?;
		let only = parser.eat(T![ONLY])?.is_some();

		let first = parse_relate_expr(parser).await?;

		let peek = parser.peek_expect("`->` or `<-`")?;
		let rightward = match peek.token {
			T![->] => {
				let _ = parser.next();
				true
			}
			T![<] => {
				if let Some(peek1) = parser.peek_joined1()?
					&& let T![-] = peek1.token
				{
					let _ = parser.next();
					let _ = parser.next();
					false
				} else {
					return Err(parser.unexpected("`->` or `<-`"));
				}
			}
			_ => return Err(parser.unexpected("`->` or `<-`")),
		};

		let peek =
			parser.peek_expect("a parameter, identifier, record-id or covered expression")?;
		let through = match peek.token {
			BaseTokenKind::Param => {
				let param = parser.parse_sync()?;
				parser.push(Expr::Param(param))
			}
			BaseTokenKind::OpenParen => {
				let _ = parser.next();
				let expr = parser.parse_enter().await?;
				let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;
				expr
			}
			x if x.is_identifier() => {
				if let Some(peek1) = parser.peek1()?
					&& let T![:] = peek1.token
				{
					let id = parser.parse().await?;
					parser.push(Expr::RecordId(id))
				} else {
					let ident = parser.parse_sync::<NodeId<_>>()?;
					let span = ident.ast_span(parser);
					let path = parser.push(ast::Path {
						start: ident,
						parts: None,
						span,
					});
					parser.push(Expr::Path(path))
				}
			}
			_ => {
				return Err(
					parser.unexpected("a parameter, identifier, record-id or covered expression")
				);
			}
		};

		if rightward {
			let _ = parser.expect(T![->])?;
		} else if let T![<] = parser.peek_expect("`<-`")?.token
			&& let Some(peek1) = parser.peek_joined1()?
			&& let T![-] = peek1.token
		{
			let _ = parser.next();
			let _ = parser.next();
		} else {
			return Err(parser.unexpected("`<-`"));
		}

		let last = parser.parse_enter().await?;

		let (from, to) = if rightward {
			(first, last)
		} else {
			(last, first)
		};

		let data = try_parse_record_data(parser).await?;

		// Was in the previous parser for backwards compatiblity
		// TODO (4.0): Remove
		let _ = parser.eat(T![UNIQUE])?;
		let output = if let Some(x) = parser.peek()?
			&& let T![RETURN] = x.token
		{
			Some(parser.parse().await?)
		} else {
			None
		};
		let timeout = try_parse_clause_expr(parser, T![TIMEOUT]).await?;

		let span = parser.span_since(start.span);
		Ok(ast::Relate {
			only,
			from,
			through,
			to,
			data,
			output,
			timeout,
			span,
		})
	}
}

impl Parse for ast::Order {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let span = parser.peek_span();
		let expr = parser.parse_enter().await?;

		let collate = parser.eat(T![COLLATE])?.is_some();
		let numeric = parser.eat(T![NUMERIC])?.is_some();

		let peek = parser.peek()?;
		let direction = match peek.map(|x| x.token) {
			Some(T![ASCENDING]) => {
				let _ = parser.next();
				Some(ast::OrderDirection::Ascending)
			}
			Some(T![DESCENDING]) => {
				let _ = parser.next();
				Some(ast::OrderDirection::Descending)
			}
			_ => None,
		};

		let span = parser.span_since(span);
		Ok(ast::Order {
			expr,
			collate,
			numeric,
			direction,
			span,
		})
	}
}

impl Parse for ast::OrderBy {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let _ = parser.expect(T![ORDER])?;
		let _ = parser.expect(T![BY])?;

		let peek = parser.peek_expect("an expression")?;
		if let T![RAND] = peek.token
			&& let Some(peek1) = parser.peek1()?
			&& let BaseTokenKind::OpenParen = peek1.token
		{
			let _ = parser.next();
			let _ = parser.next();
			let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek1.span);
			let span = parser.span_since(peek.span);
			return Ok(OrderBy::Rand(span));
		}

		let (_, v) = parse_seperated_list(parser, T![,], Parser::parse).await?;
		Ok(OrderBy::List(v))
	}
}

impl Parse for ast::Select {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let select = parser.expect(T![SELECT])?;
		let fields = parser.parse().await?;

		let omit = if parser.eat(T![OMIT])?.is_some() {
			Some(parse_seperated_list(parser, T![,], Parser::parse_enter).await?.1)
		} else {
			None
		};

		let _ = parser.expect(T![FROM])?;

		let only = parser.eat(T![ONLY])?.is_some();

		let (_, from) =
			parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await).await?;

		let with_index = try_parse_with(parser).await?;
		let condition = try_parse_clause_expr(parser, T![WHERE]).await?;

		let split_span = parser.peek_span();
		let split = if parser.eat(T![SPLIT])?.is_some() {
			let _ = parser.eat(T![ON])?;
			let (_, splits) =
				parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await)
					.await?;
			Some(splits)
		} else {
			None
		};
		let split_span = parser.span_since(split_span);

		let group = if let Some(x) = parser.eat(T![GROUP])? {
			if parser.eat(T![ALL])?.is_some() {
				Some(ast::Group::All)
			} else {
				if split.is_some() {
					return Err(parser.with_error(|parser|{
					Level::Error.title(format!("Unexpected token `{}`, selects cannot both have a `GROUP BY` clause and a `SPLIT ON` clause",parser.slice(x.span))).snippet(parser.snippet().annotate(AnnotationKind::Primary.span(x.span)).annotate(AnnotationKind::Context.span(split_span).label("Previous `SPLIT ON` clause"))).to_diagnostic()
				}));
				}

				let _ = parser.eat(T![BY])?;
				let (_, groups) =
					parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await)
						.await?;
				Some(ast::Group::Fields(groups))
			}
		} else {
			None
		};

		let order = if let Some(x) = parser.peek()?
			&& let T![ORDER] = x.token
		{
			Some(parser.parse().await?)
		} else {
			None
		};

		let (start, limit) = if let Some(peek) = parser.peek()?
			&& let T![START] = peek.token
		{
			let _ = parser.next()?;
			let _ = parser.eat(T![AT])?;
			let start = Some(parser.parse_enter().await?);

			let limit = if parser.eat(T![LIMIT])?.is_some() {
				let _ = parser.eat(T![BY])?;
				Some(parser.parse_enter().await?)
			} else {
				None
			};
			(start, limit)
		} else {
			let limit = if parser.eat(T![LIMIT])?.is_some() {
				let _ = parser.eat(T![BY])?;
				Some(parser.parse_enter().await?)
			} else {
				None
			};

			let start = if parser.eat(T![START])?.is_some() {
				let _ = parser.eat(T![AT])?;
				Some(parser.parse_enter().await?)
			} else {
				None
			};
			(start, limit)
		};

		let fetch = if parser.eat(T![FETCH])?.is_some() {
			Some(
				parse_seperated_list(parser, T![,], async |parser| {
					// Check for param or identifier,
					// This check could be removed but keeping it in place can allow us to
					// constrain the allowed syntax in the future without cause backwards
					// incompatible changes
					if let Some(t) = parser.peek()?
						&& (t.token.is_identifier() || t.token == BaseTokenKind::Param)
					{
						parser.parse_enter().await
					} else {
						Err(parser.unexpected("a fetch expression"))
					}
				})
				.await?
				.1,
			)
		} else {
			None
		};

		let version = try_parse_clause_expr(parser, T![VERSION]).await?;
		let timeout = try_parse_clause_expr(parser, T![TIMEOUT]).await?;

		let tempfiles = parser.eat(T![TEMPFILES])?.is_some();

		let explain = try_parse_explain(parser).await?;

		let span = parser.span_since(select.span);
		Ok(ast::Select {
			fields,
			omit,
			only,
			from,
			with_index,
			condition,

			split,
			group,

			order,

			start,
			limit,

			fetch,

			version,
			timeout,

			tempfiles,

			explain,

			span,
		})
	}
}

async fn parse_insert_data(parser: &mut Parser<'_, '_>) -> ParseResult<InsertData> {
	// Did not start with a `(` so it cannot be the tuples
	if let Some(peek) = parser.peek()?
		&& !matches!(peek.token, BaseTokenKind::OpenParen)
	{
		return Ok(InsertData::Expr(parser.parse_enter().await?));
	}

	// We need to do something a bit hacky here.
	// Because we are speculating we might push a bunch of values which will then not be used.
	// To ensure that we cannot overflow the u32::MAX integer of nodes we need to remove these
	// values when recovering from speculation.
	// TODO: Implement a more proper checkpointing system for the library.
	let place_len = parser.ast.library().place.len();
	let places_len = parser.ast.library().places.len();
	let ident_len = parser.ast.library().ident.len();

	let speculate = parser
		.speculate(async |parser| {
			let mut places_len = 0;
			let (places_span, places) = parse_delimited_list(
				parser,
				BaseTokenKind::OpenParen,
				BaseTokenKind::CloseParen,
				T![,],
				async |parser| {
					let res = parser.parse().await;
					places_len += 1;
					res
				},
			)
			.await?;

			let Some(places) = places else {
				return Err(ParseError::speculate());
			};

			let _ = parser.expect(T![VALUES])?;

			// After parsing the VALUES token we can be sure this has to be tuples
			parser
				.commit(async |parser| {
					let (_, values) = parse_seperated_list(parser, T![,], async |parser| {
						let mut expr_len = 0;
						let (exprs_span, exprs) = parse_delimited_list(
							parser,
							BaseTokenKind::OpenParen,
							BaseTokenKind::CloseParen,
							T![,],
							async |parser| {
								let res = parser.parse_enter::<Expr>().await?;
								expr_len += 1;
								Ok(res)
							},
						)
						.await?;


						if let Some(values) = exprs && places_len == expr_len {
							Ok(values)
						}else{
							Err(parser.with_error(|parser|{
								Level::Error.title(format!("Invalid number of inser values, found {expr_len} value(s) but field tuple has {places_len} value(s)"))
									.snippet(parser.snippet()
										.annotate(AnnotationKind::Primary.span(exprs_span))
										.annotate(AnnotationKind::Context.span(places_span)
											.label(format!("This fields tuple has {places_len} entries")))
									)
									.to_diagnostic()
							}))
						}
					})
					.await?;

					let span = parser.span_since(places_span);
					Ok(InsertTuples{
						places,
						values,
						span,
					})
				})
				.await
		})
		.await?;

	if let Some(res) = speculate {
		return Ok(InsertData::Tuples(res));
	}

	parser.ast.library_mut().place.truncate(place_len);
	parser.ast.library_mut().places.truncate(places_len);
	parser.ast.library_mut().ident.truncate(ident_len);

	Ok(InsertData::Expr(parser.parse_enter().await?))
}

impl Parse for ast::Insert {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![INSERT])?;

		let relation = parser.eat(T![RELATION])?.is_some();
		let ignore = parser.eat(T![IGNORE])?.is_some();

		let into = if parser.eat(T![INTO])?.is_some() {
			let expect = "a parameter or an identifier";
			let peek = parser.peek_expect(expect)?;
			let into = match peek.token {
				BaseTokenKind::Param => InsertInto::Param(parser.parse_sync()?),
				x if x.is_identifier() => InsertInto::Table(parser.parse_sync()?),
				_ => return Err(parser.unexpected(expect)),
			};
			Some(into)
		} else {
			None
		};

		let data = parse_insert_data(parser).await?;

		let on_duplicate = if parser.eat(T![ON])?.is_some() {
			let _ = parser.expect(T![DUPLICATE])?;
			let _ = parser.expect(T![KEY])?;
			let _ = parser.expect(T![UPDATE])?;
			let (_, assignments) = parse_seperated_list(parser, T![,], Parser::parse).await?;
			Some(assignments)
		} else {
			None
		};

		let output = if let Some(x) = parser.peek()?
			&& let T![RETURN] = x.token
		{
			Some(parser.parse().await?)
		} else {
			None
		};

		let version = try_parse_clause_expr(parser, T![VERSION]).await?;
		let timeout = try_parse_clause_expr(parser, T![TIMEOUT]).await?;

		let span = parser.span_since(start.span);
		Ok(ast::Insert {
			relation,
			ignore,
			into,
			span,
			on_duplicate,
			output,
			version,
			timeout,
			data,
		})
	}
}

impl Parse for ast::Rebuild {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![REBUILD])?;
		let _ = parser.expect(T![INDEX])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let concurrently = parser.eat(T![CONCURRENTLY])?.is_some();

		let span = parser.span_since(start.span);
		Ok(ast::Rebuild {
			if_exists,
			name,
			table,
			concurrently,
			span,
		})
	}
}

impl Parse for ast::Access {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![ACCESS])?;

		let access = parser.parse_sync()?;
		let base = if parser.eat(T![ON])?.is_some() {
			Some(parser.parse_sync()?)
		} else {
			None
		};

		let expect = "`GRANT`, `SHOW`, `REVOKE`, and `PURGE`";
		let peek = parser.peek_expect(expect)?;
		let kind = match peek.token {
			T![GRANT] => {
				let _ = parser.next();
				let _ = parser.expect(T![FOR])?;
				let expect = "`USER` or `RECORD`";
				let subject = parser.peek_expect(expect)?;
				match subject.token {
					T![USER] => {
						let _ = parser.next();
						let subject = ast::AccessSubject::User(parser.parse_sync()?);
						let span = parser.span_since(peek.span);
						ast::AccessKind::Grant(ast::AccessGrant {
							subject,
							span,
						})
					}
					T![RECORD] => {
						let _ = parser.next();
						let subject = ast::AccessSubject::Subject(parser.parse().await?);
						let span = parser.span_since(peek.span);
						ast::AccessKind::Grant(ast::AccessGrant {
							subject,
							span,
						})
					}
					_ => return Err(parser.unexpected(expect)),
				}
			}
			T![SHOW] => {
				let _ = parser.next();
				let expect = "`ALL`, `GRANT`, or `WHERE`";
				let which = parser.peek_expect(expect)?;
				let kind = match which.token {
					T![ALL] => {
						let _ = parser.next();
						ast::AccessShowKind::All
					}
					T![GRANT] => {
						let _ = parser.next();
						ast::AccessShowKind::Grant(parser.parse_sync()?)
					}
					T![WHERE] => {
						let _ = parser.next();
						ast::AccessShowKind::Condition(parser.parse_enter().await?)
					}
					_ => return Err(parser.unexpected(expect)),
				};

				let span = parser.span_since(peek.span);
				ast::AccessKind::Show(ast::AccessShow {
					kind,
					span,
				})
			}
			_ => return Err(parser.unexpected(expect)),
		};

		let span = parser.span_since(start.span);
		Ok(ast::Access {
			access,
			base,
			span,
			kind,
		})
	}
}

impl Parse for ast::Explain {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![EXPLAIN])?;

		let analyze = parser.eat(T![ANALYZE])?.is_some();

		let format = if parser.eat(T![FORMAT])?.is_some() {
			let expect = "`JSON` or `TEXT`";
			let peek = parser.peek_expect(expect)?;
			let res = match peek.token {
				T![JSON] => {
					let _ = parser.next();
					ast::ExplainFormat::Json
				}
				T![TEXT] => {
					let _ = parser.next();
					ast::ExplainFormat::Text
				}
				_ => return Err(parser.unexpected(expect)),
			};
			Some(res)
		} else {
			None
		};

		let expr = parser.parse_enter().await?;

		let span = parser.span_since(start.span);
		Ok(ast::Explain {
			analyze,
			format,
			expr,
			span,
		})
	}
}
