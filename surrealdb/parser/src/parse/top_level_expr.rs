use ast::{AstSpan, KillKind, NodeId, NodeListId, Query, TopLevelExpr, Transaction, UseKind};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, T};

use super::{Parse, ParseResult, ParseSync, Parser};
use crate::parse::ParserState;

fn name_previous_statement(prev: NodeId<TopLevelExpr>, parser: &Parser<'_, '_>) -> &'static str {
	match parser[prev] {
		TopLevelExpr::Transaction(_) => "a transaction",
		TopLevelExpr::Expr(n) => match parser[n] {
			ast::Expr::Path(n) => {
				if parser[n].parts.is_none() {
					"an identifier"
				} else {
					"a path"
				}
			}
			ast::Expr::Create(_)
			| ast::Expr::Update(_)
			| ast::Expr::Upsert(_)
			| ast::Expr::Delete(_)
			| ast::Expr::Relate(_)
			| ast::Expr::Select(_)
			| ast::Expr::DefineNamespace(_)
			| ast::Expr::DefineDatabase(_)
			| ast::Expr::DefineTable(_)
			| ast::Expr::DefineFunction(_)
			| ast::Expr::DefineModule(_)
			| ast::Expr::DefineParam(_)
			| ast::Expr::DefineApi(_)
			| ast::Expr::DefineEvent(_)
			| ast::Expr::DefineField(_)
			| ast::Expr::DefineIndex(_)
			| ast::Expr::DefineAnalyzer(_)
			| ast::Expr::DefineBucket(_)
			| ast::Expr::DefineSequence(_)
			| ast::Expr::DefineConfig(_)
			| ast::Expr::DefineAccess(_)
			| ast::Expr::RemoveNamespace(_)
			| ast::Expr::RemoveDatabase(_)
			| ast::Expr::RemoveTable(_)
			| ast::Expr::RemoveFunction(_)
			| ast::Expr::RemoveModule(_)
			| ast::Expr::RemoveParam(_)
			| ast::Expr::RemoveApi(_)
			| ast::Expr::RemoveEvent(_)
			| ast::Expr::RemoveField(_)
			| ast::Expr::RemoveIndex(_)
			| ast::Expr::RemoveAnalyzer(_)
			| ast::Expr::RemoveBucket(_)
			| ast::Expr::RemoveSequence(_)
			| ast::Expr::RemoveAccess(_)
			| ast::Expr::AlterSystem(_)
			| ast::Expr::AlterNamespace(_)
			| ast::Expr::AlterDatabase(_)
			| ast::Expr::AlterTable(_) => "a statement expression",
			_ => "an expression",
		},
		_ => "a statement",
	}
}

impl Parse for ast::Query {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let span = parser.peek_span();

		let mut exprs = None;
		let mut cur: Option<NodeListId<ast::TopLevelExpr>> = None;
		while let Some(next) = parser.peek()? {
			if let Some(cur) = cur
				&& parser.eat(T![;])?.is_none()
			{
				return Err(parser.with_error(|parser| {
					let last_stmt = parser[cur].cur;
					let last_stmt_span = last_stmt.ast_span(parser);
					let last_stmt_end = Span {
						start: last_stmt_span.end,
						end: last_stmt_span.end,
					};
					let last_stmt_name = name_previous_statement(last_stmt, parser);

					Level::Error
						.title(format!(
							"Unexpected token `{}`, expected `;`",
							parser.slice(next.span)
						))
						.snippet(
							parser
								.snippet()
								.annotate(
									AnnotationKind::Primary
										.span(next.span)
										.label("Maybe missing a semicolon before this token?"),
								)
								.annotate(AnnotationKind::Context.span(last_stmt_span).label(
									format!(
										"This last statement here was parsed as {last_stmt_name}"
									),
								))
								.annotate(
									AnnotationKind::Context
										.span(last_stmt_end)
										.label("Expected a `;` here"),
								),
						)
						.to_diagnostic()
				}));
			}

			// eat all the empty statements.
			while parser.eat(T![;])?.is_some() {}

			if parser.eof() {
				break;
			}

			let expr = parser.parse().await?;
			parser.push_list(expr, &mut exprs, &mut cur);
		}

		let span = parser.span_since(span);

		Ok(Query {
			exprs,
			span,
		})
	}
}

impl Parse for ast::TopLevelExpr {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let next = parser.peek_expect("an expression")?;
		match next.token {
			T![BEGIN] => {
				if parser.state.contains(ParserState::TRANSACTION) {
					return Err(parser.with_error(|parser| {
						Level::Error
							.title(format!("Unexpected token `{}`", parser.slice(next.span)))
							.snippet(parser.snippet().annotate(
								AnnotationKind::Primary.span(next.span).label(
									"You cannot start a second transaction within an existing transaction",
								),
							))
							.to_diagnostic()
					}));
				}

				let tx = parser
					.with_state(
						|state| state | ParserState::TRANSACTION,
						async |parser| parser.parse().await,
					)
					.await?;
				Ok(TopLevelExpr::Transaction(tx))
			}
			T![CANCEL] => Err(parser.with_error(|parser| {
				Level::Error
					.title("Unexpected token `CANCEL` expected an expression")
					.snippet(parser.snippet().annotate(
						AnnotationKind::Primary.span(next.span).label(
							"`CANCEL` statements can only be used within a transaction block",
						),
					))
					.to_diagnostic()
			})),
			T![COMMIT] => Err(parser.with_error(|parser| {
				Level::Error
					.title("Unexpected token `COMMIT` expected an expression")
					.snippet(parser.snippet().annotate(
						AnnotationKind::Primary.span(next.span).label(
							"`COMMIT` statements can only be used within a transaction block",
						),
					))
					.to_diagnostic()
			})),
			T![USE] => Ok(TopLevelExpr::Use(parser.parse_sync()?)),
			T![OPTION] => Ok(TopLevelExpr::Option(parser.parse_sync()?)),
			T![KILL] => Ok(TopLevelExpr::Kill(parser.parse_sync()?)),
			T![SHOW] => Ok(TopLevelExpr::Show(parser.parse().await?)),
			_ => Ok(TopLevelExpr::Expr(parser.parse().await?)),
		}
	}
}

impl Parse for ast::Transaction {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		debug_assert!(parser.state.contains(ParserState::TRANSACTION));

		let start = parser.expect(T![BEGIN])?.span;
		let _ = parser.eat(T![TRANSACTION])?;

		let mut head = None;
		let mut tail = None;
		let (end, commits) = loop {
			if parser.eat(T![;])?.is_none() {
				let span = parser.peek_span();
				return Err(parser.with_error(|parser| {
					Level::Error
						.title(format!(
							"Unexpected token `{}`, expected `;` as the transaction block has not yet ended",
							parser.slice(span)
						))
						.snippet(
							parser.snippet().annotate(AnnotationKind::Primary.span(span)).annotate(
								AnnotationKind::Context
									.span(start)
									.label("This transaction is still open"),
							),
						)
						.to_diagnostic()
				}));
			}

			let Some(next) = parser.peek()? else {
				let span = parser.peek_span();
				return Err(parser.with_error(|parser| {
					Level::Error
						.title("Unexpected end of query, expected transaction block to end")
						.snippet(
							parser.snippet().annotate(AnnotationKind::Primary.span(span)).annotate(
								AnnotationKind::Context
									.span(start)
									.label("Expected this transaction to end"),
							),
						)
						.to_diagnostic()
				}));
			};

			match next.token {
				T![CANCEL] => {
					parser.next()?;
					break (next.span, false);
				}
				T![COMMIT] => {
					parser.next()?;
					break (next.span, true);
				}
				T![BEGIN] => {
					return Err(parser.with_error(|parser| {
						Level::Error
							.title(
								"Unexpected token `BEGIN`, cannot start a transaction within another transaction",
							)
							.snippet(
								parser
									.snippet()
									.annotate(AnnotationKind::Primary.span(next.span))
									.annotate(
										AnnotationKind::Context
											.span(start)
											.label("Expected this transaction to end"),
									),
							)
							.to_diagnostic()
					}));
				}
				_ => {
					let node = parser.parse_enter::<TopLevelExpr>().await?;
					parser.push_list(node, &mut head, &mut tail);
				}
			}
		};

		Ok(Transaction {
			span: start.extend(end),
			statements: head,
			commits,
		})
	}
}

impl ParseSync for ast::Kill {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = parser.expect(T![KILL])?.span;

		let peek = parser.peek_expect("a UUID or a parameter")?;
		let kind = match peek.token {
			BaseTokenKind::UuidString => KillKind::Uuid(parser.parse_sync()?),
			BaseTokenKind::Param => KillKind::Param(parser.parse_sync()?),
			_ => return Err(parser.unexpected("a UUID or a parameter")),
		};
		Ok(ast::Kill {
			kind,
			span: parser.span_since(start),
		})
	}
}

impl ParseSync for ast::Use {
	fn parse_sync(parser: &mut Parser) -> super::ParseResult<Self> {
		let start = parser.expect(T![USE])?.span;

		let (kind, span) = if parser.eat(T![NAMESPACE])?.is_some() {
			let ns = parser.parse_sync()?;
			if parser.eat(T![DATABASE])?.is_some() {
				let db = parser.parse_sync()?;
				(
					UseKind::NamespaceDatabase {
						namespace: ns,
						database: db,
					},
					start.extend(parser[db].span),
				)
			} else {
				(UseKind::Namespace(ns), start.extend(parser[ns].span))
			}
		} else if parser.eat(T![DATABASE])?.is_some() {
			let db = parser.parse_sync()?;

			(UseKind::Database(db), start.extend(parser[db].span))
		} else {
			return Err(parser.unexpected("either `NAMESPACE` or `DATABASE`"));
		};

		Ok(ast::Use {
			kind,
			span,
		})
	}
}

impl ParseSync for ast::OptionStmt {
	fn parse_sync(parser: &mut Parser) -> super::ParseResult<Self> {
		let start = parser.expect(T![OPTION])?.span;

		let name = parser.parse_sync()?;
		let value = if parser.eat(T![=])?.is_some() {
			let value_token = parser.peek_expect("either `true` or `false`")?;
			let value = match value_token.token {
				T![true] => true,
				T![false] => false,
				_ => return Err(parser.unexpected("either `true` or `false`")),
			};
			let _ = parser.next();
			value
		} else {
			true
		};

		let span = parser.span_since(start);

		Ok(ast::OptionStmt {
			name,
			value,
			span,
		})
	}
}

impl Parse for ast::Show {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![SHOW])?;

		let _ = parser.expect(T![CHANGES])?;
		let _ = parser.expect(T![FOR])?;

		let peek = parser.peek_expect("keyword `TABLE` or `DATABASE`")?;
		let target = match peek.token {
			T![TABLE] => {
				let _ = parser.next();
				ast::ShowTarget::Table(parser.parse_sync()?)
			}
			T![DATABASE] => {
				let _ = parser.next();
				ast::ShowTarget::Database(peek.span)
			}
			_ => return Err(parser.unexpected("keyword `TABLE` or `DATABASE`")),
		};

		let _ = parser.expect(T![SINCE])?;

		let peek = parser.peek_expect("a datetime or integer")?;
		let since = match peek.token {
			BaseTokenKind::DateTimeString => ast::ShowSince::Timestamp(parser.parse_sync()?),
			BaseTokenKind::Int => ast::ShowSince::VersionStamp(parser.parse_sync()?),
			_ => return Err(parser.unexpected("a datetime or integer")),
		};

		let limit = if parser.eat(T![LIMIT])?.is_some() {
			Some(parser.parse_enter().await?)
		} else {
			None
		};

		let span = parser.span_since(start.span);
		Ok(ast::Show {
			target,
			since,
			limit,
			span,
		})
	}
}
