use ast::{KillKind, Query, TopLevelExpr, Transaction, UseKind};
use common::source_error::{AnnotationKind, Level};
use token::{BaseTokenKind, T};

use super::{Parse, ParseResult, ParseSync, Parser};
use crate::parse::ParserState;

impl Parse for ast::Query {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let span = parser.peek_span();

		let mut exprs = None;
		let mut cur = None;
		while let Some(next) = parser.peek()? {
			if exprs.is_some() {
				if parser.eat(T![;])?.is_none() {
					return Err(parser.with_error(|parser| {
						Level::Error
							.title(format!(
								"Unexpected token `{}`, expected `;`",
								parser.slice(next.span)
							))
							.snippet(
								parser.snippet().annotate(
									AnnotationKind::Primary
										.span(span)
										.label("Maybe missing a semicolon on the last statement?"),
								),
							)
							.to_diagnostic()
					}));
				}
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
						async |parser| parser.parse_push().await,
					)
					.await?;
				Ok(TopLevelExpr::Transaction(tx))
			}
			T![CANCEL] => {
				return Err(parser.with_error(|parser| {
					Level::Error
						.title("Unexpected token `CANCEL` expected an expression")
						.snippet(parser.snippet().annotate(
							AnnotationKind::Primary.span(next.span).label(
								"`CANCEL` statements can only be used within a transaction block",
							),
						))
						.to_diagnostic()
				}));
			}
			T![COMMIT] => {
				return Err(parser.with_error(|parser| {
					Level::Error
						.title("Unexpected token `COMMIT` expected an expression")
						.snippet(parser.snippet().annotate(
							AnnotationKind::Primary.span(next.span).label(
								"`COMMIT` statements can only be used within a transaction block",
							),
						))
						.to_diagnostic()
				}));
			}
			T![USE] => Ok(TopLevelExpr::Use(parser.parse_sync_push()?)),
			T![OPTION] => Ok(TopLevelExpr::Option(parser.parse_sync_push()?)),
			T![KILL] => Ok(TopLevelExpr::Kill(parser.parse_sync_push()?)),
			_ => Ok(TopLevelExpr::Expr(parser.parse_push().await?)),
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
			BaseTokenKind::UuidString => KillKind::Uuid(parser.parse_sync_push()?),
			BaseTokenKind::Param => KillKind::Param(parser.parse_sync_push()?),
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
			let ns = parser.parse_sync_push()?;
			if parser.eat(T![DATABASE])?.is_some() {
				let db = parser.parse_sync_push()?;
				(UseKind::NamespaceDatabase(ns, db), start.extend(parser[db].span))
			} else {
				(UseKind::Namespace(ns), start.extend(parser[ns].span))
			}
		} else if parser.eat(T![DATABASE])?.is_some() {
			let db = parser.parse_sync_push()?;

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

		let name = parser.parse_sync_push()?;
		let _ = parser.expect(T![=])?;
		let value_token = parser.peek_expect("either `true` or `false`")?;
		let value = match value_token.token {
			T![true] => true,
			T![false] => false,
			_ => return Err(parser.unexpected("either `true` or `false`")),
		};

		let span = start.extend(value_token.span);

		Ok(ast::OptionStmt {
			name,
			value,
			span,
		})
	}
}
