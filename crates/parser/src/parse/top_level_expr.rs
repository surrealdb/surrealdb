use ast::{Query, TopLevelExpr, Transaction, UseStatementKind};
use common::source_error::{AnnotationKind, Level, Snippet};

use crate::parse::ParserState;

use super::{Parse, ParseResult, ParseSync, Parser};

impl Parse for ast::Query {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let mut head = None;
		let mut tail = None;
		while !parser.lex.is_empty() {
			if head.is_some() {
				if parser.eat(t![;])?.is_none() {
					return Err(parser.with_error(|_, span| {
						Level::Error.title("Unexpected token `{}`, expected `;`").element(
							Snippet::base().annotate(
								AnnotationKind::Primary
									.span(span)
									.label("Maybe missing a semicolon on the last statement?"),
							),
						)
					}));
				}
			}

			let expr = parser.parse().await?;
			parser.push_list(expr, &mut head, &mut tail);
		}

		Ok(Query(head))
	}
}

impl Parse for ast::TopLevelExpr {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let next = parser.peek_expect("an expression")?;
		match next.token {
			t![BEGIN] => {
				if parser.state.contains(ParserState::TRANSACTION) {
					return Err(parser.with_error(|parser, span| {
						Level::Error
							.title(format!("Unexpected token `{}`", parser.slice(span)))
							.element(Snippet::base().annotate(
								AnnotationKind::Primary.span(span).label(
									"You cannot start a second transaction within an existing transaction",
								),
							))
							.into()
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
			t![CANCEL] => {
				return Err(parser.with_error(|_, span| {
					Level::Error
						.title("Unexpected token `CANCEL` expected an expression")
						.element(Snippet::base().annotate(
							AnnotationKind::Primary.span(span).label(
								"`CANCEL` statements can only be used within a transaction block",
							),
						))
						.into()
				}));
			}
			t![COMMIT] => {
				return Err(parser.with_error(|_, span| {
					Level::Error
						.title("Unexpected token `COMMIT` expected an expression")
						.element(Snippet::base().annotate(
							AnnotationKind::Primary.span(span).label(
								"`COMMIT` statements can only be used within a transaction block",
							),
						))
						.into()
				}));
			}
			t![USE] => Ok(TopLevelExpr::Use(parser.parse_sync_push()?)),
			t![OPTION] => Ok(TopLevelExpr::Option(parser.parse_sync_push()?)),
			_ => Ok(TopLevelExpr::Expr(parser.parse_push().await?)),
		}
	}
}

impl Parse for ast::Transaction {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		debug_assert!(parser.state.contains(ParserState::TRANSACTION));
		let start = parser.expect(t![BEGIN])?.span;

		let mut head = None;
		let mut tail = None;
		let (end, commits) = loop {
			if parser.eat(t![;])?.is_none() {
				return Err(parser.with_error(|parser, span| {
					Level::Error
						.title(format!(
							"Unexpected token `{}`, expected `;` as the transaction block has not yet ended",
							parser.slice(span)
						))
						.element(
							Snippet::base().annotate(AnnotationKind::Primary.span(span)).annotate(
								AnnotationKind::Context
									.span(start)
									.label("This transaction is still open"),
							),
						)
				}));
			}

			let Some(next) = parser.peek()? else {
				return Err(parser.with_error(|_, span| {
					Level::Error
						.title("Unexpected end of query, expected transaction block to end")
						.element(
							Snippet::base().annotate(AnnotationKind::Primary.span(span)).annotate(
								AnnotationKind::Context
									.span(start)
									.label("Expected this transaction to end"),
							),
						)
				}));
			};

			match next.token {
				t![CANCEL] => {
					parser.next()?;
					break (next.span, false);
				}
				t![COMMIT] => {
					parser.next()?;
					break (next.span, true);
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

impl ParseSync for ast::UseStatement {
	fn parse_sync(parser: &mut Parser) -> super::ParseResult<Self> {
		let start = parser.expect(t![USE])?.span;

		let (kind, span) = if parser.eat(t![NAMESPACE])?.is_some() {
			let ns = parser.parse_sync_push()?;
			if parser.eat(t![DATABASE])?.is_some() {
				let db = parser.parse_sync_push()?;
				(UseStatementKind::NamespaceDatabase(ns, db), start.extend(parser[db].span))
			} else {
				(UseStatementKind::Namespace(ns), start.extend(parser[ns].span))
			}
		} else if parser.eat(t![DATABASE])?.is_some() {
			let db = parser.parse_sync_push()?;

			(UseStatementKind::Database(db), start.extend(parser[db].span))
		} else {
			return Err(parser.unexpected("either `NAMESPACE` or `DATABASE`"));
		};

		Ok(ast::UseStatement {
			kind,
			span,
		})
	}
}

impl ParseSync for ast::OptionStatement {
	fn parse_sync(parser: &mut Parser) -> super::ParseResult<Self> {
		let start = parser.expect(t![OPTION])?.span;

		let name = parser.parse_sync_push()?;
		parser.expect(t![=])?;
		let value_token = parser.peek_expect("either `true` or `false`")?;
		let value = match value_token.token {
			t![true] => true,
			t![false] => false,
			_ => return Err(parser.unexpected("either `true` or `false`")),
		};

		let span = start.extend(value_token.span);

		Ok(ast::OptionStatement {
			name,
			value,
			span,
		})
	}
}
