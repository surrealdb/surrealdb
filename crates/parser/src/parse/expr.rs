use ast::{TopLevelExpr, Transaction, UseStatementKind};

use super::{Parse, ParseSync, Parser};

impl Parse for ast::TopLevelExpr {
	async fn parse(parser: &mut Parser<'_, '_>) -> Result<Self, ()> {
		let Some(next) = parser.peek()? else {
			todo!()
		};
		match next.token {
			t![BEGIN] => {
				todo!()
			}
			t![CANCEL] => {
				// Invalid.
				todo!()
			}
			t![USE] => Ok(TopLevelExpr::Use(parser.parse_sync_push()?)),
			_ => todo!(),
		}
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
			todo!()
		};

		Ok(ast::UseStatement {
			kind,
			span,
		})
	}
}

fn parse_transaction(parser: &mut Parser<'_, '_>) -> Result<Transaction, ()> {
	todo!()
}
