use ast::{Base, Expr};
use token::T;

use super::Parser;
use crate::Parse;
use crate::parse::ParseResult;

impl Parse for ast::If {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![IF])?;
		let condition = parser.parse_enter_push().await?;

		if parser.eat(T![THEN])?.is_some() {
			let then = parser.parse_enter_push().await?;

			let otherwise = if parser.eat(T![ELSE])?.is_some() {
				let peek = parser.peek_expect("a `else` body")?;
				if let T![IF] = peek.token {
					let otherwise = parser.parse_enter_push().await?;
					Some(parser.push(Expr::If(otherwise)))
				} else {
					let res = parser.parse_enter_push().await?;
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
			let then = parser.parse_push().await?;
			let then = parser.push(Expr::Block(then));

			let otherwise = if parser.eat(T![ELSE])?.is_some() {
				let peek = parser.peek_expect("a `else` body")?;
				if let T![IF] = peek.token {
					let otherwise = parser.parse_enter_push().await?;
					Some(parser.push(Expr::If(otherwise)))
				} else {
					let block = parser.parse_push().await?;
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

impl Parse for ast::Let {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![LET])?;

		let param = parser.parse_sync_push()?;

		if parser.eat(T![:])?.is_some() {
			return parser.todo();
		}

		let _ = parser.expect(T![=])?;

		let expr = parser.parse_enter_push().await?;
		let span = parser.span_since(start.span);

		Ok(ast::Let {
			param,
			expr,
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
					Some(parser.parse_enter_push().await?)
				} else {
					None
				};
				ast::InfoKind::Database {
					version,
				}
			}
			T![TABLE] => {
				let _ = parser.next();
				let name = parser.parse_enter_push().await?;
				let version = if parser.eat(T![VERSION])?.is_some() {
					Some(parser.parse_enter_push().await?)
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
				let name = parser.parse_enter_push().await?;

				let base = if parser.eat(T![ON])?.is_some() {
					let peek = parser.peek_expect("`NAMESPACE`, `DATABASE`, or `ROOT`")?;
					let base = match peek.token {
						T![NAMESPACE] => Base::Namespace,
						T![DATABASE] => Base::Database,
						T![ROOT] => Base::Root,
						_ => return Err(parser.unexpected("`NAMESPACE`, `DATABASE`, or `ROOT`")),
					};
					let _ = parser.next();
					Some(base)
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
				let name = parser.parse_enter_push().await?;
				let _ = parser.expect(T![ON])?;
				parser.eat(T![TABLE])?;
				let table = parser.parse_enter_push().await?;
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
