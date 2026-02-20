use ast::Expr;
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
