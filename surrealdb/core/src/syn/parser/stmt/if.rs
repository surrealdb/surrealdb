use reblessive::Stk;

use crate::sql::Expr;
use crate::sql::statements::IfelseStatement;
use crate::syn::parser::mac::{expected, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub(crate) async fn parse_if_stmt(&mut self, stk: &mut Stk) -> ParseResult<IfelseStatement> {
		let condition = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;

		let mut res = IfelseStatement {
			exprs: Vec::new(),
			close: None,
		};

		let next = self.next();
		match next.kind {
			t!("THEN") => {
				let body = stk.run(|stk| self.parse_expr_inherit(stk)).await?;
				self.eat(t!(";"));
				res.exprs.push((condition, body));
				self.parse_worded_tail(stk, &mut res).await?;
			}
			t!("{") => {
				let body = self.parse_block(stk, next.span).await?;
				res.exprs.push((condition, Expr::Block(Box::new(body))));
				self.parse_bracketed_tail(stk, &mut res).await?;
			}
			_ => unexpected!(self, next, "THEN or '{'"),
		}

		Ok(res)
	}

	async fn parse_worded_tail(
		&mut self,
		stk: &mut Stk,
		res: &mut IfelseStatement,
	) -> ParseResult<()> {
		loop {
			let next = self.next();
			match next.kind {
				t!("END") => return Ok(()),
				t!("ELSE") => {
					if self.eat(t!("IF")) {
						let condition = stk.run(|stk| self.parse_expr_inherit(stk)).await?;
						expected!(self, t!("THEN"));
						let body = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
						self.eat(t!(";"));
						res.exprs.push((condition, body));
					} else {
						let value = stk.run(|stk| self.parse_expr_inherit(stk)).await?;
						self.eat(t!(";"));
						expected!(self, t!("END"));
						res.close = Some(value);
						return Ok(());
					}
				}
				_ => unexpected!(self, next, "if to end"),
			}
		}
	}

	async fn parse_bracketed_tail(
		&mut self,
		stk: &mut Stk,
		res: &mut IfelseStatement,
	) -> ParseResult<()> {
		loop {
			match self.peek_kind() {
				t!("ELSE") => {
					self.pop_peek();
					if self.eat(t!("IF")) {
						let condition = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
						let span = expected!(self, t!("{")).span;
						let body = self.parse_block(stk, span).await?;
						res.exprs.push((condition, Expr::Block(Box::new(body))));
					} else {
						let span = expected!(self, t!("{")).span;
						let value = self.parse_block(stk, span).await?;
						res.close = Some(Expr::Block(Box::new(value)));
						return Ok(());
					}
				}
				_ => return Ok(()),
			}
		}
	}
}
