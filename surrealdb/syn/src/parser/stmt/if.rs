use surrealdb_sql::Expr;
use surrealdb_sql::statements::IfelseStatement;

use crate::parser::mac::{expected, unexpected};
use crate::parser::{ParseResult, Parser};
use crate::token::t;

impl Parser<'_> {
	pub fn parse_if_stmt(&mut self) -> ParseResult<IfelseStatement> {
		let condition = self.parse_expr_inherit()?;

		let mut res = IfelseStatement {
			exprs: Vec::new(),
			close: None,
		};

		let next = self.next();
		match next.kind {
			t!("THEN") => {
				let body = self.parse_expr_inherit()?;
				self.eat(t!(";"));
				res.exprs.push((condition, body));
				self.parse_worded_tail(&mut res)?;
			}
			t!("{") => {
				let body = self.parse_block(next.span)?;
				res.exprs.push((condition, Expr::Block(Box::new(body))));
				self.parse_bracketed_tail(&mut res)?;
			}
			_ => unexpected!(self, next, "THEN or '{'"),
		}

		Ok(res)
	}

	fn parse_worded_tail(&mut self, res: &mut IfelseStatement) -> ParseResult<()> {
		loop {
			let next = self.next();
			match next.kind {
				t!("END") => return Ok(()),
				t!("ELSE") => {
					if self.eat(t!("IF")) {
						let condition = self.parse_expr_inherit()?;
						expected!(self, t!("THEN"));
						let body = self.parse_expr_inherit()?;
						self.eat(t!(";"));
						res.exprs.push((condition, body));
					} else {
						let value = self.parse_expr_inherit()?;
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

	fn parse_bracketed_tail(&mut self, res: &mut IfelseStatement) -> ParseResult<()> {
		loop {
			match self.peek_kind() {
				t!("ELSE") => {
					self.pop_peek();
					if self.eat(t!("IF")) {
						let condition = self.parse_expr_inherit()?;
						let span = expected!(self, t!("{")).span;
						let body = self.parse_block(span)?;
						res.exprs.push((condition, Expr::Block(Box::new(body))));
					} else {
						let span = expected!(self, t!("{")).span;
						let value = self.parse_block(span)?;
						res.close = Some(Expr::Block(Box::new(value)));
						return Ok(());
					}
				}
				_ => return Ok(()),
			}
		}
	}
}
