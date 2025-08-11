use reblessive::Stk;

use crate::sql::statements::RelateStatement;
use crate::sql::{Expr, Literal};
use crate::syn::parser::mac::{expected, expected_whitespace, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub async fn parse_relate_stmt(&mut self, stk: &mut Stk) -> ParseResult<RelateStatement> {
		let only = self.eat(t!("ONLY"));
		let (from, through, to) = stk.run(|stk| self.parse_relation(stk)).await?;
		let uniq = self.eat(t!("UNIQUE"));

		let data = self.try_parse_data(stk).await?;
		let output = self.try_parse_output(stk).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		Ok(RelateStatement {
			only,
			through,
			from,
			to,
			uniq,
			data,
			output,
			timeout,
			parallel,
		})
	}

	pub async fn parse_relation(&mut self, stk: &mut Stk) -> ParseResult<(Expr, Expr, Expr)> {
		let first = self.parse_relate_expr(stk).await?;
		let next = self.next();
		let is_o = match next.kind {
			t!("->") => true,
			t!("<") => {
				expected_whitespace!(self, t!("-"));
				false
			}
			_ => unexpected!(self, next, "a relation arrow"),
		};
		let through = self.parse_relate_kind(stk).await?;
		if is_o {
			expected!(self, t!("->"));
		} else {
			expected!(self, t!("<"));
			expected_whitespace!(self, t!("-"));
		};
		let second = self.parse_relate_expr(stk).await?;
		if is_o {
			Ok((first, through, second))
		} else {
			Ok((second, through, first))
		}
	}

	pub async fn parse_relate_kind(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		match self.peek_kind() {
			t!("$param") => self.next_token_value().map(Expr::Param),
			t!("(") => {
				let span = self.pop_peek().span;
				let res = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
				self.expect_closing_delimiter(t!(")"), span)?;
				Ok(res)
			}
			_ => self.parse_thing_or_table(stk).await,
		}
	}

	pub async fn parse_relate_expr(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		match self.peek_kind() {
			t!("[") => {
				let start = self.pop_peek().span;
				self.parse_array(stk, start).await.map(|x| Expr::Literal(Literal::Array(x)))
			}
			t!("$param") => self.next_token_value().map(Expr::Param),
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPSERT")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("ALTER")
			| t!("REMOVE")
			| t!("REBUILD")
			| t!("INFO")
			| t!("IF") => self.parse_expr_field(stk).await,
			t!("(") => {
				let open = self.pop_peek().span;
				let res = self.parse_expr_field(stk).await?;
				self.expect_closing_delimiter(t!(")"), open)?;
				Ok(res)
			}
			_ => self.parse_record_id(stk).await.map(|x| Expr::Literal(Literal::RecordId(x))),
		}
	}

	pub async fn parse_thing_or_table(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		if self.peek_whitespace1().kind == t!(":") {
			self.parse_record_id(stk).await.map(|x| Expr::Literal(Literal::RecordId(x)))
		} else {
			self.next_token_value().map(Expr::Table)
		}
	}
}
