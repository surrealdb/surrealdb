use reblessive::Stk;

use crate::sql::statements::RelateStatement;
use crate::sql::{Expr, Literal};
use crate::syn::parser::mac::{expected, expected_whitespace, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub async fn parse_relate_stmt(&mut self, stk: &mut Stk) -> ParseResult<RelateStatement> {
		let only = self.eat(t!("ONLY"));
		let (kind, from, with) = stk.run(|stk| self.parse_relation(stk)).await?;
		let uniq = self.eat(t!("UNIQUE"));

		let data = self.try_parse_data(stk).await?;
		let output = self.try_parse_output(stk).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		Ok(RelateStatement {
			only,
			kind,
			from,
			with,
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
		let kind = self.parse_relate_kind(stk).await?;
		if is_o {
			expected!(self, t!("->"));
		} else {
			expected!(self, t!("<"));
			expected_whitespace!(self, t!("-"));
		};
		let second = self.parse_relate_expr(stk).await?;
		if is_o {
			Ok((kind, first, second))
		} else {
			Ok((kind, second, first))
		}
	}

	pub async fn parse_relate_kind(&mut self, ctx: &mut Stk) -> ParseResult<Expr> {
		match self.peek_kind() {
			t!("$param") => self.next_token_value().map(Expr::Param),
			t!("(") => {
				let span = self.pop_peek().span;
				let res = self.parse_inner_subquery(ctx, Some(span)).await?;
				Ok(res)
			}
			_ => self.parse_thing_or_table(ctx).await,
		}
	}

	pub async fn parse_relate_expr(&mut self, ctx: &mut Stk) -> ParseResult<Expr> {
		let old = self.table_as_field;
		self.table_as_field = true;
		let r = self.parse_relate_expr_inner(ctx).await;
		self.table_as_field = old;
		r
	}

	async fn parse_relate_expr_inner(&mut self, ctx: &mut Stk) -> ParseResult<Expr> {
		match self.peek_kind() {
			t!("[") => {
				let start = self.pop_peek().span;
				self.parse_array(ctx, start).await.map(|x| Expr::Literal(Literal::Array(x)))
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
			| t!("INFO") => self.parse_inner_subquery(ctx, None).await,
			t!("IF") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_if_stmt(ctx)).await.map(|x| Expr::If(Box::new(x)))
			}
			t!("(") => {
				let span = self.pop_peek().span;
				let res = self.parse_inner_subquery(ctx, Some(span)).await?;
				Ok(res)
			}
			_ => self.parse_record_id(ctx).await.map(|x| Expr::Literal(Literal::RecordId(x))),
		}
	}

	pub async fn parse_thing_or_table(&mut self, ctx: &mut Stk) -> ParseResult<Expr> {
		if self.peek_whitespace1().kind == t!(":") {
			self.parse_record_id(ctx).await.map(|x| Expr::Literal(Literal::RecordId(x)))
		} else {
			self.next_token_value().map(|x| Expr::Table(x))
		}
	}
}
