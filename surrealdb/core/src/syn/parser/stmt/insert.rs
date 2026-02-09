use reblessive::Stk;

use crate::sql::statements::InsertStatement;
use crate::sql::{Data, Expr};
use crate::syn::error::bail;
use crate::syn::parser::mac::expected;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub(crate) async fn parse_insert_stmt(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<InsertStatement> {
		let relation = self.eat(t!("RELATION"));
		let ignore = self.eat(t!("IGNORE"));
		let into = if self.eat(t!("INTO")) {
			let r = match self.peek().kind {
				t!("$param") => {
					let param = self.next_token_value()?;
					Expr::Param(param)
				}
				_ => {
					let table = self.parse_ident()?;
					Expr::Table(table)
				}
			};
			Some(r)
		} else {
			None
		};

		let data = self.parse_insert_values(stk).await?;

		let update = if self.eat(t!("ON")) {
			Some(self.parse_insert_update(stk).await?)
		} else {
			None
		};
		let output = self.try_parse_output(stk).await?;

		// VERSION is no longer supported in INSERT statements, it is left here for backwards
		// compatibility.
		if self.eat(t!("VERSION")) {
			stk.run(|ctx| self.parse_expr_field(ctx)).await?;
		};

		let timeout = self.try_parse_timeout(stk).await?;
		Ok(InsertStatement {
			into,
			data,
			ignore,
			update,
			output,
			timeout,
			relation,
		})
	}

	async fn parse_insert_values(&mut self, stk: &mut Stk) -> ParseResult<Data> {
		let token = self.peek();
		// not a `(` so it cant be `(a,b) VALUES (c,d)`
		if token.kind != t!("(") {
			let value = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
			return Ok(Data::SingleExpression(value));
		}

		// No way to correctly parse this without backtracking.
		// Try to first parse the VALUES case, if that doesn't work retry.
		let speculate_result = self
			.speculate(stk, async |stk, this| {
				this.pop_peek();

				// Some keywords can actually also align with plain idioms, for example `DELETE[1]`
				// This should not be parsed as an idiom.
				if Self::kind_starts_statement(this.peek().kind){
					return Ok(None);
				}


				// If the first value fails to match a idiom, it could still be a valid normal
				// expression so retry in this case.
				let Ok(first) = this.parse_plain_idiom(stk).await else {
					return Ok(None);
				};

				let mut idioms = vec![first];
				let mut ate_comma = false;
				loop {
					if !this.eat(t!(",")) {
						if ate_comma{
							this.expect_closing_delimiter(t!(")"), token.span)?;
						}else if !this.eat(t!(")")){
							return Ok(None)
						}
						break;
					}

					ate_comma = true;

					if this.eat(t!(")")) {
						break;
					}

					// at this point we are sure it has to be a values expression because we ate a
					// `,` so we can throw an error if this fails to parse.
					idioms.push(this.parse_plain_idiom(stk).await?);
				}

				let select_span = token.span.covers(this.last_span());

				if ate_comma {
					expected!(this, t!("VALUES"));
				} else {
					// if we did not eat `,` it could still be a single expression.
					// if we do not find `VALUES` we will have to try again.
					if !this.eat(t!("VALUES")) {
						return Ok(None);
					}
				}

				// after this point it has to be a values expression.

				let mut insertions = Vec::new();
				loop {
					let mut values = Vec::new();
					let start = expected!(this, t!("(")).span;
					loop {
						values.push(stk.run(|ctx| this.parse_expr_field(ctx)).await?);

						if !this.eat(t!(",")) {
							this.expect_closing_delimiter(t!(")"), start)?;
							break;
						}

						if this.eat(t!(")")){
							break
						}

					}

					let span = start.covers(this.last_span());

					if values.len() != idioms.len() {
						bail!("Invalid numbers of values to insert, found {} value(s) but selector requires {} value(s).",
						values.len(), idioms.len(),
						@span,
						@select_span => "This selector has {} field(s)",idioms.len()
						);
					}

					insertions.push(values);

					if !this.eat(t!(",")) {
						break;
					}
				}

				Ok(Some(
						insertions.into_iter().map(|row| idioms.iter().cloned().zip(row).collect()).collect(),
				))

			})
		.await?;

		if let Some(x) = speculate_result {
			Ok(Data::ValuesExpression(x))
		} else {
			let expr = stk.run(|stk| self.parse_expr_field(stk)).await?;
			Ok(Data::SingleExpression(expr))
		}
	}

	async fn parse_insert_update(&mut self, stk: &mut Stk) -> ParseResult<Data> {
		expected!(self, t!("DUPLICATE"));
		expected!(self, t!("KEY"));
		expected!(self, t!("UPDATE"));

		let mut res = Vec::new();
		loop {
			res.push(self.parse_assignment(stk).await?);

			if !self.eat(t!(",")) {
				break;
			}
		}
		Ok(Data::UpdateExpression(res))
	}
}
