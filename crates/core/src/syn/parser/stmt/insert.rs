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
					let table = self.next_token_value()?;
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

		let version = if self.eat(t!("VERSION")) {
			Some(stk.run(|ctx| self.parse_expr_field(ctx)).await?)
		} else {
			None
		};
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		Ok(InsertStatement {
			into,
			data,
			ignore,
			update,
			output,
			timeout,
			parallel,
			relation,
			version,
		})
	}

	async fn parse_insert_values(&mut self, stk: &mut Stk) -> ParseResult<Data> {
		let token = self.peek();
		// not a `(` so it cant be `(a,b) VALUES (c,d)`
		if token.kind != t!("(") {
			let value = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
			return Ok(Data::SingleExpression(value));
		}

		self.pop_peek();
		// might still be a subquery `(select foo from ...`
		let subquery = stk.run(|ctx| self.parse_expr_field(ctx)).await?;

		let mut idioms = Vec::new();
		let select_span = if self.eat(t!(",")) {
			// found an values expression, so subquery must be an idiom
			let Expr::Idiom(idiom) = subquery else {
				bail!("Invalid value, expected an idiom in INSERT VALUES statement.",
					@token.span.covers(self.last_span()) => "Only idioms are allowed here")
			};

			idioms.push(idiom);

			loop {
				idioms.push(self.parse_plain_idiom(stk).await?);

				if !self.eat(t!(",")) {
					break;
				}
			}

			self.expect_closing_delimiter(t!(")"), token.span)?;

			expected!(self, t!("VALUES"));

			token.span.covers(self.last_span())
		} else {
			// not a comma so it might be a single (a) VALUES (b) or a subquery
			self.expect_closing_delimiter(t!(")"), token.span)?;
			let select_span = token.span.covers(self.last_span());

			if !self.eat(t!("VALUES")) {
				// found a subquery
				return Ok(Data::SingleExpression(subquery));
			}

			// found an values expression, so subquery must be an idiom
			let Expr::Idiom(idiom) = subquery else {
				bail!("Invalid value, expected an idiom in INSERT VALUES statement.",
					@select_span => "Only idioms are allowed here")
			};

			idioms.push(idiom);
			select_span
		};

		let mut insertions = Vec::new();
		loop {
			let mut values = Vec::new();
			let start = expected!(self, t!("(")).span;
			loop {
				values.push(stk.run(|ctx| self.parse_expr_field(ctx)).await?);

				if !self.eat(t!(",")) {
					break;
				}
			}

			self.expect_closing_delimiter(t!(")"), start)?;
			let span = start.covers(self.last_span());

			if values.len() != idioms.len() {
				bail!("Invalid numbers of values to insert, found {} value(s) but selector requires {} value(s).",
					values.len(), idioms.len(),
					@span,
					@select_span => "This selector has {} field(s)",idioms.len()
				);
			}

			insertions.push(values);

			if !self.eat(t!(",")) {
				break;
			}
		}

		Ok(Data::ValuesExpression(
			insertions.into_iter().map(|row| idioms.iter().cloned().zip(row).collect()).collect(),
		))
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
