use reblessive::Stk;

use crate::syn::v2::parser::mac::unexpected;
use crate::syn::v2::parser::ParseError;
use crate::{
	sql::{statements::InsertStatement, Data, Value},
	syn::v2::{
		parser::{mac::expected, ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub(crate) async fn parse_insert_stmt(
		&mut self,
		ctx: &mut Stk,
	) -> ParseResult<InsertStatement> {
		let mut ignore = false;
		let mut relation = false;

		for _ in 0..2 {
			match self.peek_kind() {
				t!("RELATION") => {
					if relation {
						break;
					}
					self.pop_peek();
					relation = true;
				}
				t!("IGNORE") => {
					if ignore {
						break;
					}
					self.pop_peek();
					ignore = true;
				}
				_ => {
					break;
				}
			}
		}

		let into = if self.peek_kind() == t!("INTO") {
			self.pop_peek();
			let next = self.next();
			match next.kind {
				t!("$param") => {
					let param = self.token_value(next)?;
					Value::Param(param)
				}
				_ => {
					let table = self.token_value(next)?;
					Value::Table(table)
				}
			}
		} else {
			if !relation {
				// TODO: This should always error, and there's probably a better way to do this
				expected!(self, t!("INTO"));
			}
			Value::None
		};

		let data = match self.peek_kind() {
			t!("(") => {
				if relation {
					unexpected!(self, t!("("), "an array of relations");
				}
				let start = self.pop_peek().span;
				let fields = self.parse_idiom_list(ctx).await?;
				self.expect_closing_delimiter(t!(")"), start)?;
				expected!(self, t!("VALUES"));

				let start = expected!(self, t!("(")).span;
				let mut values = vec![ctx.run(|ctx| self.parse_value(ctx)).await?];
				while self.eat(t!(",")) {
					values.push(ctx.run(|ctx| self.parse_value(ctx)).await?);
				}
				self.expect_closing_delimiter(t!(")"), start)?;

				let mut values = vec![values];
				while self.eat(t!(",")) {
					let start = expected!(self, t!("(")).span;
					let mut inner_values = vec![ctx.run(|ctx| self.parse_value(ctx)).await?];
					while self.eat(t!(",")) {
						inner_values.push(ctx.run(|ctx| self.parse_value(ctx)).await?);
					}
					values.push(inner_values);
					self.expect_closing_delimiter(t!(")"), start)?;
				}

				Data::ValuesExpression(
					values
						.into_iter()
						.map(|row| fields.iter().cloned().zip(row).collect())
						.collect(),
				)
			}
			_ => {
				let value = ctx.run(|ctx| self.parse_value(ctx)).await?;
				Data::SingleExpression(value)
			}
		};

		let update = if self.eat(t!("ON")) {
			Some(self.parse_insert_update(ctx).await?)
		} else {
			None
		};
		let output = self.try_parse_output(ctx).await?;
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
			relation: false,
		})
	}

	async fn parse_insert_update(&mut self, ctx: &mut Stk) -> ParseResult<Data> {
		expected!(self, t!("DUPLICATE"));
		expected!(self, t!("KEY"));
		expected!(self, t!("UPDATE"));
		let l = self.parse_plain_idiom(ctx).await?;
		let o = self.parse_assigner()?;
		let r = ctx.run(|ctx| self.parse_value(ctx)).await?;
		let mut data = vec![(l, o, r)];

		while self.eat(t!(",")) {
			let l = self.parse_plain_idiom(ctx).await?;
			let o = self.parse_assigner()?;
			let r = ctx.run(|ctx| self.parse_value(ctx)).await?;
			data.push((l, o, r))
		}

		Ok(Data::UpdateExpression(data))
	}
}
