use reblessive::Stk;

use crate::{
	sql::{statements::InsertStatement, Data, Value},
	syn::{
		parser::{mac::expected, ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub(crate) async fn parse_insert_stmt(
		&mut self,
		ctx: &mut Stk,
	) -> ParseResult<InsertStatement> {
		let relation = self.eat(t!("RELATION"));
		let ignore = self.eat(t!("IGNORE"));
		let into = if self.eat(t!("INTO")) {
			let r = match self.peek().kind {
				t!("$param") => {
					let param = self.next_token_value()?;
					Value::Param(param)
				}
				_ => {
					let table = self.next_token_value()?;
					Value::Table(table)
				}
			};
			Some(r)
		} else {
			None
		};

		let data = match self.peek_kind() {
			t!("(") => {
				let start = self.pop_peek().span;
				let fields = self.parse_idiom_list(ctx).await?;
				self.expect_closing_delimiter(t!(")"), start)?;
				expected!(self, t!("VALUES"));

				let start = expected!(self, t!("(")).span;
				let mut values = vec![ctx.run(|ctx| self.parse_value_class(ctx)).await?];
				while self.eat(t!(",")) {
					values.push(ctx.run(|ctx| self.parse_value_class(ctx)).await?);
				}
				self.expect_closing_delimiter(t!(")"), start)?;

				let mut values = vec![values];
				while self.eat(t!(",")) {
					let start = expected!(self, t!("(")).span;
					let mut inner_values = vec![ctx.run(|ctx| self.parse_value_class(ctx)).await?];
					while self.eat(t!(",")) {
						inner_values.push(ctx.run(|ctx| self.parse_value_class(ctx)).await?);
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
				let value = ctx.run(|ctx| self.parse_value_class(ctx)).await?;
				Data::SingleExpression(value)
			}
		};

		let update = if self.eat(t!("ON")) {
			Some(self.parse_insert_update(ctx).await?)
		} else {
			None
		};
		let output = self.try_parse_output(ctx).await?;
		let version = self.try_parse_version()?;
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

	async fn parse_insert_update(&mut self, ctx: &mut Stk) -> ParseResult<Data> {
		expected!(self, t!("DUPLICATE"));
		expected!(self, t!("KEY"));
		expected!(self, t!("UPDATE"));
		let l = self.parse_plain_idiom(ctx).await?;
		let o = self.parse_assigner()?;
		let r = ctx.run(|ctx| self.parse_value_class(ctx)).await?;
		let mut data = vec![(l, o, r)];

		while self.eat(t!(",")) {
			let l = self.parse_plain_idiom(ctx).await?;
			let o = self.parse_assigner()?;
			let r = ctx.run(|ctx| self.parse_value_class(ctx)).await?;
			data.push((l, o, r))
		}

		Ok(Data::UpdateExpression(data))
	}
}
