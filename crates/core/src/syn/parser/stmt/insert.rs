use reblessive::Stk;

use crate::{
	sql::{statements::InsertStatement, Array, Assignment, Data, Idiom, Subquery, Value},
	syn::{
		error::bail,
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

		let data = self.parse_insert_values(ctx).await?;

		let update = if self.eat(t!("ON")) {
			Some(self.parse_insert_update(ctx).await?)
		} else {
			None
		};
		let output = self.try_parse_output(ctx).await?;
		let version = self.try_parse_version(ctx).await?;
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

	fn extract_idiom(subquery: Subquery) -> Option<Idiom> {
		let Subquery::Value(Value::Idiom(idiom)) = subquery else {
			return None;
		};

		Some(idiom)
	}

	async fn parse_insert_values(&mut self, ctx: &mut Stk) -> ParseResult<Data> {
		let token = self.peek();
		// not a `(` so it cant be `(a,b) VALUES (c,d)`
		if token.kind != t!("(") {
			let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
			return Ok(Data::SingleExpression(value));
		}

		// might still be a subquery `(select foo from ...`
		self.pop_peek();
		let before = self.peek().span;
		let backup = self.table_as_field;
		self.table_as_field = true;
		let subquery = self.parse_inner_subquery(ctx, None).await?;
		self.table_as_field = backup;
		let subquery_span = before.covers(self.last_span());

		let mut idioms = Vec::new();
		let select_span = if !self.eat(t!(",")) {
			// not a comma so it might be a single (a) VALUES (b) or a subquery
			self.expect_closing_delimiter(t!(")"), token.span)?;
			let select_span = token.span.covers(self.last_span());

			if !self.eat(t!("VALUES")) {
				// found a subquery
				return Ok(Data::SingleExpression(Value::Subquery(Box::new(subquery))));
			}

			// found an values expression, so subquery must be an idiom
			let Some(idiom) = Self::extract_idiom(subquery) else {
				bail!("Invalid value, expected an idiom in INSERT VALUES statement.",
					@subquery_span => "Here only idioms are allowed")
			};

			idioms.push(idiom);
			select_span
		} else {
			// found an values expression, so subquery must be an idiom
			let Some(idiom) = Self::extract_idiom(subquery) else {
				bail!("Invalid value, expected an idiom in INSERT VALUES statement.",
					@subquery_span => "Here only idioms are allowed")
			};

			idioms.push(idiom);

			loop {
				idioms.push(self.parse_plain_idiom(ctx).await?);

				if !self.eat(t!(",")) {
					break;
				}
			}

			self.expect_closing_delimiter(t!(")"), token.span)?;

			expected!(self, t!("VALUES"));

			token.span.covers(self.last_span())
		};

		let mut insertions = Vec::new();
		loop {
			let mut values = Vec::new();
			let start = expected!(self, t!("(")).span;
			loop {
				values.push(self.parse_value_field(ctx).await?);

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

	async fn parse_insert_update(&mut self, ctx: &mut Stk) -> ParseResult<Data> {
		expected!(self, t!("DUPLICATE"));
		expected!(self, t!("KEY"));
		expected!(self, t!("UPDATE"));

		let token = self.peek();
		// not a `[` so it has to be `ON DUPLICATE KEY UPDATE a = b`
		if token.kind != t!("[") {
			//first update field: required cant be empty
			let l = self.parse_plain_idiom(ctx).await?;
			let o = self.parse_assigner()?;
			let r = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
			let expression = Value::from(Assignment::from((l, o, r)));

			let mut updates = Array::new();
			updates.push(expression);

			//next update fields
			while self.eat(t!(",")) {
				let l = self.parse_plain_idiom(ctx).await?;
				let o = self.parse_assigner()?;
				let r = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				let expression = Value::from(Assignment::from((l, o, r)));
				updates.push(expression);
			}

			//wrap in a vec to match the expected return type
			return Ok(Data::UpdateExpression(Value::Array(updates)));
		}

		//assert it has to be a `[` now
		match token.kind {
			t!("[") => {
				self.pop_peek();
			}
			_ => {
				bail!("Invalid update, expected @field = @value pairs or Array in ON DUPLICATE KEY UPDATE statement.")
			}
		}

		//loop through the array of updates for each record
		let mut updates = Array::new();
		loop {
			//assert it has to be a `{` now for each record
			let token_inner = self.peek();
			match token_inner.kind {
				t!("{") => {
					self.pop_peek();
				}
				_ => {
					bail!(
						"Invalid update, expected an Object in ON DUPLICATE KEY [...] UPDATE statement."
					)
				}
			}

			//allow empty object, because option to only skip update a certain record should be possible
			if self.eat(t!("}")) {
				updates.push(Value::Array(Array::new()));
				if self.eat(t!(",")) {
					continue;
				} else {
					break;
				}
			}

			//first update field: required if it is not empty
			let l = self.parse_plain_idiom(ctx).await?;
			let o = self.parse_assigner()?;
			let r = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
			let expression = Value::from(Assignment::from((l, o, r)));
			let mut expressions = Array::new();
			expressions.push(expression);

			//next update fields
			while self.eat(t!(",")) {
				let l = self.parse_plain_idiom(ctx).await?;
				let o = self.parse_assigner()?;
				let r = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				let expression = Value::from(Assignment::from((l, o, r)));
				expressions.push(expression);
			}

			//push update record for this record
			updates.push(Value::Array(expressions));

			self.expect_closing_delimiter(t!("}"), token_inner.span)?;

			//continue as long as new records to update are found
			if self.eat(t!(",")) {
				continue;
			} else {
				break;
			}
		}
		self.expect_closing_delimiter(t!("]"), token.span)?;

		Ok(Data::UpdateExpression(Value::Array(updates)))
	}
}
