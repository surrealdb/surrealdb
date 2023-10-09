use crate::{
	sql::{statements::CreateStatement, Data, Operator, Output},
	syn::{
		parser::{
			mac::{to_do, unexpected},
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub fn parse_create_stmt(&mut self) -> ParseResult<CreateStatement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("CREATE"));

		let only = self.eat(t!("ONLY"));
		let what = self.parse_whats()?;
		let data = match self.peek_token().kind {
			t!("SET") => {
				self.pop_peek();
				let mut res = Vec::new();
				loop {
					let idiom = self.parse_plain_idiom()?;
					let operator = match self.next_token().kind {
						t!("=") => Operator::Equal,
						t!("+=") => Operator::Inc,
						t!("-=") => Operator::Dec,
						t!("+?=") => Operator::Ext,
						x => unexpected!(self, x, "an assignment operator"),
					};
					let value = self.parse_value()?;
					res.push((idiom, operator, value));
					if !self.eat(t!(",")) {
						break;
					}
				}

				Some(Data::SetExpression(res))
			}
			t!("UNSET") => {
				self.pop_peek();
				let mut res = Vec::new();
				loop {
					let idiom = self.parse_plain_idiom()?;
					res.push(idiom);
					if !self.eat(t!(",")) {
						break;
					}
				}

				Some(Data::UnsetExpression(res))
			}
			t!("PATCH") => {
				self.pop_peek();
				let value = self.parse_value()?;
				Some(Data::PatchExpression(value))
			}
			t!("MERGE") => {
				self.pop_peek();
				let value = self.parse_value()?;
				Some(Data::MergeExpression(value))
			}
			t!("REPLACE") => {
				self.pop_peek();
				let value = self.parse_value()?;
				Some(Data::ReplaceExpression(value))
			}
			t!("CONTENT") => {
				self.pop_peek();
				let value = self.parse_value()?;
				Some(Data::ContentExpression(value))
			}
			_ => None,
		};

		let output = if self.eat(t!("RETURN")) {
			let output = match self.peek_token().kind {
				t!("NONE") => {
					self.pop_peek();
					Output::None
				}
				t!("NULL") => {
					self.pop_peek();
					Output::Null
				}
				t!("DIFF") => {
					self.pop_peek();
					Output::Diff
				}
				t!("AFTER") => {
					self.pop_peek();
					Output::After
				}
				t!("BEFORE") => {
					self.pop_peek();
					Output::Before
				}
				// if the next token is a `,` then the value was an identifier.
				_ => {
					let fields = self.parse_fields()?;
					Output::Fields(fields)
				}
			};
			Some(output)
		} else {
			None
		};

		let timeout = if self.eat(t!("TIMEOUT")) {
			to_do!(self)
		} else {
			None
		};

		let parallel = self.eat(t!("PARALLEL"));
		Ok(CreateStatement {
			only,
			what,
			data,
			output,
			timeout,
			parallel,
		})
	}
}

#[cfg(test)]
mod test {
	use crate::{
		sql::{
			statements::CreateStatement, Data, Duration, Field, Fields, Ident, Idiom, Number,
			Operator, Output, Part, Table, Timeout, Value, Values,
		},
		syn::parser::mac::test_parse,
	};

	#[test]
	fn basic() {
		let res = test_parse!(
			parse_create_stmt,
			"CREATE ONLY foo SET bar = 3, foo = 4 RETURN VALUE foo AS bar TIMEOUT 1s PARALLEL"
		)
		.unwrap();
		assert_eq!(
			res,
			CreateStatement {
				only: true,
				what: Values(vec![Value::Table(Table("foo".to_string()))]),
				data: Some(Data::SetExpression(vec![
					(
						Idiom(vec![Part::Field(Ident("bar".to_string()))]),
						Operator::Equal,
						Value::Number(Number::Int(3)),
					),
					(
						Idiom(vec![Part::Field(Ident("foo".to_string()))]),
						Operator::Equal,
						Value::Number(Number::Int(4)),
					),
				])),
				output: Some(Output::Fields(Fields(
					vec![Field::Single {
						expr: Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_string()))])),
						alias: Some(Idiom(vec![Part::Field(Ident("bar".to_string()))])),
					}],
					true,
				))),
				timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
				parallel: true,
			}
		);
	}

	#[test]
	fn ambigous_value() {
		let res = test_parse!(parse_create_stmt, "CREATE ONLY foo RETURN VALUE,foo").unwrap();
		assert_eq!(
			res,
			CreateStatement {
				only: true,
				what: Values(vec![Value::Table(Table("foo".to_string()))]),
				data: None,
				output: Some(Output::Fields(Fields(
					vec![
						Field::Single {
							expr: Value::Idiom(Idiom(vec![Part::Field(Ident(
								"VALUE".to_string()
							))])),
							alias: None
						},
						Field::Single {
							expr: Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_string()))])),
							alias: None
						}
					],
					false
				))),
				timeout: None,
				parallel: false
			}
		)
	}
}
