use crate::{
	sql::{statements::CreateStatement, Values},
	syn::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub fn parse_create_stmt(&mut self) -> ParseResult<CreateStatement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("CREATE"));

		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list()?);
		let data = self.try_parse_data()?;
		let output = self.try_parse_output()?;
		let timeout = self.try_parse_timeout()?;
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
