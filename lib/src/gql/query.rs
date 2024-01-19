use std::collections::BTreeMap;

use crate::err::Error;
use crate::sql;
use crate::sql::statements::SelectStatement;
use crate::sql::Fields;
use crate::sql::Limit;
use crate::sql::Query;
use crate::sql::Start;
use crate::sql::Statement;
use crate::sql::Table;
use crate::sql::Values;
use graphql_parser::parse_query;
use graphql_parser::query;
use graphql_parser::schema;

pub fn parse_and_transpile(txt: &str) -> Result<Query, Error> {
	let ast: query::Document<&str> =
		parse_query::<&str>(txt).map_err(|e| Error::Thrown(e.to_string()))?;

	info!("graphql ast: {:#?}", ast);

	let mut query: Vec<Statement> = vec![];

	for d in ast.definitions.iter() {
		match d {
			query::Definition::Operation(o) => match o {
				query::OperationDefinition::SelectionSet(s) => {
					query.extend(transpile_selection_set(s.clone())?)
				}
				query::OperationDefinition::Query(_) => todo!(),
				query::OperationDefinition::Mutation(_) => todo!(),
				query::OperationDefinition::Subscription(_) => todo!(),
			},
			query::Definition::Fragment(_) => todo!(),
		}
	}

	info!("query: {:#?}", query);

	Ok(query.into())
}

fn transpile_selection_set<'a>(
	ss: query::SelectionSet<'a, &'a str>,
) -> Result<Vec<Statement>, Error> {
	let statements = ss
		.items
		.iter()
		.map(|s| match s {
			query::Selection::Field(f) => {
				let args: BTreeMap<&str, schema::Value<&str>> =
					f.arguments.iter().cloned().collect();
				let limit = args
					.get("limit")
					.map(|v| {
						let schema::Value::Int(i) = v else {
							panic!("must be int, should be caught be validation")
						};
						i.as_i64()
					})
					.flatten();
				let start = args
					.get("start")
					.map(|v| {
						let schema::Value::Int(i) = v else {
							panic!("must be int, should be caught be validation")
						};
						i.as_i64()
					})
					.flatten();
				// where : {value: {gt: 0.5}}
				// Value::Expression(Expression::Binary {l: Expression::Field(Field {name: "value".to_string(), ..Default::default()}), o: Operator::MoreThan, r: Expression::Value(Value::Float(0.5))}
				let filter = args.get("where").map(|v| {
					let schema::Value::Object(o) = v else {
						panic!("must be object, should be caught be validation")
					};
					assert!(o.len() == 1, "multiple fields not currently supported");
					let (field_name, cond) = o.into_iter().next().unwrap();
					let schema::Value::Object(cond) = cond else {
						panic!("must be object, should be caught be validation")
					};
					assert!(cond.len() == 1, "multiple conditions not currently supported");
					let (op, val) = cond.into_iter().next().unwrap();
					let op = match *op {
						"eq" => sql::Operator::Equal,
						"gt" => sql::Operator::MoreThan,
						"ge" => sql::Operator::MoreThanOrEqual,
						"lt" => sql::Operator::LessThan,
						"le" => sql::Operator::LessThanOrEqual,
						_ => panic!("unsupported operator"),
					};
					let rhs = match val {
						schema::Value::Int(i) => {
							sql::Value::Number(sql::Number::Int(i.as_i64().unwrap()))
						}
						schema::Value::Float(f) => {
							sql::Value::Number(sql::Number::Float(f.clone()))
						}
						_ => panic!("unsupported value"),
					};
					sql::Cond(sql::Value::Expression(
						sql::Expression::Binary {
							l: sql::Value::Idiom(sql::Idiom(vec![sql::Part::Field(sql::Ident(
								field_name.to_string(),
							))])),
							o: op,
							r: rhs,
						}
						.into(),
					))
				});
				Statement::Select(SelectStatement {
					expr: Fields::all(),
					what: Values(vec![sql::Value::Table(Table(f.name.to_string()))]),
					limit: limit.map(|l| Limit(l.into())),
					start: start.map(|s| Start(s.into())),
					cond: filter,
					..Default::default()
				})
			}
			query::Selection::FragmentSpread(_) => todo!(),
			query::Selection::InlineFragment(_) => todo!(),
		})
		.collect::<Vec<_>>();
	Ok(statements)
}
