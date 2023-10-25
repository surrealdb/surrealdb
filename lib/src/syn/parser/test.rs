use crate::{
	sql::{
		block::Entry,
		statements::{CreateStatement, UpdateStatement},
		Block, Cond, Data, Dir, Duration, Field, Fields, Future, Graph, Ident, Idiom, Number,
		Operator, Output, Part, Strand, Table, Tables, Timeout, Value, Values,
	},
	syn::parser::mac::test_parse,
};

#[test]
fn parse_create() {
	let res = test_parse!(
		parse_create_stmt,
		"CREATE ONLY foo SET bar = 3, foo +?= 4 RETURN VALUE foo AS bar TIMEOUT 1s PARALLEL"
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
					Operator::Ext,
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
fn parse_update() {
	let res = test_parse!(
		parse_update_stmt,
		r#"UPDATE ONLY <future> { "test" }, a->b UNSET foo... , a->b, c[*] WHERE true RETURN DIFF TIMEOUT 1s PARALLEL"#
	)
	.unwrap();
	assert_eq!(
		res,
		UpdateStatement {
			only: true,
			what: Values(vec![
				Value::Future(Box::new(Future(Block(vec![Entry::Value(Value::Strand(Strand(
					"text".to_string()
				))),])))),
				Value::Idiom(Idiom(vec![
					Part::Field(Ident("a".to_string())),
					Part::Graph(Graph {
						dir: Dir::Out,
						what: Tables(vec![Table("b".to_string())]),
						..Default::default()
					})
				]))
			]),
			cond: Some(Cond(Value::Bool(true))),
			data: Some(Data::UnsetExpression(vec![
				Idiom(vec![Part::Field(Ident("foo".to_string())), Part::Flatten]),
				Idiom(vec![
					Part::Field(Ident("a".to_string())),
					Part::Graph(Graph {
						dir: Dir::Out,
						what: Tables(vec![Table("b".to_string())]),
						..Default::default()
					})
				]),
			])),
			output: Some(Output::Diff),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
		}
	);
}
