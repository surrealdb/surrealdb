use crate::{
	sql::{self, Id, Ident, Idiom, Part, Query, Statement, Statements, Thing, Value},
	syn::parser::mac::test_parse,
};

mod json;
mod limit;
mod stmt;
mod streaming;
mod value;

#[test]
fn multiple_semicolons() {
	let res = test_parse!(parse_query, r#";;"#).unwrap();
	let expected = sql::Query(sql::Statements(vec![]));
	assert_eq!(res, expected);
}

#[test]
fn glued_identifiers() {
	let res = test_parse!(parse_query, r#"T:1"#).unwrap();
	let expected = sql::Query(sql::Statements(vec![Statement::Value(Value::Thing(Thing {
		tb: "T".to_string(),
		id: Id::Number(1),
	}))]));
	assert_eq!(res, expected);

	let res = test_parse!(parse_query, r#"T9T9T9T:1"#).unwrap();
	let expected = sql::Query(sql::Statements(vec![Statement::Value(Value::Thing(Thing {
		tb: "T9T9T9T".to_string(),
		id: Id::Number(1),
	}))]));
	assert_eq!(res, expected);

	let res = test_parse!(parse_query, r#"Z:1"#).unwrap();
	let expected = sql::Query(sql::Statements(vec![Statement::Value(Value::Thing(Thing {
		tb: "Z".to_string(),
		id: Id::Number(1),
	}))]));
	assert_eq!(res, expected);

	let res = test_parse!(parse_query, r#"Z9Z9Z9Z:1"#).unwrap();
	let expected = sql::Query(sql::Statements(vec![Statement::Value(Value::Thing(Thing {
		tb: "Z9Z9Z9Z".to_string(),
		id: Id::Number(1),
	}))]));
	assert_eq!(res, expected);
}

#[test]
fn less_then_idiom() {
	let src = r#"
		if ($param.foo < 2){
			return 1
		}
	"#;
	test_parse!(parse_query, src).unwrap();
}

#[test]
fn escaped_params() {
	let src = r#"LET $⟨R-_fYU8Wa31kg7tz0JI6Kme⟩ = 5;
		RETURN  $⟨R-_fYU8Wa31kg7tz0JI6Kme⟩"#;

	for (idx, b) in src.as_bytes().iter().enumerate() {
		println!("{:0>4}: {:0>8b}", idx, b);
	}

	test_parse!(parse_query, src).unwrap();
}

#[test]
fn missed_qoute_caused_panic() {
	let src = r#"{"id:0,"method":"query","params"["SLEEP 30s"]}"#;

	test_parse!(parse_query, src).unwrap_err();
}

#[test]
fn query_object() {
	let src = r#"{"id":0,"method":"query","params":["SLEEP 30s"]}"#;

	test_parse!(parse_query, src).inspect_err(|e| eprintln!("{}", e.render_on(src))).unwrap();
}

#[test]
fn ident_is_field() {
	let src = r#"foo"#;

	let field =
		test_parse!(parse_query, src).inspect_err(|e| eprintln!("{}", e.render_on(src))).unwrap();
	assert_eq!(
		field,
		Query(Statements(vec![Statement::Value(Value::Idiom(Idiom(vec![Part::Field(Ident(
			"foo".to_string()
		))])))]))
	);
}

#[test]
fn escaped_params_backtick() {
	test_parse!(
		parse_query,
		r#"LET $`R-_fYU8Wa31kg7tz0JI6Kme` = 5;
		RETURN  $`R-_fYU8Wa31kg7tz0JI6Kme`"#
	)
	.unwrap();
}

#[test]
fn parse_immediate_insert_subquery() {
	let res =
		test_parse!(parse_query, r#"LET $insert = INSERT INTO t (SELECT true FROM 1);"#).unwrap();
}
