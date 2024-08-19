use crate::{
	sql::{self, Id, Statement, Thing, Value},
	syn::parser::mac::test_parse,
};

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
fn escaped_params() {
	let src = r#"LET $⟨R-_fYU8Wa31kg7tz0JI6Kme⟩ = 5;
		RETURN  $⟨R-_fYU8Wa31kg7tz0JI6Kme⟩"#;

	for (idx, b) in src.as_bytes().iter().enumerate() {
		println!("{:0>4}: {:0>8b}", idx, b);
	}

	test_parse!(parse_query, src).unwrap();
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
