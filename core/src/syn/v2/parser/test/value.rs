use std::collections::BTreeMap;

use reblessive::Stack;

use crate::{
	sql::{
		Array, Constant, Id, Number, Object, Query, Statement, Statements, Strand, Thing, Value,
	},
	syn::v2::parser::{mac::test_parse, Parser},
};

#[test]
fn parse_large_depth() {
	let mut text = String::new();
	let start = r#" r"a:[ "#;
	let middle = r#" b:{c: 1} "#;
	let end = r#" ]" "#;

	for _ in 0..1000 {
		text.push_str(start);
	}
	text.push_str(middle);
	for _ in 0..1000 {
		text.push_str(end);
	}
	let mut parser = Parser::new(text.as_bytes())
		.with_query_recursion_limit(100000)
		.with_object_recursion_limit(100000);
	let mut stack = Stack::new();
	let query = stack.run(|ctx| parser.parse_query(ctx)).finish().unwrap();
	let Query(Statements(stmts)) = query;
	let Statement::Value(Value::Thing(ref thing)) = stmts[0] else {
		panic!()
	};
	let mut thing = thing;
	for _ in 0..999 {
		let Id::Array(ref x) = thing.id else {
			panic!()
		};
		let Value::Thing(ref new_thing) = x[0] else {
			panic!()
		};
		thing = new_thing
	}
}

#[test]
fn parse_recursive_record_string() {
	let res = test_parse!(parse_value, r#" r"a:[r"b:{c: r"d:1"}"]" "#).unwrap();
	assert_eq!(
		res,
		Value::Thing(Thing {
			tb: "a".to_owned(),
			id: Id::Array(Array(vec![Value::Thing(Thing {
				tb: "b".to_owned(),
				id: Id::Object(Object(BTreeMap::from([(
					"c".to_owned(),
					Value::Thing(Thing {
						tb: "d".to_owned(),
						id: Id::Number(1)
					})
				)])))
			})]))
		})
	)
}

#[test]
fn parse_record_string_2() {
	let res = test_parse!(parse_value, r#" r'a:["foo"]' "#).unwrap();
	assert_eq!(
		res,
		Value::Thing(Thing {
			tb: "a".to_owned(),
			id: Id::Array(Array(vec![Value::Strand(Strand("foo".to_owned()))]))
		})
	)
}

#[test]
fn parse_i64() {
	let res = test_parse!(parse_value, r#" -9223372036854775808 "#).unwrap();
	assert_eq!(res, Value::Number(Number::Int(i64::MIN)));

	let res = test_parse!(parse_value, r#" 9223372036854775807 "#).unwrap();
	assert_eq!(res, Value::Number(Number::Int(i64::MAX)));
}

#[test]
fn constant_lowercase() {
	let out = test_parse!(parse_value, r#" math::pi "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathPi));
}

#[test]
fn constant_uppercase() {
	let out = test_parse!(parse_value, r#" MATH::PI "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathPi));
}

#[test]
fn constant_mixedcase() {
	let out = test_parse!(parse_value, r#" MaTh::Pi "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathPi));
}
