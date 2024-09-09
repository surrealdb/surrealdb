use std::collections::BTreeMap;

use reblessive::Stack;

use crate::{
	sql::{
		Array, Constant, Id, Number, Object, Query, Statement, Statements, Strand, Thing, Value,
	},
	syn::parser::{mac::test_parse, Parser},
};

#[test]
fn parse_coordinate() {
	test_parse!(parse_value_table, "(1.88, -18.0)").unwrap();
}

#[test]
fn parse_like_operator() {
	test_parse!(parse_value_table, "a ~ b").unwrap();
}

#[test]
fn parse_range_operator() {
	test_parse!(parse_value_table, "1..2").unwrap();
}

#[test]
fn parse_large_depth_object() {
	let mut text = String::new();
	let start = r#" { foo: "#;
	let middle = r#" {bar: 1} "#;
	let end = r#" } "#;

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
	let query = stack.enter(|stk| parser.parse_query(stk)).finish().unwrap();
	let Query(Statements(stmts)) = query;
	let Statement::Value(Value::Object(ref object)) = stmts[0] else {
		panic!()
	};
	let mut object = object;
	for _ in 0..999 {
		let Some(Value::Object(ref new_object)) = object.get("foo") else {
			panic!()
		};
		object = new_object
	}
}

#[test]
fn parse_large_depth_record_id() {
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
	let query = stack.enter(|stk| parser.parse_query(stk)).finish().unwrap();
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
	let res = test_parse!(parse_value_table, r#" r"a:[r"b:{c: r"d:1"}"]" "#).unwrap();
	assert_eq!(
		res,
		Value::Thing(Thing {
			tb: "a".to_owned(),
			id: Id::from(Array(vec![Value::Thing(Thing {
				tb: "b".to_owned(),
				id: Id::from(Object(BTreeMap::from([(
					"c".to_owned(),
					Value::Thing(Thing {
						tb: "d".to_owned(),
						id: Id::from(1)
					})
				)])))
			})]))
		})
	)
}

#[test]
fn parse_record_string_2() {
	let res = test_parse!(parse_value_table, r#" r'a:["foo"]' "#).unwrap();
	assert_eq!(
		res,
		Value::Thing(Thing {
			tb: "a".to_owned(),
			id: Id::from(Array(vec![Value::Strand(Strand("foo".to_owned()))]))
		})
	)
}

#[test]
fn parse_i64() {
	let res = test_parse!(parse_value_table, r#" -9223372036854775808 "#).unwrap();
	assert_eq!(res, Value::Number(Number::Int(i64::MIN)));

	let res = test_parse!(parse_value_table, r#" 9223372036854775807 "#).unwrap();
	assert_eq!(res, Value::Number(Number::Int(i64::MAX)));
}

#[test]
fn constant_lowercase() {
	let out = test_parse!(parse_value_table, r#" math::pi "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathPi));

	let out = test_parse!(parse_value_table, r#" math::inf "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathInf));

	let out = test_parse!(parse_value_table, r#" math::neg_inf "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathNegInf));
}

#[test]
fn constant_uppercase() {
	let out = test_parse!(parse_value_table, r#" MATH::PI "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathPi));

	let out = test_parse!(parse_value_table, r#" MATH::INF "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathInf));

	let out = test_parse!(parse_value_table, r#" MATH::NEG_INF "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathNegInf));
}

#[test]
fn constant_mixedcase() {
	let out = test_parse!(parse_value_table, r#" MaTh::Pi "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathPi));

	let out = test_parse!(parse_value_table, r#" MaTh::Inf "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathInf));

	let out = test_parse!(parse_value_table, r#" MaTh::Neg_Inf "#).unwrap();
	assert_eq!(out, Value::Constant(Constant::MathNegInf));
}

#[test]
fn scientific_decimal() {
	let res = test_parse!(parse_value_table, r#" 9.7e-7dec "#).unwrap();
	assert!(matches!(res, Value::Number(Number::Decimal(_))));
	assert_eq!(res.to_string(), "0.00000097dec")
}

#[test]
fn scientific_number() {
	let res = test_parse!(parse_value_table, r#" 9.7e-5"#).unwrap();
	assert!(matches!(res, Value::Number(Number::Float(_))));
	assert_eq!(res.to_string(), "0.000097f")
}

#[test]
fn empty_string() {
	test_parse!(parse_value_table, "").unwrap_err();
}
