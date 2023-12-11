use std::collections::BTreeMap;

use crate::{
	sql::{Array, Constant, Id, Number, Object, Strand, Thing, Value},
	syn::v2::parser::mac::test_parse,
};

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
