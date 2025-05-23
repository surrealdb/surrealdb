use crate::{
	sql::{Object, SqlValue, Strand},
	syn::parser::mac::test_parse,
};

#[test]
fn object_with_negative() {
	test_parse!(parse_json, r#"{"foo": -1 }"#).unwrap();
}

#[test]
fn object_with_trailing_whitespace() {
	test_parse!(parse_json, r#"{"foo": -1 }\n"#).unwrap();
}

#[test]
fn array_with_negative() {
	test_parse!(parse_json, r#"[-1]"#).unwrap();
}

#[test]
fn not_record_id() {
	let res = test_parse!(parse_json, r#" 'foo:bar-baz'  "#).unwrap();
	assert_eq!(res, SqlValue::Strand(Strand("foo:bar-baz".to_owned())))
}

#[test]
fn not_a_record_id_in_object() {
	let res = test_parse!(parse_json, r#"{ "data":"focus:outline-none", }"#).unwrap();
	let object = res.coerce_to::<Object>().unwrap();
	let data = object.get("data").unwrap();
	assert_eq!(*data, SqlValue::Strand(Strand("focus:outline-none".to_owned())))
}
