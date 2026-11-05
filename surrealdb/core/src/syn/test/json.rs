use crate::syn::{self, ParserSettings};
use crate::types::PublicValue;

/// Parses `text` as JSON under the default settings.
fn json(text: &str) -> Result<PublicValue, surrealdb_expr::parser::ParseError> {
	surrealdb_expr::parser::json_with_settings(text, ParserSettings::default())
}

/// Parses `text` as JSON, reading values out of strings.
fn json_legacy(text: &str) -> Result<PublicValue, surrealdb_expr::parser::ParseError> {
	surrealdb_expr::parser::json_with_settings(
		text,
		ParserSettings {
			legacy_strands: true,
			..Default::default()
		},
	)
}

#[test]
fn object_with_negative() {
	json(r#"{"foo": -1 }"#).unwrap();
}

#[test]
fn object_with_trailing_whitespace() {
	json("{\"foo\": -1 }\n").unwrap();
}

#[test]
fn array_with_negative() {
	json(r#"[-1]"#).unwrap();
}

#[test]
fn not_record_id() {
	let res = json(r#" 'foo:bar-baz'  "#).unwrap();
	assert_eq!(res, PublicValue::String("foo:bar-baz".to_owned()))
}

#[test]
fn not_a_record_id_in_object() {
	let res = json(r#"{ "data":"focus:outline-none"}"#).unwrap();
	let object = res.into_object().unwrap();
	let data = object.get("data").unwrap();
	assert_eq!(*data, PublicValue::String("focus:outline-none".to_owned()))
}

#[test]
fn legacy_uuid() {
	let v = json_legacy(r#" "11111111-1111-1111-1111-111111111111" "#).unwrap();
	let surrealdb_types::Value::Uuid(_) = v else {
		panic!()
	};
}

#[test]
fn legacy_datetime() {
	let v = json_legacy(r#" "2024-01-01T00:00:00Z" "#).unwrap();
	let surrealdb_types::Value::Datetime(_) = v else {
		panic!()
	};
}

#[test]
fn json_surrogate_pair() {
	let res = syn::json(r#""\uD83D\uDE00""#).unwrap();
	assert_eq!(res, PublicValue::String("\u{1F600}".to_owned()));
}

#[test]
fn json_surrogate_pair_in_object() {
	let res = syn::json(r#"{"emoji": "\uD83D\uDE00"}"#).unwrap();
	let object = res.into_object().unwrap();
	let emoji = object.get("emoji").unwrap();
	assert_eq!(*emoji, PublicValue::String("\u{1F600}".to_owned()));
}

#[test]
fn surrealql_rejects_surrogate_pair() {
	let res = syn::value(r#""\uD83D\uDE00""#);
	assert!(res.is_err(), "SurrealQL should reject surrogate pairs");
}
