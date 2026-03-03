use crate::syn::{self, ParserSettings};
use crate::types::PublicValue;

#[test]
fn object_with_negative() {
	syn::parse_with(r#"{"foo": -1 }"#.as_bytes(), async |parser, stk| parser.parse_json(stk).await)
		.unwrap();
}

#[test]
fn object_with_trailing_whitespace() {
	syn::parse_with(r#"{"foo": -1 }\n"#.as_bytes(), async |parser, stk| {
		parser.parse_json(stk).await
	})
	.unwrap();
}

#[test]
fn array_with_negative() {
	syn::parse_with(r#"[-1]"#.as_bytes(), async |parser, stk| parser.parse_json(stk).await)
		.unwrap();
}

#[test]
fn not_record_id() {
	let res = syn::parse_with(r#" 'foo:bar-baz'  "#.as_bytes(), async |parser, stk| {
		parser.parse_json(stk).await
	})
	.unwrap();
	assert_eq!(res, PublicValue::String("foo:bar-baz".to_owned()))
}

#[test]
fn not_a_record_id_in_object() {
	let res =
		syn::parse_with(r#"{ "data":"focus:outline-none"}"#.as_bytes(), async |parser, stk| {
			parser.parse_json(stk).await
		})
		.unwrap();

	let object = res.into_object().unwrap();
	let data = object.get("data").unwrap();
	assert_eq!(*data, PublicValue::String("focus:outline-none".to_owned()))
}

#[test]
fn legacy_uuid() {
	let v = syn::parse_with_settings(
		r#" "11111111-1111-1111-1111-111111111111" "#.as_bytes(),
		ParserSettings {
			legacy_strands: true,
			..Default::default()
		},
		async |parser, stk| parser.parse_json(stk).await,
	)
	.unwrap();

	let surrealdb_types::Value::Uuid(_) = v else {
		panic!()
	};
}

#[test]
fn legacy_datetime() {
	let v = syn::parse_with_settings(
		r#" "2024-01-01T00:00:00Z" "#.as_bytes(),
		ParserSettings {
			legacy_strands: true,
			..Default::default()
		},
		async |parser, stk| parser.parse_json(stk).await,
	)
	.unwrap();

	let surrealdb_types::Value::Datetime(_) = v else {
		panic!()
	};
}
