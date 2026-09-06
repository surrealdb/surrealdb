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
fn legacy_uuid_prefixed_composite_stays_string() {
	let composite =
		"3bb7beb4-128e-488f-aeab-974dd3a2df39__8b0e61e5-02fb-48f7-9e07-89f3cb7c4966__18";
	let input = format!(r#""{composite}""#);
	let v = syn::parse_with_settings(
		input.as_bytes(),
		ParserSettings {
			legacy_strands: true,
			..Default::default()
		},
		async |parser, stk| parser.parse_json(stk).await,
	)
	.unwrap();

	assert_eq!(v, PublicValue::String(composite.to_owned()));
}

#[test]
fn rpc_json_decode_uuid_prefixed_composite_stays_string() {
	use crate::rpc::format::json;

	let composite =
		"3bb7beb4-128e-488f-aeab-974dd3a2df39__8b0e61e5-02fb-48f7-9e07-89f3cb7c4966__18";
	let input = format!(r#"{{"floorId": "{composite}"}}"#);
	let v = json::decode(input.as_bytes()).unwrap();
	let object = v.into_object().unwrap();
	let floor_id = object.get("floorId").unwrap();
	assert_eq!(*floor_id, PublicValue::String(composite.to_owned()));
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
