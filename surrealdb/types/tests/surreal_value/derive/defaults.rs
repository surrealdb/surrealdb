use rstest::rstest;
use surrealdb_types::{SurrealValue, Value, object};

#[derive(Clone, Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
struct LiteralDefaults {
	#[surreal(default = "true", rename = "is_enabled")]
	enabled: bool,
	#[surreal(default = "false")]
	disabled: bool,
	#[surreal(default = "42")]
	count: i64,
	#[surreal(default = "-9223372036854775808")]
	minimum: i64,
	#[surreal(default = "9223372036854775807")]
	maximum: i64,
	#[surreal(default = "0xff_u32")]
	hex: u32,
	#[surreal(default = "-1.25e2_f64")]
	ratio: f64,
	#[surreal(default = r#""guest""#)]
	name: String,
	#[surreal(default = r##"r#"line\ntext"#"##, wrap)]
	wrapped: String,
}

#[test]
fn missing_fields_use_literal_defaults() {
	let parsed = LiteralDefaults::from_value(Value::Object(object! {})).unwrap();
	assert_eq!(
		parsed,
		LiteralDefaults {
			enabled: true,
			disabled: false,
			count: 42,
			minimum: i64::MIN,
			maximum: i64::MAX,
			hex: 255,
			ratio: -125.0,
			name: "guest".into(),
			wrapped: r"line\ntext".into(),
		}
	);
}

#[test]
fn present_fields_override_literal_defaults() {
	let expected = LiteralDefaults {
		enabled: false,
		disabled: true,
		count: 7,
		minimum: -1,
		maximum: 1,
		hex: 16,
		ratio: 0.5,
		name: "member".into(),
		wrapped: "wrapped".into(),
	};
	let value = expected.clone().into_value();
	let parsed = LiteralDefaults::from_value(value).unwrap();
	assert_eq!(parsed, expected);
}

#[rstest]
#[case(Value::None)]
#[case(Value::Null)]
#[case(Value::String("true".into()))]
fn invalid_present_field_does_not_use_literal_default(#[case] value: Value) {
	assert!(LiteralDefaults::from_value(Value::Object(object! { is_enabled: value })).is_err());
}

#[derive(Debug, Default, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
#[surreal(default)]
struct ContainerDefault {
	#[surreal(default = "true")]
	enabled: bool,
}

#[test]
fn container_default_takes_precedence_over_field_default() {
	let parsed = ContainerDefault::from_value(Value::Object(object! {})).unwrap();
	assert_eq!(parsed, ContainerDefault::default());
	let parsed = ContainerDefault::from_value(Value::Object(object! { enabled: true })).unwrap();
	assert!(parsed.enabled);
}

#[derive(Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
#[surreal(tag = "kind")]
enum TaggedDefault {
	Item {
		#[surreal(default = "true")]
		enabled: bool,
	},
}

#[test]
fn tagged_named_variant_uses_literal_default() {
	let parsed = TaggedDefault::from_value(Value::Object(object! { kind: "Item" })).unwrap();
	assert_eq!(
		parsed,
		TaggedDefault::Item {
			enabled: true
		}
	);
}
