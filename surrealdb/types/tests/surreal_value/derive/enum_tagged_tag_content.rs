use surrealdb_types::{Number, SurrealValue, Value, array, object};

////////////////////////////////////////////////////
/////////////// Enum tagged with tag ///////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "tag", content = "content")]
enum EnumTaggedWithTagAndContent {
	Foo,
	Bar {
		prop: String,
	},
	Baz(String),
	Qux(String, i64),
}

#[test]
fn test_enum_tagged_with_tag_and_content() {
	// kind_of
	let enum_kind = EnumTaggedWithTagAndContent::kind_of();
	let kind_foo =
		r#"Literal(Object({"content": Literal(Object({})), "tag": Literal(String("Foo"))}))"#;
	let kind_bar = r#"Literal(Object({"content": Literal(Object({"prop": String})), "tag": Literal(String("Bar"))}))"#;
	let kind_baz = r#"Literal(Object({"content": String, "tag": Literal(String("Baz"))}))"#;
	let kind_qux = r#"Literal(Object({"content": Literal(Array([String, Int])), "tag": Literal(String("Qux"))}))"#;
	assert_eq!(
		format!("{:?}", enum_kind),
		format!("Either([{kind_foo}, {kind_bar}, {kind_baz}, {kind_qux}])")
	);

	// is_value
	assert!(EnumTaggedWithTagAndContent::is_value(&Value::Object(object! {
		content: Value::Object(object! {}),
		tag: Value::String("Foo".to_string())
	})));

	assert!(EnumTaggedWithTagAndContent::is_value(&Value::Object(object! {
		tag: Value::String("Bar".to_string()),
		content: Value::Object(object! { prop: Value::String("bar".to_string()) })
	})));

	assert!(EnumTaggedWithTagAndContent::is_value(&Value::Object(object! {
		tag: Value::String("Baz".to_string()),
		content: Value::String("baz".to_string())
	})));

	assert!(EnumTaggedWithTagAndContent::is_value(&Value::Object(object! {
		tag: Value::String("Qux".to_string()),
		content: Value::Array(array![Value::String("qux".to_string()), Value::Number(Number::Int(1))])
	})));
}

#[test]
fn test_enum_tagged_with_tag_variant_foo() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTagAndContent::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(object! {
			tag: Value::String("Foo".to_string()),
			content: Value::Object(object! {}),
		})
	);

	// from_value
	let converted = EnumTaggedWithTagAndContent::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedWithTagAndContent::Foo);

	// is_value
	assert!(EnumTaggedWithTagAndContent::is_value(&value));
	assert!(value.is::<EnumTaggedWithTagAndContent>());
}

#[test]
fn test_enum_tagged_with_tag_variant_bar() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTagAndContent::Bar {
		prop: "bar".to_string(),
	};
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(object! {
			tag: Value::String("Bar".to_string()),
			content: Value::Object(object! { prop: Value::String("bar".to_string()) })
		})
	);

	// from_value
	let converted = EnumTaggedWithTagAndContent::from_value(value.clone()).unwrap();
	assert_eq!(
		converted,
		EnumTaggedWithTagAndContent::Bar {
			prop: "bar".to_string()
		}
	);

	// is_value
	assert!(EnumTaggedWithTagAndContent::is_value(&value));
	assert!(value.is::<EnumTaggedWithTagAndContent>());
}

#[test]
fn test_enum_tagged_with_tag_variant_baz() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTagAndContent::Baz("baz".to_string());
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(
			object! { tag: Value::String("Baz".to_string()), content: Value::String("baz".to_string()) }
		)
	);

	// from_value
	let converted = EnumTaggedWithTagAndContent::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedWithTagAndContent::Baz("baz".to_string()));

	// is_value
	assert!(EnumTaggedWithTagAndContent::is_value(&value));
	assert!(value.is::<EnumTaggedWithTagAndContent>());
}

#[test]
fn test_enum_tagged_with_tag_variant_qux() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTagAndContent::Qux("qux".to_string(), 1);
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(
			object! { tag: Value::String("Qux".to_string()), content: Value::Array(array![Value::String("qux".to_string()), Value::Number(Number::Int(1))]) }
		)
	);

	// from_value
	let converted = EnumTaggedWithTagAndContent::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedWithTagAndContent::Qux("qux".to_string(), 1));

	// is_value
	assert!(EnumTaggedWithTagAndContent::is_value(&value));
	assert!(value.is::<EnumTaggedWithTagAndContent>());
}

////////////////////////////////////////////////////
////////// Enum tagged variant lowercase ///////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "tag", content = "content", lowercase)]
enum EnumTaggedWithTagAndContentLowercase {
	Foo,
}

#[test]
fn test_enum_tagged_with_tag_variant_lowercase() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTagAndContentLowercase::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(
			object! { tag: Value::String("foo".to_string()), content: Value::Object(object! {}) }
		)
	);

	// from_value
	let converted = EnumTaggedWithTagAndContentLowercase::from_value(value).unwrap();
	assert_eq!(converted, EnumTaggedWithTagAndContentLowercase::Foo);

	// kind_of
	let enum_kind = EnumTaggedWithTagAndContentLowercase::kind_of();
	assert_eq!(
		format!("{:?}", enum_kind),
		r#"Literal(Object({"content": Literal(Object({})), "tag": Literal(String("foo"))}))"#
	);

	// is_value
	assert!(EnumTaggedWithTagAndContentLowercase::is_value(&Value::Object(
		object! { tag: Value::String("foo".to_string()), content: Value::Object(object! {}) }
	)));
	assert!(!EnumTaggedWithTagAndContentLowercase::is_value(&Value::String("Foo".to_string())));
}

////////////////////////////////////////////////////
////////// Enum tagged variant uppercase ///////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "tag", content = "content", uppercase)]
enum EnumTaggedWithTagAndContentUppercase {
	Foo,
}

#[test]
fn test_enum_tagged_with_tag_variant_uppercase() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTagAndContentUppercase::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(
			object! { tag: Value::String("FOO".to_string()), content: Value::Object(object! {}) }
		)
	);

	// from_value
	let converted = EnumTaggedWithTagAndContentUppercase::from_value(value).unwrap();
	assert_eq!(converted, EnumTaggedWithTagAndContentUppercase::Foo);

	// kind_of
	let enum_kind = EnumTaggedWithTagAndContentUppercase::kind_of();
	assert_eq!(
		format!("{:?}", enum_kind),
		r#"Literal(Object({"content": Literal(Object({})), "tag": Literal(String("FOO"))}))"#
	);

	// is_value
	assert!(EnumTaggedWithTagAndContentUppercase::is_value(&Value::Object(
		object! { tag: Value::String("FOO".to_string()), content: Value::Object(object! {}) }
	)));
	assert!(!EnumTaggedWithTagAndContentUppercase::is_value(&Value::String("Foo".to_string())));
}
