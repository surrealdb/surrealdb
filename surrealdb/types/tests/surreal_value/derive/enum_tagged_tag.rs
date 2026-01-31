use surrealdb_types::{SurrealValue, Value, object};

////////////////////////////////////////////////////
/////////////// Enum tagged with tag ///////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "tag")]
enum EnumTaggedWithTag {
	Foo,
	Bar {
		prop: String,
	},
}

#[test]
fn test_enum_tagged_with_tag() {
	// kind_of
	let enum_kind = EnumTaggedWithTag::kind_of();
	let kind_foo = r#"Literal(Object({"tag": Literal(String("Foo"))}))"#;
	let kind_bar = r#"Literal(Object({"prop": String, "tag": Literal(String("Bar"))}))"#;
	assert_eq!(format!("{:?}", enum_kind), format!("Either([{kind_foo}, {kind_bar}])"));

	// is_value
	let value_foo = Value::Object(object! { tag: Value::String("Foo".to_string()) });
	let value_bar = Value::Object(
		object! { tag: Value::String("Bar".to_string()), prop: Value::String("bar".to_string()) },
	);
	assert!(EnumTaggedWithTag::is_value(&value_foo));
	assert!(EnumTaggedWithTag::is_value(&value_bar));
}

#[test]
fn test_enum_tagged_with_tag_variant_foo() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTag::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(value, Value::Object(object! { tag: Value::String("Foo".to_string()) }));

	// from_value
	let converted = EnumTaggedWithTag::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedWithTag::Foo);

	// is_value
	assert!(EnumTaggedWithTag::is_value(&value));
	assert!(value.is::<EnumTaggedWithTag>());
}

#[test]
fn test_enum_tagged_with_tag_variant_bar() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTag::Bar {
		prop: "bar".to_string(),
	};
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(
			object! { tag: Value::String("Bar".to_string()), prop: Value::String("bar".to_string()) }
		)
	);

	// from_value
	let converted = EnumTaggedWithTag::from_value(value.clone()).unwrap();
	assert_eq!(
		converted,
		EnumTaggedWithTag::Bar {
			prop: "bar".to_string()
		}
	);

	// is_value
	assert!(EnumTaggedWithTag::is_value(&value));
	assert!(value.is::<EnumTaggedWithTag>());
}

////////////////////////////////////////////////////
////////// Enum tagged variant lowercase ///////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "tag", lowercase)]
enum EnumTaggedWithTagLowercase {
	Foo,
}

#[test]
fn test_enum_tagged_with_tag_variant_lowercase() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTagLowercase::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(value, Value::Object(object! { tag: Value::String("foo".to_string()) }));

	// from_value
	let converted = EnumTaggedWithTagLowercase::from_value(value).unwrap();
	assert_eq!(converted, EnumTaggedWithTagLowercase::Foo);

	// kind_of
	let enum_kind = EnumTaggedWithTagLowercase::kind_of();
	assert_eq!(format!("{:?}", enum_kind), r#"Literal(Object({"tag": Literal(String("foo"))}))"#);

	// is_value
	assert!(EnumTaggedWithTagLowercase::is_value(&Value::Object(
		object! { tag: Value::String("foo".to_string()) }
	)));
	assert!(!EnumTaggedWithTagLowercase::is_value(&Value::String("Foo".to_string())));
}

////////////////////////////////////////////////////
////////// Enum tagged variant uppercase ///////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "tag", uppercase)]
enum EnumTaggedWithTagUppercase {
	Foo,
}

#[test]
fn test_enum_tagged_with_tag_variant_uppercase() {
	// into_value
	let enum_tagged_variant = EnumTaggedWithTagUppercase::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(value, Value::Object(object! { tag: Value::String("FOO".to_string()) }));

	// from_value
	let converted = EnumTaggedWithTagUppercase::from_value(value).unwrap();
	assert_eq!(converted, EnumTaggedWithTagUppercase::Foo);

	// kind_of
	let enum_kind = EnumTaggedWithTagUppercase::kind_of();
	assert_eq!(format!("{:?}", enum_kind), r#"Literal(Object({"tag": Literal(String("FOO"))}))"#);

	// is_value
	assert!(EnumTaggedWithTagUppercase::is_value(&Value::Object(
		object! { tag: Value::String("FOO".to_string()) }
	)));
	assert!(!EnumTaggedWithTagUppercase::is_value(&Value::String("Foo".to_string())));
}
