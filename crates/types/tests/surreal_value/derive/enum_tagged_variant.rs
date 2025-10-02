use surrealdb_types::{Number, SurrealValue, Value, array, object};

////////////////////////////////////////////////////
/////////////// Enum tagged variant ////////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
enum EnumTaggedVariant {
	Foo,
	Bar {
		prop: String,
	},
	Baz(String),
	Qux(String, i64),
}

#[test]
fn test_enum_tagged_variant() {
	// kind_of
	let enum_kind = EnumTaggedVariant::kind_of();
	let kind_foo = r#"Literal(Object({"Foo": Literal(Object({}))}))"#;
	let kind_bar = r#"Literal(Object({"Bar": Literal(Object({"prop": String}))}))"#;
	let kind_baz = r#"Literal(Object({"Baz": String}))"#;
	let kind_qux = r#"Literal(Object({"Qux": Literal(Array([String, Int]))}))"#;
	assert_eq!(
		format!("{:?}", enum_kind),
		format!("Either([{kind_foo}, {kind_bar}, {kind_baz}, {kind_qux}])")
	);

	// is_value
	let value_foo = Value::Object(object! { Foo: Value::Object(object! {}) });
	let value_bar = Value::Object(
		object! { Bar: Value::Object(object! { prop: Value::String("bar".to_string()) }) },
	);
	let value_baz = Value::Object(object! { Baz: Value::String("baz".to_string()) });
	let value_qux = Value::Object(
		object! { Qux: Value::Array(array! [Value::String("qux".to_string()), Value::Number(Number::Int(1))]) },
	);
	assert!(EnumTaggedVariant::is_value(&value_foo));
	assert!(EnumTaggedVariant::is_value(&value_bar));
	assert!(EnumTaggedVariant::is_value(&value_baz));
	assert!(EnumTaggedVariant::is_value(&value_qux));
}

#[test]
fn test_enum_tagged_variant_foo() {
	// into_value
	let enum_tagged_variant = EnumTaggedVariant::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(value, Value::Object(object! { Foo: Value::Object(object! {}) }));

	// from_value
	let converted = EnumTaggedVariant::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedVariant::Foo);

	// is_value
	assert!(EnumTaggedVariant::is_value(&value));
	assert!(value.is::<EnumTaggedVariant>());
}

#[test]
fn test_enum_tagged_variant_bar() {
	// into_value
	let enum_tagged_variant = EnumTaggedVariant::Bar {
		prop: "bar".to_string(),
	};
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(
			object! { Bar: Value::Object(object! { prop: Value::String("bar".to_string()) }) }
		)
	);

	// from_value
	let converted = EnumTaggedVariant::from_value(value.clone()).unwrap();
	assert_eq!(
		converted,
		EnumTaggedVariant::Bar {
			prop: "bar".to_string()
		}
	);

	// is_value
	assert!(EnumTaggedVariant::is_value(&value));
	assert!(value.is::<EnumTaggedVariant>());
}

#[test]
fn test_enum_tagged_variant_baz() {
	// into_value
	let enum_tagged_variant = EnumTaggedVariant::Baz("baz".to_string());
	let value = enum_tagged_variant.into_value();
	assert_eq!(value, Value::Object(object! { Baz: Value::String("baz".to_string()) }));

	// from_value
	let converted = EnumTaggedVariant::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedVariant::Baz("baz".to_string()));

	// is_value
	assert!(EnumTaggedVariant::is_value(&value));
	assert!(value.is::<EnumTaggedVariant>());
}

#[test]
fn test_enum_tagged_variant_qux() {
	// into_value
	let enum_tagged_variant = EnumTaggedVariant::Qux("qux".to_string(), 1);
	let value = enum_tagged_variant.into_value();
	assert_eq!(
		value,
		Value::Object(
			object! { Qux: Value::Array(array! [Value::String("qux".to_string()), Value::Number(Number::Int(1))]) }
		)
	);

	// from_value
	let converted = EnumTaggedVariant::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedVariant::Qux("qux".to_string(), 1));

	// is_value
	assert!(EnumTaggedVariant::is_value(&value));
	assert!(value.is::<EnumTaggedVariant>());
}

////////////////////////////////////////////////////
////////// Enum tagged variant lowercase ///////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(lowercase)]
enum EnumTaggedVariantLowercase {
	Foo,
}

#[test]
fn test_enum_tagged_variant_lowercase() {
	// into_value
	let enum_tagged_variant = EnumTaggedVariantLowercase::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(value, Value::Object(object! { foo: Value::Object(object! {}) }));

	// from_value
	let converted = EnumTaggedVariantLowercase::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedVariantLowercase::Foo);

	// kind_of
	let enum_kind = EnumTaggedVariantLowercase::kind_of();
	assert_eq!(format!("{:?}", enum_kind), r#"Literal(Object({"foo": Literal(Object({}))}))"#);

	// is_value
	assert!(EnumTaggedVariantLowercase::is_value(&Value::Object(
		object! { foo: Value::Object(object! {}) }
	)));
	assert!(!EnumTaggedVariantLowercase::is_value(&Value::String("foo".to_string())));
}

////////////////////////////////////////////////////
////////// Enum tagged variant uppercase ///////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(uppercase)]
enum EnumTaggedVariantUppercase {
	Foo,
}

#[test]
fn test_enum_tagged_variant_uppercase() {
	// into_value
	let enum_tagged_variant = EnumTaggedVariantUppercase::Foo;
	let value = enum_tagged_variant.into_value();
	assert_eq!(value, Value::Object(object! { FOO: Value::Object(object! {}) }));

	// from_value
	let converted = EnumTaggedVariantUppercase::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumTaggedVariantUppercase::Foo);

	// kind_of
	let enum_kind = EnumTaggedVariantUppercase::kind_of();
	assert_eq!(format!("{:?}", enum_kind), r#"Literal(Object({"FOO": Literal(Object({}))}))"#);

	// is_value
	assert!(EnumTaggedVariantUppercase::is_value(&Value::Object(
		object! { FOO: Value::Object(object! {}) }
	)));
	assert!(!EnumTaggedVariantUppercase::is_value(&Value::String("FOO".to_string())));
}
