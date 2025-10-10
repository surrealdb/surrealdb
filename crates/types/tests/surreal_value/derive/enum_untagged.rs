use surrealdb_types::{SurrealValue, Value};

////////////////////////////////////////////////////
////////////////// Enum untagged ///////////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(untagged)]
enum EnumUntagged {
	Foo,
	Bar,
}

#[test]
fn test_enum_untagged() {
	// kind_of
	let enum_kind = EnumUntagged::kind_of();
	assert_eq!(
		format!("{:?}", enum_kind),
		r#"Either([Literal(String("Foo")), Literal(String("Bar"))])"#
	);

	// is_value
	assert!(EnumUntagged::is_value(&Value::String("Foo".to_string())));
	assert!(EnumUntagged::is_value(&Value::String("Bar".to_string())));
}

#[test]
fn test_enum_untagged_foo() {
	// into_value
	let enum_untagged = EnumUntagged::Foo;
	let value = enum_untagged.into_value();
	assert_eq!(value, Value::String("Foo".to_string()));

	// from_value
	let converted = EnumUntagged::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUntagged::Foo);

	// is_value
	assert!(EnumUntagged::is_value(&value));
	assert!(value.is::<EnumUntagged>());
}

#[test]
fn test_enum_untagged_bar() {
	// into_value
	let enum_untagged = EnumUntagged::Bar;
	let value = enum_untagged.into_value();
	assert_eq!(value, Value::String("Bar".to_string()));

	// from_value
	let converted = EnumUntagged::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUntagged::Bar);

	// is_value
	assert!(EnumUntagged::is_value(&value));
	assert!(value.is::<EnumUntagged>());
}

////////////////////////////////////////////////////
///////////// Enum untagged lowercase //////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(untagged, lowercase)]
enum EnumUntaggedLowercase {
	Foo,
	Bar,
}

#[test]
fn test_enum_untagged_lowercase() {
	// kind_of
	let enum_kind = EnumUntaggedLowercase::kind_of();
	assert_eq!(
		format!("{:?}", enum_kind),
		r#"Either([Literal(String("foo")), Literal(String("bar"))])"#
	);

	// is_value
	assert!(EnumUntaggedLowercase::is_value(&Value::String("foo".to_string())));
	assert!(EnumUntaggedLowercase::is_value(&Value::String("bar".to_string())));
}

#[test]
fn test_enum_untagged_lowercase_foo() {
	// into_value
	let enum_untagged = EnumUntaggedLowercase::Foo;
	let value = enum_untagged.into_value();
	assert_eq!(value, Value::String("foo".to_string()));

	// from_value
	let converted = EnumUntaggedLowercase::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUntaggedLowercase::Foo);

	// is_value
	assert!(EnumUntaggedLowercase::is_value(&value));
	assert!(value.is::<EnumUntaggedLowercase>());
}

#[test]
fn test_enum_untagged_lowercase_bar() {
	// into_value
	let enum_untagged = EnumUntaggedLowercase::Bar;
	let value = enum_untagged.into_value();
	assert_eq!(value, Value::String("bar".to_string()));

	// from_value
	let converted = EnumUntaggedLowercase::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUntaggedLowercase::Bar);

	// is_value
	assert!(EnumUntaggedLowercase::is_value(&value));
	assert!(value.is::<EnumUntaggedLowercase>());
}

////////////////////////////////////////////////////
///////////// Enum untagged uppercase //////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(untagged, uppercase)]
enum EnumUntaggedUppercase {
	Foo,
	Bar,
}

#[test]
fn test_enum_untagged_uppercase() {
	// kind_of
	let enum_kind = EnumUntaggedUppercase::kind_of();
	assert_eq!(
		format!("{:?}", enum_kind),
		r#"Either([Literal(String("FOO")), Literal(String("BAR"))])"#
	);

	// is_value
	assert!(EnumUntaggedUppercase::is_value(&Value::String("FOO".to_string())));
	assert!(EnumUntaggedUppercase::is_value(&Value::String("BAR".to_string())));
}

#[test]
fn test_enum_untagged_uppercase_foo() {
	// into_value
	let enum_untagged = EnumUntaggedUppercase::Foo;
	let value = enum_untagged.into_value();
	assert_eq!(value, Value::String("FOO".to_string()));

	// from_value
	let converted = EnumUntaggedUppercase::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUntaggedUppercase::Foo);

	// is_value
	assert!(EnumUntaggedUppercase::is_value(&value));
	assert!(value.is::<EnumUntaggedUppercase>());
}

#[test]
fn test_enum_untagged_uppercase_bar() {
	// into_value
	let enum_untagged = EnumUntaggedUppercase::Bar;
	let value = enum_untagged.into_value();
	assert_eq!(value, Value::String("BAR".to_string()));

	// from_value
	let converted = EnumUntaggedUppercase::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUntaggedUppercase::Bar);

	// is_value
	assert!(EnumUntaggedUppercase::is_value(&value));
	assert!(value.is::<EnumUntaggedUppercase>());
}
