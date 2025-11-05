use surrealdb_types::{Array, SurrealValue, Value};

////////////////////////////////////////////////////
////////////// Enum mixed with value ///////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(untagged)]
enum EnumMixedWithValue {
	#[surreal(value = false)]
	None,
	Some(Vec<String>),
}

#[test]
fn test_enum_mixed_with_value() {
	// kind_of
	let enum_kind = EnumMixedWithValue::kind_of();
	assert_eq!(
		format!("{:?}", enum_kind),
		r#"Either([Literal(Bool(false)), Array(String, None)])"#
	);

	// is_value
	assert!(!EnumMixedWithValue::is_value(&Value::None));
	assert!(!EnumMixedWithValue::is_value(&Value::Bool(true)));
	assert!(EnumMixedWithValue::is_value(&Value::Array(Array::new())));
}

#[test]
fn test_enum_mixed_with_value_none() {
	// into_value
	let enum_mixed_with_value = EnumMixedWithValue::None;
	let value = enum_mixed_with_value.into_value();
	assert_eq!(value, Value::Bool(false));

	// from_value
	let converted = EnumMixedWithValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumMixedWithValue::None);

	// is_value
	assert!(EnumMixedWithValue::is_value(&value));
	assert!(value.is::<EnumMixedWithValue>());
}

#[test]
fn test_enum_mixed_with_value_some() {
	// into_value
	let enum_mixed_with_value = EnumMixedWithValue::Some(vec!["Alice".to_string()]);
	let value = enum_mixed_with_value.into_value();
	assert_eq!(value, Value::Array(Array::from(vec![Value::String("Alice".to_string())])));

	// from_value
	let converted = EnumMixedWithValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumMixedWithValue::Some(vec!["Alice".to_string()]));

	// is_value
	assert!(EnumMixedWithValue::is_value(&value));
	assert!(value.is::<EnumMixedWithValue>());
}
