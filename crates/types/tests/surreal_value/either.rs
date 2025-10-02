use surrealdb_types::{Either2, Either3, Either8, Either10, Kind, SurrealValue, Value};

#[test]
fn test_either2_string_or_int() {
	type StringOrInt = Either2<String, i64>;

	// Test string variant
	let val = Value::String("hello".to_string());
	let either = StringOrInt::from_value(val.clone()).unwrap();
	assert!(matches!(either, Either2::A(_)));
	assert_eq!(either.into_value(), val);

	// Test int variant
	let val = Value::Number(42.into());
	let either = StringOrInt::from_value(val.clone()).unwrap();
	assert!(matches!(either, Either2::B(_)));
	assert_eq!(either.into_value(), val);

	// Test invalid type (bool)
	let val = Value::Bool(true);
	let result = StringOrInt::from_value(val);
	assert!(result.is_err());
	let err = result.unwrap_err().to_string();
	assert!(err.contains("Expected"));
	assert!(err.contains("union type"));
}

#[test]
fn test_either3_multiple_types() {
	type MultiType = Either3<String, i64, bool>;

	// Test each variant
	let val = Value::String("test".to_string());
	let either = MultiType::from_value(val.clone()).unwrap();
	assert!(matches!(either, Either3::A(_)));

	let val = Value::Number(100.into());
	let either = MultiType::from_value(val.clone()).unwrap();
	assert!(matches!(either, Either3::B(_)));

	let val = Value::Bool(false);
	let either = MultiType::from_value(val.clone()).unwrap();
	assert!(matches!(either, Either3::C(_)));
}

#[test]
fn test_either_roundtrip() {
	type StringOrBool = Either2<String, bool>;

	let original = Value::String("roundtrip".to_string());
	let either = StringOrBool::from_value(original.clone()).unwrap();
	let converted = either.into_value();
	assert_eq!(original, converted);
}

#[test]
fn test_either_kind() {
	type StringOrInt = Either2<String, i64>;
	let kind = StringOrInt::kind_of();

	// Should be an Either kind with 2 variants
	if let Kind::Either(variants) = kind {
		assert_eq!(variants.len(), 2);
	} else {
		panic!("Expected Either kind");
	}
}

#[test]
fn test_either_is_value() {
	type StringOrInt = Either2<String, i64>;

	assert!(StringOrInt::is_value(&Value::String("test".to_string())));
	assert!(StringOrInt::is_value(&Value::Number(42.into())));
	assert!(!StringOrInt::is_value(&Value::Bool(true)));
	assert!(!StringOrInt::is_value(&Value::None));
}

#[test]
fn test_either8_many_variants() {
	type ManyTypes = Either8<String, i64, bool, f64, Vec<i32>, Option<String>, u32, i32>;

	// Test string variant
	let val = Value::String("test".to_string());
	let either = ManyTypes::from_value(val).unwrap();
	assert!(matches!(either, Either8::A(_)));

	// Test bool variant (third type)
	let val = Value::Bool(true);
	let either = ManyTypes::from_value(val).unwrap();
	assert!(matches!(either, Either8::C(_)));
}

#[test]
fn test_either10_max_variants() {
	type MaxTypes = Either10<String, i64, bool, f64, u32, i32, u64, i16, u16, i8>;

	// Test first variant
	let val = Value::String("max".to_string());
	let either = MaxTypes::from_value(val.clone()).unwrap();
	assert!(matches!(either, Either10::A(_)));
	assert_eq!(either.into_value(), val);

	// Test last variant (i8)
	let val = Value::Number(5.into());
	let either = MaxTypes::from_value(val.clone()).unwrap();
	// Should match one of the numeric types
	assert!(matches!(
		either,
		Either10::B(_)
			| Either10::E(_)
			| Either10::F(_)
			| Either10::G(_)
			| Either10::H(_)
			| Either10::I(_)
			| Either10::J(_)
	));
}

#[test]
fn test_either_nested() {
	type Inner = Either2<String, i64>;
	type Outer = Either2<Inner, bool>;

	// Test nested string
	let val = Value::String("nested".to_string());
	let inner = Inner::from_value(val.clone()).unwrap();
	let inner_val = inner.into_value();
	let outer = Outer::from_value(inner_val.clone()).unwrap();
	assert!(matches!(outer, Either2::A(_)));
}

#[test]
fn test_either_error_message() {
	type StringOrInt = Either2<String, i64>;

	let val = Value::Bool(true);
	let result = StringOrInt::from_value(val);

	assert!(result.is_err());
	let err_msg = result.unwrap_err().to_string();

	// Check that error contains useful information
	assert!(err_msg.contains("Expected") || err_msg.contains("union"));
	assert!(err_msg.contains("Bool") || err_msg.contains("bool"));
}
