use surrealdb_types::{
	ConversionError, Kind, LengthMismatchError, Number, OutOfRangeError, TypeError, Value,
};

#[test]
fn test_conversion_error_basic() {
	let err = ConversionError::new(Kind::String, Kind::Int);

	assert_eq!(err.expected, Kind::String);
	assert_eq!(err.actual, Kind::Int);
	assert!(err.context.is_none());

	let msg = err.to_string();
	// Kind Display shows lowercase: "string", "int"
	assert!(msg.contains("string"));
	assert!(msg.contains("int"));
}

#[test]
fn test_conversion_error_from_value() {
	let value = Value::Number(42.into());
	let err = ConversionError::from_value(Kind::String, &value);

	assert_eq!(err.expected, Kind::String);
	assert_eq!(err.actual, Kind::Number);
}

#[test]
fn test_conversion_error_with_context() {
	let err =
		ConversionError::new(Kind::String, Kind::Int).with_context("field 'name' on struct User");

	assert_eq!(err.context, Some("field 'name' on struct User".to_string()));

	let msg = err.to_string();
	assert!(msg.contains("field 'name' on struct User"));
}

#[test]
fn test_out_of_range_error() {
	let err = OutOfRangeError::new(300, "i8");

	assert_eq!(err.value, "300");
	assert_eq!(err.target_type, "i8");
	assert!(err.context.is_none());

	let msg = err.to_string();
	assert!(msg.contains("300"));
	assert!(msg.contains("i8"));
	assert!(msg.contains("out of range"));
}

#[test]
fn test_out_of_range_error_with_context() {
	let err = OutOfRangeError::new(-1, "u8").with_context("array index");

	assert_eq!(err.context, Some("array index".to_string()));

	let msg = err.to_string();
	assert!(msg.contains("array index"));
}

#[test]
fn test_length_mismatch_error() {
	let err = LengthMismatchError::new(5, 3, "Vec<String>");

	assert_eq!(err.expected, 5);
	assert_eq!(err.actual, 3);
	assert_eq!(err.target_type, "Vec<String>");

	let msg = err.to_string();
	assert!(msg.contains("5"));
	assert!(msg.contains("3"));
	assert!(msg.contains("Vec<String>"));
	assert!(msg.contains("Length mismatch"));
}

#[test]
fn test_type_error_variants() {
	let conv_err = TypeError::Conversion(ConversionError::new(Kind::String, Kind::Int));
	let range_err = TypeError::OutOfRange(OutOfRangeError::new(256, "u8"));
	let len_err = TypeError::LengthMismatch(LengthMismatchError::new(3, 2, "tuple"));
	let invalid_err = TypeError::Invalid("custom error".to_string());

	// All should display without panicking
	assert!(!conv_err.to_string().is_empty());
	assert!(!range_err.to_string().is_empty());
	assert!(!len_err.to_string().is_empty());
	assert!(!invalid_err.to_string().is_empty());
}

// Integration tests with actual conversions

#[test]
fn test_conversion_error_in_practice() {
	// Try to convert a number to a string
	let value = Value::Number(42.into());
	let result: Result<String, _> = value.into_t();

	assert!(result.is_err());
	let err = result.unwrap_err();

	let err_msg = err.to_string();
	// Kind Display shows lowercase
	assert!(err_msg.contains("string"));
	assert!(err_msg.contains("number"));
}

#[test]
fn test_out_of_range_error_in_practice() {
	// Try to convert a large number to i8
	let value = Value::Number(300.into());
	let result: Result<i8, _> = value.into_t();

	assert!(result.is_err());
	let err = result.unwrap_err();

	let err_msg = err.to_string();
	assert!(err_msg.contains("300"));
	assert!(err_msg.contains("out of range") || err_msg.contains("i8"));
}

#[test]
fn test_length_mismatch_in_practice() {
	// Try to convert wrong-length array to tuple
	let value = Value::Array(vec![Value::Number(1.into()), Value::Number(2.into())].into());
	let result: Result<(i64, i64, i64), _> = value.into_t();

	assert!(result.is_err());
	let err = result.unwrap_err();

	let err_msg = err.to_string();
	assert!(err_msg.contains("3") && err_msg.contains("2") || err_msg.contains("Length"));
}

#[test]
fn test_u8_overflow() {
	let value = Value::Number(256.into());
	let result: Result<u8, _> = value.into_t();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let err_msg = err.to_string();

	// Should mention 256 and u8
	assert!(err_msg.contains("256"));
	assert!(err_msg.contains("u8") || err_msg.contains("out of range"));
}

#[test]
fn test_u8_underflow() {
	let value = Value::Number((-1).into());
	let result: Result<u8, _> = value.into_t();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let err_msg = err.to_string();

	// Should mention -1 and u8
	assert!(err_msg.contains("-1") || err_msg.contains("out of range"));
}

#[test]
fn test_i8_overflow() {
	let value = Value::Number(128.into());
	let result: Result<i8, _> = value.into_t();

	assert!(result.is_err());
}

#[test]
fn test_i8_underflow() {
	let value = Value::Number((-129).into());
	let result: Result<i8, _> = value.into_t();

	assert!(result.is_err());
}

#[test]
fn test_u64_success() {
	// This should work
	let value = Value::Number(Number::Int(i64::MAX));
	let result: Result<u64, _> = value.clone().into_t();

	assert!(result.is_ok());
}

#[test]
fn test_array_length_mismatch_tuple() {
	// 2-element array into 3-element tuple
	let value = Value::Array(vec![Value::Number(1.into()), Value::Number(2.into())].into());
	let result: Result<(i64, i64, i64), _> = value.into_t();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let err_msg = err.to_string();

	// Should mention expected 3, got 2
	assert!(err_msg.contains("Length") || (err_msg.contains("3") && err_msg.contains("2")));
}

#[test]
fn test_fixed_array_length_mismatch() {
	// Wrong length for fixed-size array
	let value = Value::Array(vec![Value::Number(1.into()), Value::Number(2.into())].into());
	let result: Result<[i64; 5], _> = value.into_t();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let err_msg = err.to_string();

	// Should mention array length issue
	assert!(err_msg.contains("5") && err_msg.contains("2"));
}

#[test]
fn test_nested_conversion_error() {
	// Array with wrong inner type
	let value = Value::Array(
		vec![
			Value::String("hello".to_string()),
			Value::Number(42.into()), // Wrong type!
		]
		.into(),
	);
	let result: Result<Vec<String>, _> = value.into_t();

	assert!(result.is_err());
	let err = result.unwrap_err();
	let err_msg = err.to_string();

	// Should mention the type mismatch (lowercase)
	assert!(err_msg.contains("string") || err_msg.contains("number"));
}

#[test]
fn test_error_converts_to_anyhow() {
	// Ensure our errors work with anyhow
	let conv_err = ConversionError::new(Kind::String, Kind::Int);
	let anyhow_err: anyhow::Error = conv_err.into();
	assert!(!anyhow_err.to_string().is_empty());

	let range_err = OutOfRangeError::new(300, "i8");
	let anyhow_err: anyhow::Error = range_err.into();
	assert!(!anyhow_err.to_string().is_empty());

	let len_err = LengthMismatchError::new(3, 2, "tuple");
	let anyhow_err: anyhow::Error = len_err.into();
	assert!(!anyhow_err.to_string().is_empty());
}

#[test]
fn test_multiple_numeric_types() {
	// Test all numeric types for safety
	let values = vec![
		(Value::Number(0.into()), true),     // 0 fits in all types
		(Value::Number(255.into()), true),   // Max u8
		(Value::Number(256.into()), false),  // Over u8
		(Value::Number((-1).into()), false), // Under u8
	];

	for (value, should_succeed) in values {
		let result: Result<u8, _> = value.into_t();
		assert_eq!(result.is_ok(), should_succeed, "Failed for u8 with {:?}", result);
	}
}

#[test]
fn test_error_message_quality() {
	// Ensure error messages are helpful
	let value = Value::String("hello".to_string());
	let result: Result<i64, _> = value.into_t();

	let err_msg = result.unwrap_err().to_string();

	// Good error messages should:
	// 1. Mention what was expected
	assert!(err_msg.to_lowercase().contains("int") || err_msg.to_lowercase().contains("number"));

	// 2. Mention what was received
	assert!(err_msg.contains("String") || err_msg.contains("string"));

	println!("Error message: {}", err_msg);
}
