use surrealdb_types::{
	AlreadyExistsError, AuthError, ConfigurationError, ConnectionError, ConversionError, Error,
	ErrorKind, Kind, LengthMismatchError, NotAllowedError, NotFoundError, Number, Object,
	OutOfRangeError, QueryError, SerializationError, SurrealValue, TypeError, ValidationError,
	Value, object,
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
	let invalid_err = TypeError::Invalid("custom error".to_owned());

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
	let result: Result<u64, _> = value.into_t();

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
fn test_conversion_errors_implement_std_error() {
	// Ensure our conversion errors implement std::error::Error and have non-empty display
	let conv_err = ConversionError::new(Kind::String, Kind::Int);
	assert!(!conv_err.to_string().is_empty());

	let range_err = OutOfRangeError::new(300, "i8");
	assert!(!range_err.to_string().is_empty());

	let len_err = LengthMismatchError::new(3, 2, "tuple");
	assert!(!len_err.to_string().is_empty());
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

// -----------------------------------------------------------------------------
// Public API Error type (wire-friendly, chaining)
// -----------------------------------------------------------------------------

#[test]
fn test_public_error_new() {
	let err = Error::not_found("The table 'users' does not exist".to_string(), None);

	assert_eq!(err.kind(), &ErrorKind::NotFound);
	assert_eq!(err.message(), "The table 'users' does not exist");
	assert!(err.details().is_none());
	assert!(err.cause().is_none());
}

#[test]
fn test_public_error_with_details() {
	let err = Error::not_allowed("Token expired".to_string(), AuthError::TokenExpired);

	assert_eq!(err.kind(), &ErrorKind::NotAllowed);
	assert!(err.details().is_some());
	let d = err.not_allowed_details().unwrap();
	assert!(matches!(d, NotAllowedError::Auth(AuthError::TokenExpired)));
}

#[test]
fn test_public_error_validation_details() {
	let err =
		Error::validation("Invalid request".to_string(), Some(ValidationError::InvalidRequest));
	assert_eq!(err.kind(), &ErrorKind::Validation);
	assert_eq!(err.validation_details(), Some(ValidationError::InvalidRequest));

	let err_no_details = Error::validation("Parse error".to_string(), None);
	assert_eq!(err_no_details.validation_details(), None);

	let err_wrong_kind = Error::not_allowed("Auth failed".to_string(), None);
	assert_eq!(err_wrong_kind.validation_details(), None);
}

#[test]
fn test_public_error_with_cause() {
	let root = Error::internal("connection refused".to_string());
	let top = Error::query("Failed to execute query".to_string(), None).with_cause(root);

	assert_eq!(top.kind(), &ErrorKind::Query);
	assert!(top.cause().is_some());
	let cause = top.cause().unwrap();
	assert_eq!(cause.kind(), &ErrorKind::Internal);
	assert_eq!(cause.message(), "connection refused");
	assert!(cause.cause().is_none());
}

#[test]
fn test_public_error_chain() {
	let root = Error::internal("root".to_string());
	let mid = Error::validation("mid".to_string(), None).with_cause(root);
	let top = Error::not_allowed("top".to_string(), None).with_cause(mid);

	let chain: Vec<_> = top.chain().collect();
	assert_eq!(chain.len(), 3);
	assert_eq!(chain[0].kind(), &ErrorKind::NotAllowed);
	assert_eq!(chain[1].kind(), &ErrorKind::Validation);
	assert_eq!(chain[2].kind(), &ErrorKind::Internal);
}

#[test]
fn test_public_error_display() {
	let err = Error::thrown("Something went wrong".to_string());
	assert!(err.to_string().contains("Something went wrong"));

	let with_cause = Error::validation("outer".to_string(), None)
		.with_cause(Error::internal("inner".to_string()));
	let display = with_cause.to_string();
	assert!(display.contains("outer"));
	assert!(display.contains("inner"));
}

#[test]
fn test_public_error_std_error_source() {
	let inner = Error::internal("inner".to_string());
	let outer = Error::query("outer".to_string(), None).with_cause(inner);

	let source = std::error::Error::source(&outer).unwrap();
	assert_eq!(source.to_string(), "inner");
}

#[test]
fn test_error_kind_unknown_falls_back_to_internal() {
	// Forward compatibility: unknown wire kind strings fall back to Internal
	// so that older SDKs can still deserialize errors from newer servers.
	let mut obj = Object::new();
	obj.insert("kind", "future_kind");
	obj.insert("message", "Message");
	let value = Value::Object(obj);
	let err = Error::from_value(value).unwrap();
	assert_eq!(err.kind(), &ErrorKind::Internal);
	assert_eq!(err.message(), "Message");
}

#[test]
fn test_error_deserialize_without_kind_defaults_to_internal() {
	// Backwards compatibility: wire format without "kind" (e.g. older clients) defaults to Internal
	// when deserialising via SurrealValue::from_value (#[surreal(default)]).
	let mut obj = Object::new();
	obj.insert("message", "Something went wrong");
	let value = Value::Object(obj);
	let err = Error::from_value(value).unwrap();
	assert_eq!(err.kind(), &ErrorKind::Internal);
	assert_eq!(err.message(), "Something went wrong");
}

// -----------------------------------------------------------------------------
// Detail enum wire format: { kind, details? } pattern
// -----------------------------------------------------------------------------

#[test]
fn test_detail_wire_format_unit_variant() {
	// Unit variants produce { "kind": "VariantName" } with no details field
	let val = AuthError::TokenExpired.into_value();
	let Value::Object(obj) = &val else {
		panic!("Expected object, got {val:?}");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("TokenExpired".into())));
	assert!(!obj.contains_key("details"), "Unit variant should not have details field");

	// Round-trip
	let parsed = AuthError::from_value(val).unwrap();
	assert_eq!(parsed, AuthError::TokenExpired);
}

#[test]
fn test_detail_wire_format_struct_variant() {
	// Struct variants produce { "kind": "VariantName", "details": { fields... } }
	let val = AuthError::InvalidRole {
		name: "admin".into(),
	}
	.into_value();
	let Value::Object(obj) = &val else {
		panic!("Expected object, got {val:?}");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("InvalidRole".into())));
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details object");
	};
	assert_eq!(details.get("name"), Some(&Value::String("admin".into())));

	// Round-trip
	let parsed = AuthError::from_value(val).unwrap();
	assert_eq!(
		parsed,
		AuthError::InvalidRole {
			name: "admin".into()
		}
	);
}

#[test]
fn test_detail_wire_format_newtype_variant() {
	// Newtype variant (Auth wrapping AuthError) produces nested { kind, details? }
	let val = NotAllowedError::Auth(AuthError::TokenExpired).into_value();
	let Value::Object(obj) = &val else {
		panic!("Expected object, got {val:?}");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Auth".into())));
	let Some(Value::Object(inner)) = obj.get("details") else {
		panic!("Expected details object with inner auth error");
	};
	assert_eq!(inner.get("kind"), Some(&Value::String("TokenExpired".into())));
	assert!(!inner.contains_key("details"), "Inner unit variant should not have details");

	// Round-trip
	let parsed = NotAllowedError::from_value(val).unwrap();
	assert_eq!(parsed, NotAllowedError::Auth(AuthError::TokenExpired));
}

#[test]
fn test_detail_wire_format_newtype_with_struct_inner() {
	// Auth wrapping a struct variant
	let val = NotAllowedError::Auth(AuthError::NotAllowed {
		actor: "user:john".into(),
		action: "edit".into(),
		resource: "table:secrets".into(),
	})
	.into_value();
	let Value::Object(obj) = &val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Auth".into())));
	let Some(Value::Object(inner)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(inner.get("kind"), Some(&Value::String("NotAllowed".into())));
	let Some(Value::Object(inner_details)) = inner.get("details") else {
		panic!("Expected inner details");
	};
	assert_eq!(inner_details.get("actor"), Some(&Value::String("user:john".into())));

	// Round-trip
	let parsed = NotAllowedError::from_value(val).unwrap();
	assert!(matches!(parsed, NotAllowedError::Auth(AuthError::NotAllowed { .. })));
}

#[test]
fn test_detail_wire_format_all_flat_enums() {
	// Verify all flat detail enums produce the { kind, details? } pattern

	// ValidationError unit
	let val = ValidationError::Parse.into_value();
	let parsed = ValidationError::from_value(val).unwrap();
	assert_eq!(parsed, ValidationError::Parse);

	// ValidationError struct
	let val = ValidationError::InvalidParameter {
		name: "x".into(),
	}
	.into_value();
	let parsed = ValidationError::from_value(val).unwrap();
	assert_eq!(
		parsed,
		ValidationError::InvalidParameter {
			name: "x".into()
		}
	);

	// ConfigurationError unit
	let val = ConfigurationError::LiveQueryNotSupported.into_value();
	let parsed = ConfigurationError::from_value(val).unwrap();
	assert_eq!(parsed, ConfigurationError::LiveQueryNotSupported);

	// SerializationError unit
	let val = SerializationError::Deserialization.into_value();
	let parsed = SerializationError::from_value(val).unwrap();
	assert_eq!(parsed, SerializationError::Deserialization);

	// NotFoundError struct
	let val = NotFoundError::Table {
		name: "users".into(),
	}
	.into_value();
	let Value::Object(obj) = &val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Table".into())));
	let parsed = NotFoundError::from_value(val).unwrap();
	assert_eq!(
		parsed,
		NotFoundError::Table {
			name: "users".into()
		}
	);

	// NotFoundError unit
	let val = NotFoundError::Transaction.into_value();
	let parsed = NotFoundError::from_value(val).unwrap();
	assert_eq!(parsed, NotFoundError::Transaction);

	// AlreadyExistsError struct
	let val = AlreadyExistsError::Record {
		id: "users:123".into(),
	}
	.into_value();
	let parsed = AlreadyExistsError::from_value(val).unwrap();
	assert_eq!(
		parsed,
		AlreadyExistsError::Record {
			id: "users:123".into()
		}
	);

	// ConnectionError unit
	let val = ConnectionError::Uninitialised.into_value();
	let parsed = ConnectionError::from_value(val).unwrap();
	assert_eq!(parsed, ConnectionError::Uninitialised);
}

#[test]
fn test_detail_wire_format_full_error_round_trip() {
	// Full Error with details round-trips through into_value/from_value
	let err = Error::not_allowed("Token expired".to_string(), AuthError::TokenExpired);
	let val = err.into_value();
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.kind(), &ErrorKind::NotAllowed);
	assert_eq!(parsed.message(), "Token expired");
	let details = parsed.not_allowed_details().unwrap();
	assert!(matches!(details, NotAllowedError::Auth(AuthError::TokenExpired)));
}

#[test]
fn test_detail_wire_format_query_timeout() {
	use std::time::Duration;
	let val = QueryError::TimedOut {
		duration: Duration::from_secs(5),
	}
	.into_value();
	let Value::Object(obj) = &val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("TimedOut".into())));
	assert!(obj.contains_key("details"), "Struct variant should have details field");

	let parsed = QueryError::from_value(val).unwrap();
	assert!(
		matches!(parsed, QueryError::TimedOut { duration } if duration == Duration::from_secs(5))
	);
}

// -----------------------------------------------------------------------------
// Error serialization snapshots: verify exact wire format and round-trip
// -----------------------------------------------------------------------------

#[test]
fn test_error_snapshot_not_allowed_auth_token_expired() {
	let err = Error::not_allowed("Token expired".to_string(), AuthError::TokenExpired);
	let val = err.into_value();

	// Note: cause is serialized as None (SurrealValue doesn't support skip_serializing_if
	// on named struct fields, so Option::None becomes Value::None in the output)
	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("NotAllowed".into())));
	assert_eq!(obj.get("message"), Some(&Value::String("Token expired".into())));
	// Verify details has the new { kind, details? } structure
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details object");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Auth".into())));
	let Some(Value::Object(inner)) = details.get("details") else {
		panic!("Expected inner details");
	};
	assert_eq!(inner.get("kind"), Some(&Value::String("TokenExpired".into())));
	assert!(!inner.contains_key("details"), "Unit variant should not have inner details key");

	// Round-trip back to Error
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.kind(), &ErrorKind::NotAllowed);
	assert_eq!(parsed.message(), "Token expired");
	let details = parsed.not_allowed_details().unwrap();
	assert!(matches!(details, NotAllowedError::Auth(AuthError::TokenExpired)));
}

#[test]
fn test_error_snapshot_not_allowed_auth_invalid_role() {
	let err = Error::not_allowed(
		"Bad role".to_string(),
		AuthError::InvalidRole {
			name: "admin".into(),
		},
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Auth".into())));
	let Some(Value::Object(auth_details)) = details.get("details") else {
		panic!("Expected auth details");
	};
	assert_eq!(auth_details.get("kind"), Some(&Value::String("InvalidRole".into())));
	let Some(Value::Object(role_details)) = auth_details.get("details") else {
		panic!("Expected role details");
	};
	assert_eq!(role_details.get("name"), Some(&Value::String("admin".into())));

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	let details = parsed.not_allowed_details().unwrap();
	assert!(matches!(
		details,
		NotAllowedError::Auth(AuthError::InvalidRole { name }) if name == "admin"
	));
}

#[test]
fn test_error_snapshot_not_found_table() {
	let err = Error::not_found(
		"Table not found".to_string(),
		NotFoundError::Table {
			name: "users".into(),
		},
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Table".into())));
	let Some(Value::Object(table_details)) = details.get("details") else {
		panic!("Expected table details");
	};
	assert_eq!(table_details.get("name"), Some(&Value::String("users".into())));

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.kind(), &ErrorKind::NotFound);
	let details = parsed.not_found_details().unwrap();
	assert!(matches!(
		details,
		NotFoundError::Table { name } if name == "users"
	));
}

#[test]
fn test_error_snapshot_validation_parse() {
	let err = Error::validation("Parse error".to_string(), ValidationError::Parse);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Validation".into())));
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Parse".into())));
	assert!(!details.contains_key("details"), "Unit variant should not have inner details key");

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.validation_details(), Some(ValidationError::Parse));
}

#[test]
fn test_error_snapshot_query_timed_out() {
	use std::time::Duration;
	let err = Error::query(
		"Timed out".to_string(),
		QueryError::TimedOut {
			duration: Duration::from_secs(5),
		},
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Query".into())));
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("TimedOut".into())));
	assert!(details.contains_key("details"), "Struct variant should have details");

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.kind(), &ErrorKind::Query);
	let details = parsed.query_details().unwrap();
	assert!(matches!(
		details,
		QueryError::TimedOut { duration } if duration == Duration::from_secs(5)
	));
}

#[test]
fn test_error_snapshot_already_exists_record() {
	let err = Error::already_exists(
		"Record exists".to_string(),
		AlreadyExistsError::Record {
			id: "users:123".into(),
		},
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Record".into())));
	let Some(Value::Object(record_details)) = details.get("details") else {
		panic!("Expected record details");
	};
	assert_eq!(record_details.get("id"), Some(&Value::String("users:123".into())));

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.kind(), &ErrorKind::AlreadyExists);
	let details = parsed.already_exists_details().unwrap();
	assert!(matches!(
		details,
		AlreadyExistsError::Record { id } if id == "users:123"
	));
}

#[test]
fn test_error_snapshot_not_allowed_method() {
	let err = Error::not_allowed(
		"Method blocked".to_string(),
		NotAllowedError::Method {
			name: "begin".into(),
		},
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Method".into())));
	let Some(Value::Object(method_details)) = details.get("details") else {
		panic!("Expected method details");
	};
	assert_eq!(method_details.get("name"), Some(&Value::String("begin".into())));

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	let details = parsed.not_allowed_details().unwrap();
	assert!(matches!(
		details,
		NotAllowedError::Method { name } if name == "begin"
	));
}

#[test]
fn test_error_snapshot_with_cause_chain() {
	let inner = Error::internal("connection lost".to_string());
	let outer = Error::query("Query failed".to_string(), QueryError::Cancelled).with_cause(inner);
	let val = outer.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Query".into())));

	// Verify cause is present and has correct structure
	let Some(Value::Object(cause)) = obj.get("cause") else {
		panic!("Expected cause object");
	};
	assert_eq!(cause.get("kind"), Some(&Value::String("Internal".into())));
	assert_eq!(cause.get("message"), Some(&Value::String("connection lost".into())));

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.kind(), &ErrorKind::Query);
	let cause = parsed.cause().unwrap();
	assert_eq!(cause.kind(), &ErrorKind::Internal);
	assert_eq!(cause.message(), "connection lost");
}

#[test]
fn test_error_snapshot_internal_no_details() {
	let err = Error::internal("Unexpected".to_string());
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Internal".into())));
	assert_eq!(obj.get("message"), Some(&Value::String("Unexpected".into())));
	// details is None (serialized as Value::None since SurrealValue doesn't skip None fields)
	assert_eq!(obj.get("details"), Some(&Value::None));
	assert_eq!(obj.get("cause"), Some(&Value::None));

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.kind(), &ErrorKind::Internal);
	assert_eq!(parsed.message(), "Unexpected");
	assert!(parsed.details().is_none());
	assert!(parsed.cause().is_none());
}

#[test]
fn test_error_snapshot_thrown_no_cause() {
	let err = Error::thrown("custom error".to_string());
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Thrown".into())));
	assert_eq!(obj.get("message"), Some(&Value::String("custom error".into())));
	assert_eq!(obj.get("details"), Some(&Value::None));
	assert_eq!(obj.get("cause"), Some(&Value::None));

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert_eq!(parsed.kind(), &ErrorKind::Thrown);
	assert_eq!(parsed.message(), "custom error");
	assert!(parsed.details().is_none());
	assert!(parsed.cause().is_none());
}

#[test]
fn test_error_snapshot_not_allowed_scripting_unit() {
	let err = Error::not_allowed("Scripting not allowed".to_string(), NotAllowedError::Scripting);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("NotAllowed".into())));
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Scripting".into())));
	assert!(!details.contains_key("details"), "Unit variant should not have inner details");

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	let details = parsed.not_allowed_details().unwrap();
	assert_eq!(details, NotAllowedError::Scripting);
}
