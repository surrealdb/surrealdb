use surrealdb_types::{
	AlreadyExistsError, AuthError, ConfigurationError, ConnectionError, ConversionError, Error,
	ErrorDetails, Kind, LengthMismatchError, NotAllowedError, NotFoundError, Number, Object,
	OutOfRangeError, QueryError, SerializationError, SurrealValue, TypeError, ValidationError,
	Value,
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

	assert!(err.is_not_found());
	assert_eq!(err.message(), "The table 'users' does not exist");
	assert!(err.not_found_details().is_none());
}

#[test]
fn test_public_error_with_details() {
	let err = Error::not_allowed("Token expired".to_string(), AuthError::TokenExpired);

	assert!(err.is_not_allowed());
	let d = err.not_allowed_details().unwrap();
	assert!(matches!(d, NotAllowedError::Auth(AuthError::TokenExpired)));
}

#[test]
fn test_public_error_validation_details() {
	let err =
		Error::validation("Invalid request".to_string(), Some(ValidationError::InvalidRequest));
	assert!(err.is_validation());
	assert_eq!(err.validation_details(), Some(&ValidationError::InvalidRequest));

	let err_no_details = Error::validation("Parse error".to_string(), None);
	assert_eq!(err_no_details.validation_details(), None);
	assert!(err_no_details.is_validation()); // kind is still Validation

	let err_wrong_kind = Error::not_allowed("Auth failed".to_string(), None);
	assert_eq!(err_wrong_kind.validation_details(), None);
}

#[test]
fn test_public_error_display() {
	let err = Error::thrown("Something went wrong".to_string());
	assert!(err.to_string().contains("Something went wrong"));
}

#[test]
fn test_public_error_details_pattern_matching() {
	let err = Error::not_found(
		"Table not found".to_string(),
		NotFoundError::Table {
			name: "users".into(),
		},
	);

	// Users can pattern match on ErrorDetails directly
	match err.details() {
		ErrorDetails::NotFound(Some(NotFoundError::Table {
			name,
		})) => assert_eq!(name, "users"),
		_ => panic!("Expected NotFound Table details"),
	}
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
	assert!(err.is_internal());
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
	assert!(err.is_internal());
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
	// Full Error with NotAllowed + Auth(TokenExpired) round-trips through into_value/from_value.
	// Wire format:
	// {
	//   "code": -32002,
	//   "message": "Token expired",
	//   "kind": "NotAllowed",
	//   "details": { "kind": "Auth", "details": { "kind": "TokenExpired" } }
	// }
	let err = Error::not_allowed("Token expired".to_string(), AuthError::TokenExpired);
	let val = err.into_value();
	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_allowed());
	assert_eq!(parsed.message(), "Token expired");
	let details = parsed.not_allowed_details().unwrap();
	assert!(matches!(details, NotAllowedError::Auth(AuthError::TokenExpired)));
}

#[test]
fn test_detail_wire_format_query_timeout() {
	// QueryError::TimedOut detail enum serialization.
	// Wire format (detail only):
	// { "kind": "TimedOut", "details": { "duration": { "secs": 5, "nanos": 0 } } }
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
// Each test documents the full JSON structure in comments.
// -----------------------------------------------------------------------------

#[test]
fn test_error_snapshot_not_allowed_auth_token_expired() {
	// Wire format:
	// {
	//   "code": -32002,
	//   "message": "Token expired",
	//   "kind": "NotAllowed",
	//   "details": { "kind": "Auth", "details": { "kind": "TokenExpired" } }
	// }
	let err = Error::not_allowed("Token expired".to_string(), AuthError::TokenExpired);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("NotAllowed".into())));
	assert_eq!(obj.get("message"), Some(&Value::String("Token expired".into())));
	assert!(!obj.contains_key("cause"));
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details object");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Auth".into())));
	let Some(Value::Object(inner)) = details.get("details") else {
		panic!("Expected inner details");
	};
	assert_eq!(inner.get("kind"), Some(&Value::String("TokenExpired".into())));
	assert!(!inner.contains_key("details"), "Unit variant should not have inner details key");

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_allowed());
	assert_eq!(parsed.message(), "Token expired");
	let details = parsed.not_allowed_details().unwrap();
	assert!(matches!(details, NotAllowedError::Auth(AuthError::TokenExpired)));
}

#[test]
fn test_error_snapshot_not_allowed_auth_invalid_role() {
	// Wire format:
	// {
	//   "code": -32002,
	//   "message": "Bad role",
	//   "kind": "NotAllowed",
	//   "details": {
	//     "kind": "Auth",
	//     "details": { "kind": "InvalidRole", "details": { "name": "admin" } }
	//   }
	// }
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
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Table not found",
	//   "kind": "NotFound",
	//   "details": { "kind": "Table", "details": { "name": "users" } }
	// }
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
	assert!(parsed.is_not_found());
	let details = parsed.not_found_details().unwrap();
	assert!(matches!(
		details,
		NotFoundError::Table { name } if name == "users"
	));
}

#[test]
fn test_error_snapshot_validation_parse() {
	// Wire format:
	// {
	//   "code": -32700,
	//   "message": "Parse error",
	//   "kind": "Validation",
	//   "details": { "kind": "Parse" }
	// }
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
	assert_eq!(parsed.validation_details(), Some(&ValidationError::Parse));
}

#[test]
fn test_error_snapshot_query_timed_out() {
	// Wire format:
	// {
	//   "code": -32004,
	//   "message": "Timed out",
	//   "kind": "Query",
	//   "details": { "kind": "TimedOut", "details": { "duration": { "secs": 5, "nanos": 0 } } }
	// }
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
	assert!(parsed.is_query());
	let details = parsed.query_details().unwrap();
	assert!(matches!(
		details,
		QueryError::TimedOut { duration } if *duration == Duration::from_secs(5)
	));
}

#[test]
fn test_error_snapshot_already_exists_record() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Record exists",
	//   "kind": "AlreadyExists",
	//   "details": { "kind": "Record", "details": { "id": "users:123" } }
	// }
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
	assert!(parsed.is_already_exists());
	let details = parsed.already_exists_details().unwrap();
	assert!(matches!(
		details,
		AlreadyExistsError::Record { id } if id == "users:123"
	));
}

#[test]
fn test_error_snapshot_not_allowed_method() {
	// Wire format:
	// {
	//   "code": -32602,
	//   "message": "Method blocked",
	//   "kind": "NotAllowed",
	//   "details": { "kind": "Method", "details": { "name": "begin" } }
	// }
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
fn test_error_snapshot_internal_no_details() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Unexpected",
	//   "kind": "Internal"
	// }
	let err = Error::internal("Unexpected".to_string());
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Internal".into())));
	assert_eq!(obj.get("message"), Some(&Value::String("Unexpected".into())));
	assert!(!obj.contains_key("details"), "No details should be present");
	assert!(!obj.contains_key("cause"), "No cause field should exist");

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_internal());
	assert_eq!(parsed.message(), "Unexpected");
	assert!(matches!(parsed.details(), ErrorDetails::Internal));
}

#[test]
fn test_error_snapshot_thrown_no_details() {
	// Wire format:
	// {
	//   "code": -32006,
	//   "message": "custom error",
	//   "kind": "Thrown"
	// }
	let err = Error::thrown("custom error".to_string());
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Thrown".into())));
	assert_eq!(obj.get("message"), Some(&Value::String("custom error".into())));
	assert!(!obj.contains_key("details"));
	assert!(!obj.contains_key("cause"));

	// Round-trip
	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_thrown());
	assert_eq!(parsed.message(), "custom error");
	assert!(matches!(parsed.details(), ErrorDetails::Thrown));
}

#[test]
fn test_error_snapshot_not_allowed_scripting_unit() {
	// Wire format:
	// {
	//   "code": -32602,
	//   "message": "Scripting not allowed",
	//   "kind": "NotAllowed",
	//   "details": { "kind": "Scripting" }
	// }
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
	assert_eq!(details, &NotAllowedError::Scripting);
}

// -----------------------------------------------------------------------------
// Comprehensive error serialization + deserialization tests
// Each test documents the exact JSON wire format in comments.
// -----------------------------------------------------------------------------

#[test]
fn test_error_wire_validation_parse() {
	// Wire format:
	// {
	//   "code": -32700,
	//   "message": "Failed to parse query",
	//   "kind": "Validation",
	//   "details": { "kind": "Parse" }
	// }
	let err = Error::validation("Failed to parse query".into(), ValidationError::Parse);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!("Expected object");
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32700))));
	assert_eq!(obj.get("message"), Some(&Value::String("Failed to parse query".into())));
	assert_eq!(obj.get("kind"), Some(&Value::String("Validation".into())));
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!("Expected details");
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Parse".into())));
	assert!(!details.contains_key("details"), "Unit variant has no inner details");

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_validation());
	assert_eq!(parsed.validation_details(), Some(&ValidationError::Parse));
	assert_eq!(parsed.message(), "Failed to parse query");
}

#[test]
fn test_error_wire_validation_invalid_params() {
	// Wire format:
	// {
	//   "code": -32603,
	//   "message": "Invalid parameters",
	//   "kind": "Validation",
	//   "details": { "kind": "InvalidParams" }
	// }
	let err = Error::validation("Invalid parameters".into(), ValidationError::InvalidParams);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32603))));
	assert_eq!(obj.get("kind"), Some(&Value::String("Validation".into())));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_validation());
	assert_eq!(parsed.validation_details(), Some(&ValidationError::InvalidParams));
}

#[test]
fn test_error_wire_validation_invalid_parameter_with_name() {
	// Wire format:
	// {
	//   "code": -32600,
	//   "message": "Invalid parameter 'limit'",
	//   "kind": "Validation",
	//   "details": { "kind": "InvalidParameter", "details": { "name": "limit" } }
	// }
	let err = Error::validation(
		"Invalid parameter 'limit'".into(),
		ValidationError::InvalidParameter {
			name: "limit".into(),
		},
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Validation".into())));
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!();
	};
	assert_eq!(details.get("kind"), Some(&Value::String("InvalidParameter".into())));
	let Some(Value::Object(inner)) = details.get("details") else {
		panic!();
	};
	assert_eq!(inner.get("name"), Some(&Value::String("limit".into())));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_validation());
	assert_eq!(
		parsed.validation_details(),
		Some(&ValidationError::InvalidParameter {
			name: "limit".into()
		})
	);
}

#[test]
fn test_error_wire_validation_no_details() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Something invalid",
	//   "kind": "Validation"
	// }
	let err = Error::validation("Something invalid".into(), None);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("Validation".into())));
	assert!(!obj.contains_key("details"), "No details when None");

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_validation());
	assert_eq!(parsed.validation_details(), None);
}

#[test]
fn test_error_wire_not_allowed_auth_session_expired() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Session has expired",
	//   "kind": "NotAllowed",
	//   "details": { "kind": "Auth", "details": { "kind": "SessionExpired" } }
	// }
	let err = Error::not_allowed("Session has expired".into(), AuthError::SessionExpired);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("NotAllowed".into())));
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!();
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Auth".into())));
	let Some(Value::Object(auth)) = details.get("details") else {
		panic!();
	};
	assert_eq!(auth.get("kind"), Some(&Value::String("SessionExpired".into())));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_allowed());
	assert!(matches!(
		parsed.not_allowed_details(),
		Some(NotAllowedError::Auth(AuthError::SessionExpired))
	));
}

#[test]
fn test_error_wire_not_allowed_auth_not_allowed_iam() {
	// Wire format:
	// {
	//   "code": -32002,
	//   "message": "Not enough permissions",
	//   "kind": "NotAllowed",
	//   "details": {
	//     "kind": "Auth",
	//     "details": {
	//       "kind": "NotAllowed",
	//       "details": { "actor": "user:john", "action": "edit", "resource": "table:secrets" }
	//     }
	//   }
	// }
	let err = Error::not_allowed(
		"Not enough permissions".into(),
		AuthError::NotAllowed {
			actor: "user:john".into(),
			action: "edit".into(),
			resource: "table:secrets".into(),
		},
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("kind"), Some(&Value::String("NotAllowed".into())));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_allowed());
	match parsed.not_allowed_details() {
		Some(NotAllowedError::Auth(AuthError::NotAllowed {
			actor,
			action,
			resource,
		})) => {
			assert_eq!(actor, "user:john");
			assert_eq!(action, "edit");
			assert_eq!(resource, "table:secrets");
		}
		other => panic!("Expected Auth::NotAllowed, got {other:?}"),
	}
}

#[test]
fn test_error_wire_not_allowed_function() {
	// Wire format:
	// {
	//   "code": -32602,
	//   "message": "Function not allowed",
	//   "kind": "NotAllowed",
	//   "details": { "kind": "Function", "details": { "name": "fn::custom" } }
	// }
	let err = Error::not_allowed(
		"Function not allowed".into(),
		NotAllowedError::Function {
			name: "fn::custom".into(),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_allowed());
	match parsed.not_allowed_details() {
		Some(NotAllowedError::Function {
			name,
		}) => assert_eq!(name, "fn::custom"),
		other => panic!("Expected Function, got {other:?}"),
	}
}

#[test]
fn test_error_wire_not_allowed_target() {
	// Wire format:
	// {
	//   "code": -32602,
	//   "message": "Net target not allowed",
	//   "kind": "NotAllowed",
	//   "details": { "kind": "Target", "details": { "name": "example.com" } }
	// }
	let err = Error::not_allowed(
		"Net target not allowed".into(),
		NotAllowedError::Target {
			name: "example.com".into(),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_allowed());
	match parsed.not_allowed_details() {
		Some(NotAllowedError::Target {
			name,
		}) => assert_eq!(name, "example.com"),
		other => panic!("Expected Target, got {other:?}"),
	}
}

#[test]
fn test_error_wire_not_allowed_no_details() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Not allowed",
	//   "kind": "NotAllowed"
	// }
	let err = Error::not_allowed("Not allowed".into(), None);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert!(!obj.contains_key("details"));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_allowed());
	assert_eq!(parsed.not_allowed_details(), None);
}

#[test]
fn test_error_wire_not_found_record() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Record not found",
	//   "kind": "NotFound",
	//   "details": { "kind": "Record", "details": { "id": "person:jane" } }
	// }
	let err = Error::not_found(
		"Record not found".into(),
		NotFoundError::Record {
			id: "person:jane".into(),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_found());
	match parsed.not_found_details() {
		Some(NotFoundError::Record {
			id,
		}) => assert_eq!(id, "person:jane"),
		other => panic!("Expected Record, got {other:?}"),
	}
}

#[test]
fn test_error_wire_not_found_namespace() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Namespace not found",
	//   "kind": "NotFound",
	//   "details": { "kind": "Namespace", "details": { "name": "test_ns" } }
	// }
	let err = Error::not_found(
		"Namespace not found".into(),
		NotFoundError::Namespace {
			name: "test_ns".into(),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_found());
	match parsed.not_found_details() {
		Some(NotFoundError::Namespace {
			name,
		}) => assert_eq!(name, "test_ns"),
		other => panic!("Expected Namespace, got {other:?}"),
	}
}

#[test]
fn test_error_wire_not_found_database() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Database not found",
	//   "kind": "NotFound",
	//   "details": { "kind": "Database", "details": { "name": "test_db" } }
	// }
	let err = Error::not_found(
		"Database not found".into(),
		NotFoundError::Database {
			name: "test_db".into(),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_found());
	match parsed.not_found_details() {
		Some(NotFoundError::Database {
			name,
		}) => assert_eq!(name, "test_db"),
		other => panic!("Expected Database, got {other:?}"),
	}
}

#[test]
fn test_error_wire_not_found_method() {
	// Wire format:
	// {
	//   "code": -32601,
	//   "message": "Method not found",
	//   "kind": "NotFound",
	//   "details": { "kind": "Method", "details": { "name": "unknown_method" } }
	// }
	let err = Error::not_found(
		"Method not found".into(),
		NotFoundError::Method {
			name: "unknown_method".into(),
		},
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32601))));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_found());
	match parsed.not_found_details() {
		Some(NotFoundError::Method {
			name,
		}) => assert_eq!(name, "unknown_method"),
		other => panic!("Expected Method, got {other:?}"),
	}
}

#[test]
fn test_error_wire_not_found_transaction() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Transaction not found",
	//   "kind": "NotFound",
	//   "details": { "kind": "Transaction" }
	// }
	let err = Error::not_found("Transaction not found".into(), NotFoundError::Transaction);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	let Some(Value::Object(details)) = obj.get("details") else {
		panic!();
	};
	assert_eq!(details.get("kind"), Some(&Value::String("Transaction".into())));
	assert!(!details.contains_key("details"), "Unit variant has no inner details");

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_found());
	assert_eq!(parsed.not_found_details(), Some(&NotFoundError::Transaction));
}

#[test]
fn test_error_wire_not_found_session() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Session not found",
	//   "kind": "NotFound",
	//   "details": { "kind": "Session", "details": { "id": "abc-123" } }
	// }
	let err = Error::not_found(
		"Session not found".into(),
		NotFoundError::Session {
			id: Some("abc-123".into()),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_not_found());
	match parsed.not_found_details() {
		Some(NotFoundError::Session {
			id,
		}) => assert_eq!(id, &Some("abc-123".to_string())),
		other => panic!("Expected Session, got {other:?}"),
	}
}

#[test]
fn test_error_wire_query_cancelled() {
	// Wire format:
	// {
	//   "code": -32005,
	//   "message": "Query was cancelled",
	//   "kind": "Query",
	//   "details": { "kind": "Cancelled" }
	// }
	let err = Error::query("Query was cancelled".into(), QueryError::Cancelled);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32005))));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_query());
	assert_eq!(parsed.query_details(), Some(&QueryError::Cancelled));
}

#[test]
fn test_error_wire_query_not_executed() {
	// Wire format:
	// {
	//   "code": -32003,
	//   "message": "Query not executed",
	//   "kind": "Query",
	//   "details": { "kind": "NotExecuted" }
	// }
	let err = Error::query("Query not executed".into(), QueryError::NotExecuted);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_query());
	assert_eq!(parsed.query_details(), Some(&QueryError::NotExecuted));
}

#[test]
fn test_error_wire_query_timed_out() {
	// Wire format:
	// {
	//   "code": -32004,
	//   "message": "Query timed out",
	//   "kind": "Query",
	//   "details": { "kind": "TimedOut", "details": { "duration": { "secs": 30, "nanos": 0 } } }
	// }
	use std::time::Duration;
	let err = Error::query(
		"Query timed out".into(),
		QueryError::TimedOut {
			duration: Duration::from_secs(30),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_query());
	match parsed.query_details() {
		Some(QueryError::TimedOut {
			duration,
		}) => assert_eq!(*duration, Duration::from_secs(30)),
		other => panic!("Expected TimedOut, got {other:?}"),
	}
}

#[test]
fn test_error_wire_already_exists_table() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Table already exists",
	//   "kind": "AlreadyExists",
	//   "details": { "kind": "Table", "details": { "name": "users" } }
	// }
	let err = Error::already_exists(
		"Table already exists".into(),
		AlreadyExistsError::Table {
			name: "users".into(),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_already_exists());
	match parsed.already_exists_details() {
		Some(AlreadyExistsError::Table {
			name,
		}) => assert_eq!(name, "users"),
		other => panic!("Expected Table, got {other:?}"),
	}
}

#[test]
fn test_error_wire_already_exists_namespace() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Namespace already exists",
	//   "kind": "AlreadyExists",
	//   "details": { "kind": "Namespace", "details": { "name": "prod" } }
	// }
	let err = Error::already_exists(
		"Namespace already exists".into(),
		AlreadyExistsError::Namespace {
			name: "prod".into(),
		},
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_already_exists());
	match parsed.already_exists_details() {
		Some(AlreadyExistsError::Namespace {
			name,
		}) => assert_eq!(name, "prod"),
		other => panic!("Expected Namespace, got {other:?}"),
	}
}

#[test]
fn test_error_wire_configuration_live_query() {
	// Wire format:
	// {
	//   "code": -32604,
	//   "message": "Live queries not supported",
	//   "kind": "Configuration",
	//   "details": { "kind": "LiveQueryNotSupported" }
	// }
	let err = Error::configuration(
		"Live queries not supported".into(),
		ConfigurationError::LiveQueryNotSupported,
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32604))));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_configuration());
	assert_eq!(
		parsed.configuration_details(),
		Some(&ConfigurationError::LiveQueryNotSupported)
	);
}

#[test]
fn test_error_wire_configuration_bad_graphql() {
	// Wire format:
	// {
	//   "code": -32606,
	//   "message": "Bad GraphQL config",
	//   "kind": "Configuration",
	//   "details": { "kind": "BadGraphqlConfig" }
	// }
	let err =
		Error::configuration("Bad GraphQL config".into(), ConfigurationError::BadGraphqlConfig);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_configuration());
	assert_eq!(
		parsed.configuration_details(),
		Some(&ConfigurationError::BadGraphqlConfig)
	);
}

#[test]
fn test_error_wire_serialization_error() {
	// Wire format:
	// {
	//   "code": -32007,
	//   "message": "Failed to serialize",
	//   "kind": "Serialization",
	//   "details": { "kind": "Serialization" }
	// }
	let err = Error::serialization(
		"Failed to serialize".into(),
		SerializationError::Serialization,
	);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_serialization());
	assert_eq!(
		parsed.serialization_details(),
		Some(&SerializationError::Serialization)
	);
}

#[test]
fn test_error_wire_deserialization_error() {
	// Wire format:
	// {
	//   "code": -32008,
	//   "message": "Failed to deserialize",
	//   "kind": "Serialization",
	//   "details": { "kind": "Deserialization" }
	// }
	let err = Error::serialization(
		"Failed to deserialize".into(),
		SerializationError::Deserialization,
	);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32008))));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_serialization());
	assert_eq!(
		parsed.serialization_details(),
		Some(&SerializationError::Deserialization)
	);
}

#[test]
fn test_error_wire_connection_uninitialised() {
	// Wire format:
	// {
	//   "code": -32001,
	//   "message": "Connection not initialised",
	//   "kind": "Connection",
	//   "details": { "kind": "Uninitialised" }
	// }
	let err =
		Error::connection("Connection not initialised".into(), ConnectionError::Uninitialised);
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32001))));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_connection());
	assert_eq!(
		parsed.connection_details(),
		Some(&ConnectionError::Uninitialised)
	);
}

#[test]
fn test_error_wire_connection_already_connected() {
	// Wire format:
	// {
	//   "code": -32001,
	//   "message": "Already connected",
	//   "kind": "Connection",
	//   "details": { "kind": "AlreadyConnected" }
	// }
	let err = Error::connection("Already connected".into(), ConnectionError::AlreadyConnected);
	let val = err.into_value();

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_connection());
	assert_eq!(
		parsed.connection_details(),
		Some(&ConnectionError::AlreadyConnected)
	);
}

#[test]
fn test_error_wire_thrown() {
	// Wire format:
	// {
	//   "code": -32006,
	//   "message": "An error occurred: user validation failed",
	//   "kind": "Thrown"
	// }
	let err = Error::thrown("An error occurred: user validation failed".into());
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32006))));
	assert_eq!(obj.get("kind"), Some(&Value::String("Thrown".into())));
	assert!(!obj.contains_key("details"));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_thrown());
	assert_eq!(parsed.message(), "An error occurred: user validation failed");
}

#[test]
fn test_error_wire_internal() {
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "An unexpected error occurred",
	//   "kind": "Internal"
	// }
	let err = Error::internal("An unexpected error occurred".into());
	let val = err.into_value();

	let Value::Object(ref obj) = val else {
		panic!();
	};
	assert_eq!(obj.get("code"), Some(&Value::Number(Number::Int(-32000))));
	assert_eq!(obj.get("kind"), Some(&Value::String("Internal".into())));
	assert!(!obj.contains_key("details"));

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_internal());
	assert_eq!(parsed.message(), "An unexpected error occurred");
}

// -----------------------------------------------------------------------------
// Forward/backward compatibility
// -----------------------------------------------------------------------------

#[test]
fn test_error_wire_unknown_kind_falls_back_to_internal() {
	// Simulates a future server sending a kind the client doesn't know about.
	// Wire format:
	// {
	//   "kind": "RateLimit",
	//   "message": "Too many requests"
	// }
	let mut obj = Object::new();
	obj.insert("kind", Value::String("RateLimit".into()));
	obj.insert("message", Value::String("Too many requests".into()));
	let val = Value::Object(obj);

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_internal()); // Unknown kinds fall back to Internal
	assert_eq!(parsed.message(), "Too many requests");
}

#[test]
fn test_error_wire_missing_kind_defaults_to_internal() {
	// Old server format without "kind" field.
	// Wire format:
	// {
	//   "code": -32000,
	//   "message": "Legacy error"
	// }
	let mut obj = Object::new();
	obj.insert("code", Value::Number(Number::Int(-32000)));
	obj.insert("message", Value::String("Legacy error".into()));
	let val = Value::Object(obj);

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_internal());
	assert_eq!(parsed.message(), "Legacy error");
}

#[test]
fn test_error_wire_missing_code_defaults_to_internal_code() {
	// Wire format without "code" field — defaults to -32000.
	// {
	//   "kind": "Thrown",
	//   "message": "Custom error"
	// }
	let mut obj = Object::new();
	obj.insert("kind", Value::String("Thrown".into()));
	obj.insert("message", Value::String("Custom error".into()));
	let val = Value::Object(obj);

	let parsed = Error::from_value(val).unwrap();
	assert!(parsed.is_thrown());
	assert_eq!(parsed.message(), "Custom error");
}

#[test]
fn test_error_wire_details_with_unknown_inner_kind() {
	// Details contain an unknown inner variant — the ErrorDetails manual
	// SurrealValue impl handles this by falling back to Internal when the
	// detail variant's kind doesn't match any known variant for that ErrorKind.
	// Wire format:
	// {
	//   "kind": "NotFound",
	//   "message": "Not found",
	//   "details": { "kind": "FutureResource", "details": { "id": "xyz" } }
	// }
	let err = Error::from_parts(
		"Not found".into(),
		Some("NotFound"),
		Some({
			let mut details_obj = Object::new();
			details_obj.insert("kind", Value::String("FutureResource".into()));
			let mut inner = Object::new();
			inner.insert("id", Value::String("xyz".into()));
			details_obj.insert("details", Value::Object(inner));
			Value::Object(details_obj)
		}),
	);
	// from_parts gracefully falls back when details can't be parsed
	assert!(err.is_not_found());
	assert_eq!(err.not_found_details(), None);
}

// -----------------------------------------------------------------------------
// Utility method coverage
// -----------------------------------------------------------------------------

#[test]
fn test_error_utility_methods() {
	let validation = Error::validation("test".into(), None);
	assert!(validation.is_validation());
	assert!(!validation.is_not_allowed());
	assert!(!validation.is_internal());
	assert_eq!(validation.kind_str(), "Validation");

	let not_allowed = Error::not_allowed("test".into(), None);
	assert!(not_allowed.is_not_allowed());
	assert!(!not_allowed.is_validation());
	assert_eq!(not_allowed.kind_str(), "NotAllowed");

	let not_found = Error::not_found("test".into(), None);
	assert!(not_found.is_not_found());
	assert_eq!(not_found.kind_str(), "NotFound");

	let already_exists = Error::already_exists("test".into(), None);
	assert!(already_exists.is_already_exists());
	assert_eq!(already_exists.kind_str(), "AlreadyExists");

	let query = Error::query("test".into(), None);
	assert!(query.is_query());
	assert_eq!(query.kind_str(), "Query");

	let config = Error::configuration("test".into(), None);
	assert!(config.is_configuration());
	assert_eq!(config.kind_str(), "Configuration");

	let serialization = Error::serialization("test".into(), None);
	assert!(serialization.is_serialization());
	assert_eq!(serialization.kind_str(), "Serialization");

	let connection = Error::connection("test".into(), None);
	assert!(connection.is_connection());
	assert_eq!(connection.kind_str(), "Connection");

	let thrown = Error::thrown("test".into());
	assert!(thrown.is_thrown());
	assert_eq!(thrown.kind_str(), "Thrown");

	let internal = Error::internal("test".into());
	assert!(internal.is_internal());
	assert_eq!(internal.kind_str(), "Internal");
}

#[test]
fn test_error_from_parts_with_details() {
	// Simulates how dbs/response.rs reconstructs errors from query results
	let mut details_obj = Object::new();
	details_obj.insert("kind", Value::String("Table".into()));
	let mut inner = Object::new();
	inner.insert("name", Value::String("users".into()));
	details_obj.insert("details", Value::Object(inner));

	let err = Error::from_parts(
		"Table not found".into(),
		Some("NotFound"),
		Some(Value::Object(details_obj)),
	);
	assert!(err.is_not_found());
	assert_eq!(err.message(), "Table not found");
	match err.not_found_details() {
		Some(NotFoundError::Table {
			name,
		}) => assert_eq!(name, "users"),
		other => panic!("Expected Table, got {other:?}"),
	}
}

#[test]
fn test_error_from_parts_without_details() {
	let err = Error::from_parts("Internal error".into(), Some("Internal"), None);
	assert!(err.is_internal());
	assert_eq!(err.message(), "Internal error");
}

#[test]
fn test_error_from_parts_unknown_kind() {
	let err = Error::from_parts("Unknown".into(), Some("FutureKind"), None);
	assert!(err.is_internal()); // Unknown falls back to Internal
}

#[test]
fn test_error_from_parts_no_kind() {
	let err = Error::from_parts("No kind".into(), None, None);
	assert!(err.is_internal()); // Missing kind defaults to Internal
}
