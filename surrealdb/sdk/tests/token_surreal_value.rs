use surrealdb::opt::auth::{AccessToken, RefreshToken, Token};
use surrealdb_types::{Object, SurrealValue, Value};

// Helper function to create a Token with both access and refresh tokens
fn create_token_with_refresh(access_str: &str, refresh_str: &str) -> Token {
	Token::from((AccessToken::from(access_str), RefreshToken::from(refresh_str)))
}

// Helper function to create a Token with only access token
fn create_token_without_refresh(access_str: &str) -> Token {
	Token::from(access_str)
}

#[test]
fn test_token_into_value_with_refresh() {
	let token = create_token_with_refresh("access_token_123", "refresh_token_456");
	let value = token.into_value();

	match value {
		Value::Object(obj) => {
			assert!(obj.get("access").is_some());
			assert!(obj.get("refresh").is_some());

			// Check that access token is serialized as a string
			if let Some(Value::String(access_str)) = obj.get("access") {
				assert_eq!(access_str, "access_token_123");
			} else {
				panic!("Access token should be serialized as a string");
			}

			// Check that refresh token is serialized as a string
			if let Some(Value::String(refresh_str)) = obj.get("refresh") {
				assert_eq!(refresh_str, "refresh_token_456");
			} else {
				panic!("Refresh token should be serialized as a string");
			}
		}
		_ => panic!("Token should be serialized as an object"),
	}
}

#[test]
fn test_token_into_value_without_refresh() {
	let token = create_token_without_refresh("access_token_123");
	let value = token.into_value();

	match value {
		Value::String(token) => {
			assert_eq!(token, "access_token_123");
		}
		_ => panic!("Token should be serialized as an object"),
	}
}

#[test]
fn test_token_from_value_with_refresh() {
	let mut obj = Object::new();
	obj.insert("access".to_string(), Value::String("access_token_123".to_string()));
	obj.insert("refresh".to_string(), Value::String("refresh_token_456".to_string()));
	let value = Value::Object(obj);

	let token = Token::from_value(value).unwrap();

	assert_eq!(token.access.as_insecure_token(), "access_token_123");
	assert!(token.refresh.is_some());
	assert_eq!(token.refresh.unwrap().as_insecure_token(), "refresh_token_456");
}

#[test]
fn test_token_from_value_legacy_string_format() {
	// Test the legacy format where Token can be created from a string
	let value = Value::String("legacy_token_123".to_string());

	let token = Token::from_value(value).unwrap();

	assert_eq!(token.access.as_insecure_token(), "legacy_token_123");
	assert!(token.refresh.is_none());
}

#[test]
fn test_token_from_value_missing_access_field() {
	let mut obj = Object::new();
	obj.insert("refresh".to_string(), Value::String("refresh_token_456".to_string()));
	let value = Value::Object(obj);

	let result: Result<Token, _> = Token::from_value(value);
	assert!(result.is_err());
}

#[test]
fn test_token_from_value_invalid_access_type() {
	let mut obj = Object::new();
	obj.insert("access".to_string(), Value::Number(surrealdb_types::Number::Int(123)));
	obj.insert("refresh".to_string(), Value::String("refresh_token_456".to_string()));
	let value = Value::Object(obj);

	let result: Result<Token, _> = Token::from_value(value);
	assert!(result.is_err());
}

#[test]
fn test_token_from_value_invalid_refresh_type() {
	let mut obj = Object::new();
	obj.insert("access".to_string(), Value::String("access_token_123".to_string()));
	obj.insert("refresh".to_string(), Value::Number(surrealdb_types::Number::Int(123)));
	let value = Value::Object(obj);

	let result: Result<Token, _> = Token::from_value(value);
	assert!(result.is_err());
}

#[test]
fn test_token_roundtrip_with_refresh() {
	let original_token = || create_token_with_refresh("access_token_123", "refresh_token_456");

	let value = original_token().into_value();
	let deserialized_token = Token::from_value(value).unwrap();

	assert_eq!(
		original_token().access.as_insecure_token(),
		deserialized_token.access.as_insecure_token()
	);
	assert!(original_token().refresh.is_some());
	assert!(deserialized_token.refresh.is_some());
	assert_eq!(
		original_token().refresh.as_ref().unwrap().as_insecure_token(),
		deserialized_token.refresh.unwrap().as_insecure_token()
	);
}

#[test]
fn test_token_roundtrip_without_refresh() {
	let original_token = || create_token_without_refresh("access_token_123");

	let value = original_token().into_value();
	let deserialized_token = Token::from_value(value).unwrap();

	assert_eq!(
		original_token().access.as_insecure_token(),
		deserialized_token.access.as_insecure_token()
	);
	assert!(original_token().refresh.is_none());
	assert!(deserialized_token.refresh.is_none());
}

#[test]
fn test_token_roundtrip_legacy_string() {
	let original_token = || Token::from("legacy_token_123".to_string());

	let value = original_token().into_value();
	let deserialized_token = Token::from_value(value).unwrap();

	assert_eq!(
		original_token().access.as_insecure_token(),
		deserialized_token.access.as_insecure_token()
	);
	assert!(original_token().refresh.is_none());
	assert!(deserialized_token.refresh.is_none());
}

#[test]
fn test_token_is_value() {
	let token = create_token_without_refresh("access_token_123");

	let value = token.into_value();
	assert!(Token::is_value(&value));
}

#[test]
fn test_token_is_value_with_invalid_type() {
	let value = Value::Number(surrealdb_types::Number::Int(123));
	assert!(!Token::is_value(&value));
}

#[test]
fn test_token_from_string_implementations() {
	// Test the From implementations for Token
	let token_from_string = Token::from("string_token".to_string());
	assert_eq!(token_from_string.access.as_insecure_token(), "string_token");
	assert!(token_from_string.refresh.is_none());

	let string_ref = "string_ref_token".to_string();
	let token_from_string_ref = Token::from(&string_ref);
	assert_eq!(token_from_string_ref.access.as_insecure_token(), "string_ref_token");
	assert!(token_from_string_ref.refresh.is_none());

	let token_from_str = Token::from("str_token");
	assert_eq!(token_from_str.access.as_insecure_token(), "str_token");
	assert!(token_from_str.refresh.is_none());
}

#[test]
fn test_token_debug_implementation() {
	let token = create_token_with_refresh("secret_token", "secret_refresh");

	let debug_output = format!("{:?}", token);
	assert!(debug_output.contains("Token"));
	assert!(debug_output.contains("REDACTED"));
	assert!(!debug_output.contains("secret_token"));
	assert!(!debug_output.contains("secret_refresh"));
}

#[test]
fn test_token_serialize_deserialize() {
	use serde_json;

	let token = create_token_with_refresh("access_token_123", "refresh_token_456");

	// Test JSON serialization
	let json = serde_json::to_string(&token).unwrap();
	let deserialized_token: Token = serde_json::from_str(&json).unwrap();

	assert_eq!(token.access.as_insecure_token(), deserialized_token.access.as_insecure_token());
	assert!(token.refresh.is_some());
	assert!(deserialized_token.refresh.is_some());
	assert_eq!(
		token.refresh.as_ref().unwrap().as_insecure_token(),
		deserialized_token.refresh.unwrap().as_insecure_token()
	);
}

#[test]
fn test_token_edge_case_empty_strings() {
	let token = create_token_with_refresh("", "");

	let value = token.into_value();
	let deserialized_token: Token = Token::from_value(value).unwrap();

	assert_eq!(deserialized_token.access.as_insecure_token(), "");
	assert!(deserialized_token.refresh.is_some());
	assert_eq!(deserialized_token.refresh.unwrap().as_insecure_token(), "");
}

#[test]
fn test_token_edge_case_very_long_strings() {
	let long_string = "a".repeat(10000);
	let token = create_token_with_refresh(&long_string, &long_string);

	let value = token.into_value();
	let deserialized_token = Token::from_value(value).unwrap();

	assert_eq!(deserialized_token.access.as_insecure_token(), long_string);
	assert!(deserialized_token.refresh.is_some());
	assert_eq!(deserialized_token.refresh.unwrap().as_insecure_token(), long_string);
}

#[test]
fn test_token_edge_case_special_characters() {
	let special_string = "!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
	let token = create_token_without_refresh(special_string);

	let value = token.into_value();
	let deserialized_token = Token::from_value(value).unwrap();

	assert_eq!(deserialized_token.access.as_insecure_token(), special_string);
	assert!(deserialized_token.refresh.is_none());
}
