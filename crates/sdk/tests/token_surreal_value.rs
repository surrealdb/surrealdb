use surrealdb::opt::auth::{AccessToken, RefreshToken, Token};
use surrealdb_types::{Kind, Object, SurrealValue, Value, kind};

// Helper function to create a Token with both access and refresh tokens
fn create_token_with_refresh(access_str: &str, refresh_str: &str) -> Token<AccessToken> {
	let access_token = AccessToken::from_value(Value::String(access_str.to_string())).unwrap();
	let refresh_token = RefreshToken::from_value(Value::String(refresh_str.to_string())).unwrap();

	// Create a token by serializing and deserializing to work around non_exhaustive
	let mut obj = Object::new();
	obj.insert("token".to_string(), access_token.into_value());
	obj.insert("refresh".to_string(), refresh_token.into_value());
	let value = Value::Object(obj);

	Token::from_value(value).unwrap()
}

// Helper function to create a Token with only access token
fn create_token_without_refresh(access_str: &str) -> Token<AccessToken> {
	let access_token = AccessToken::from_value(Value::String(access_str.to_string())).unwrap();

	// Create a token by serializing and deserializing to work around non_exhaustive
	let mut obj = Object::new();
	obj.insert("token".to_string(), access_token.into_value());
	obj.insert("refresh".to_string(), Value::None);
	let value = Value::Object(obj);

	Token::from_value(value).unwrap()
}

#[test]
fn test_token_kind_of() {
	let kind = Token::<AccessToken>::kind_of();
	// The kind should be an object with access and refresh fields
	// The actual kind is Literal(Object({"token": Any, "refresh": Any}))
	match kind {
		Kind::Literal(literal) => {
			// This is expected for Token<T> - it's a literal object kind
			assert!(matches!(literal, surrealdb_types::KindLiteral::Object(_)));
		}
		_ => {
			panic!("Expected Kind::Literal with Object, got {:?}", kind);
		}
	}
}

#[test]
fn test_token_into_value_with_refresh() {
	let token = create_token_with_refresh("access_token_123", "refresh_token_456");
	let value = token.into_value();

	match value {
		Value::Object(obj) => {
			assert!(obj.get("token").is_some());
			assert!(obj.get("refresh").is_some());

			// Check that access token is serialized as a string
			if let Some(Value::String(access_str)) = obj.get("token") {
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
		Value::Object(obj) => {
			assert!(obj.get("token").is_some());
			assert!(obj.get("refresh").is_some());

			// Check that access token is serialized as a string
			if let Some(Value::String(access_str)) = obj.get("token") {
				assert_eq!(access_str, "access_token_123");
			} else {
				panic!("Access token should be serialized as a string");
			}

			// Check that refresh token is None
			if let Some(Value::None) = obj.get("refresh") {
				// This is expected for None refresh token
			} else {
				panic!("Refresh token should be None when not provided");
			}
		}
		_ => panic!("Token should be serialized as an object"),
	}
}

#[test]
fn test_token_from_value_with_refresh() {
	let mut obj = Object::new();
	obj.insert("token".to_string(), Value::String("access_token_123".to_string()));
	obj.insert("refresh".to_string(), Value::String("refresh_token_456".to_string()));
	let value = Value::Object(obj);

	let token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(token.access.as_insecure_token(), "access_token_123");
	assert!(token.refresh.is_some());
	assert_eq!(token.refresh.unwrap().as_insecure_token(), "refresh_token_456");
}

#[test]
fn test_token_from_value_without_refresh() {
	let mut obj = Object::new();
	obj.insert("token".to_string(), Value::String("access_token_123".to_string()));
	obj.insert("refresh".to_string(), Value::None);
	let value = Value::Object(obj);

	let token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(token.access.as_insecure_token(), "access_token_123");
	assert!(token.refresh.is_none());
}

#[test]
fn test_token_from_value_legacy_string_format() {
	// Test the legacy format where Token can be created from a string
	let value = Value::String("legacy_token_123".to_string());

	let token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(token.access.as_insecure_token(), "legacy_token_123");
	assert!(token.refresh.is_none());
}

#[test]
fn test_token_from_value_missing_access_field() {
	let mut obj = Object::new();
	obj.insert("refresh".to_string(), Value::String("refresh_token_456".to_string()));
	let value = Value::Object(obj);

	let result: Result<Token<AccessToken>, _> = Token::from_value(value);
	assert!(result.is_err());
}

#[test]
fn test_token_from_value_invalid_access_type() {
	let mut obj = Object::new();
	obj.insert("token".to_string(), Value::Number(surrealdb_types::Number::Int(123)));
	obj.insert("refresh".to_string(), Value::String("refresh_token_456".to_string()));
	let value = Value::Object(obj);

	let result: Result<Token<AccessToken>, _> = Token::from_value(value);
	assert!(result.is_err());
}

#[test]
fn test_token_from_value_invalid_refresh_type() {
	let mut obj = Object::new();
	obj.insert("token".to_string(), Value::String("access_token_123".to_string()));
	obj.insert("refresh".to_string(), Value::Number(surrealdb_types::Number::Int(123)));
	let value = Value::Object(obj);

	let result: Result<Token<AccessToken>, _> = Token::from_value(value);
	assert!(result.is_err());
}

#[test]
fn test_token_roundtrip_with_refresh() {
	let original_token = create_token_with_refresh("access_token_123", "refresh_token_456");

	let value = original_token.clone().into_value();
	let deserialized_token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(
		original_token.access.as_insecure_token(),
		deserialized_token.access.as_insecure_token()
	);
	assert!(original_token.refresh.is_some());
	assert!(deserialized_token.refresh.is_some());
	assert_eq!(
		original_token.refresh.as_ref().unwrap().as_insecure_token(),
		deserialized_token.refresh.unwrap().as_insecure_token()
	);
}

#[test]
fn test_token_roundtrip_without_refresh() {
	let original_token = create_token_without_refresh("access_token_123");

	let value = original_token.clone().into_value();
	let deserialized_token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(
		original_token.access.as_insecure_token(),
		deserialized_token.access.as_insecure_token()
	);
	assert!(original_token.refresh.is_none());
	assert!(deserialized_token.refresh.is_none());
}

#[test]
fn test_token_roundtrip_legacy_string() {
	let original_token = Token::from("legacy_token_123".to_string());

	let value = original_token.clone().into_value();
	let deserialized_token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(
		original_token.access.as_insecure_token(),
		deserialized_token.access.as_insecure_token()
	);
	assert!(original_token.refresh.is_none());
	assert!(deserialized_token.refresh.is_none());
}

#[test]
fn test_token_is_value() {
	let token = create_token_without_refresh("access_token_123");

	let value = token.into_value();
	assert!(Token::<AccessToken>::is_value(&value));
}

#[test]
fn test_token_is_value_with_string() {
	let value = Value::String("legacy_token_123".to_string());
	// Token's is_value method returns false for string values because
	// the kind expects an object, but from_value can still handle strings (legacy format)
	assert!(!Token::<AccessToken>::is_value(&value));

	// However, from_value should still work with string values
	let token: Token<AccessToken> = Token::from_value(value).unwrap();
	assert_eq!(token.access.as_insecure_token(), "legacy_token_123");
	assert!(token.refresh.is_none());
}

#[test]
fn test_token_is_value_with_invalid_type() {
	let value = Value::Number(surrealdb_types::Number::Int(123));
	assert!(!Token::<AccessToken>::is_value(&value));
}

#[test]
fn test_token_with_custom_type() {
	// Test Token<T> with a custom type that implements SurrealValue
	#[derive(Debug, Clone, PartialEq)]
	struct CustomToken(String);

	impl SurrealValue for CustomToken {
		fn kind_of() -> Kind {
			kind!(string)
		}

		fn into_value(self) -> Value {
			Value::String(self.0)
		}

		fn from_value(value: Value) -> surrealdb_types::anyhow::Result<Self> {
			match value {
				Value::String(s) => Ok(CustomToken(s)),
				_ => Err(surrealdb_types::anyhow::anyhow!("Expected string")),
			}
		}
	}

	let custom_token = CustomToken("custom_token_123".to_string());

	// Create a token with custom type by serializing and deserializing
	let mut obj = Object::new();
	obj.insert("token".to_string(), custom_token.clone().into_value());
	obj.insert("refresh".to_string(), Value::None);
	let value = Value::Object(obj);

	let token: Token<CustomToken> = Token::from_value(value).unwrap();
	let deserialized_value = token.clone().into_value();
	let deserialized_token: Token<CustomToken> = Token::from_value(deserialized_value).unwrap();

	assert_eq!(token.access, deserialized_token.access);
	assert!(token.refresh.is_none());
	assert!(deserialized_token.refresh.is_none());
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
fn test_token_clone() {
	let original_token = create_token_with_refresh("access_token_123", "refresh_token_456");

	let cloned_token = original_token.clone();

	assert_eq!(original_token.access.as_insecure_token(), cloned_token.access.as_insecure_token());
	assert!(original_token.refresh.is_some());
	assert!(cloned_token.refresh.is_some());
	assert_eq!(
		original_token.refresh.as_ref().unwrap().as_insecure_token(),
		cloned_token.refresh.unwrap().as_insecure_token()
	);
}

#[test]
fn test_token_serialize_deserialize() {
	use serde_json;

	let token = create_token_with_refresh("access_token_123", "refresh_token_456");

	// Test JSON serialization
	let json = serde_json::to_string(&token).unwrap();
	let deserialized_token: Token<AccessToken> = serde_json::from_str(&json).unwrap();

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
	let deserialized_token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(deserialized_token.access.as_insecure_token(), "");
	assert!(deserialized_token.refresh.is_some());
	assert_eq!(deserialized_token.refresh.unwrap().as_insecure_token(), "");
}

#[test]
fn test_token_edge_case_very_long_strings() {
	let long_string = "a".repeat(10000);
	let token = create_token_with_refresh(&long_string, &long_string);

	let value = token.into_value();
	let deserialized_token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(deserialized_token.access.as_insecure_token(), long_string);
	assert!(deserialized_token.refresh.is_some());
	assert_eq!(deserialized_token.refresh.unwrap().as_insecure_token(), long_string);
}

#[test]
fn test_token_edge_case_special_characters() {
	let special_string = "!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
	let token = create_token_without_refresh(special_string);

	let value = token.into_value();
	let deserialized_token: Token<AccessToken> = Token::from_value(value).unwrap();

	assert_eq!(deserialized_token.access.as_insecure_token(), special_string);
	assert!(deserialized_token.refresh.is_none());
}
