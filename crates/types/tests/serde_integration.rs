use serde::{Deserialize, Serialize};
use surrealdb_types::{Object, SurrealValue, Value};

// Test that SurrealValue and serde work together with matching attributes
#[derive(SurrealValue, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct PersonWithSerde {
	#[surreal(rename = "full_name")]
	#[serde(rename = "full_name")]
	name: String,
	age: i64,
}

#[derive(SurrealValue, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct AddressWithSerde {
	street: String,
	city: String,
}

#[derive(SurrealValue, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct PersonWithFlatten {
	name: String,
	#[surreal(flatten)]
	#[serde(flatten)]
	address: AddressWithSerde,
}

#[derive(SurrealValue, Serialize, Deserialize, Debug, PartialEq, Clone)]
struct ComplexPerson {
	#[surreal(rename = "full_name")]
	#[serde(rename = "full_name")]
	name: String,
	age: i64,
	#[surreal(flatten)]
	#[serde(flatten)]
	address: AddressWithSerde,
}

#[test]
fn test_serde_rename_compatibility() {
	let person = PersonWithSerde {
		name: "Alice".to_string(),
		age: 30,
	};

	// Test SurrealValue conversion
	let surreal_value = person.clone().into_value();
	if let Value::Object(obj) = &surreal_value {
		assert!(obj.get("full_name").is_some());
		assert!(obj.get("name").is_none());
	}

	// Test serde JSON serialization
	let json = serde_json::to_string(&person).unwrap();
	assert!(json.contains("full_name"));
	assert!(!json.contains("\"name\""));

	// Test that both use the same field names
	let json_value: serde_json::Value = serde_json::from_str(&json).unwrap();
	assert_eq!(json_value["full_name"], "Alice");
	assert_eq!(json_value["age"], 30);

	// Test deserialization
	let deserialized: PersonWithSerde = serde_json::from_str(&json).unwrap();
	assert_eq!(deserialized, person);

	// Test SurrealValue round-trip
	let surreal_converted = PersonWithSerde::from_value(surreal_value).unwrap();
	assert_eq!(surreal_converted, person);
}

#[test]
fn test_serde_flatten_compatibility() {
	let person = PersonWithFlatten {
		name: "Bob".to_string(),
		address: AddressWithSerde {
			street: "123 Main St".to_string(),
			city: "Anytown".to_string(),
		},
	};

	// Test SurrealValue conversion with flatten
	let surreal_value = person.clone().into_value();
	if let Value::Object(obj) = &surreal_value {
		assert!(obj.get("name").is_some());
		assert!(obj.get("street").is_some());
		assert!(obj.get("city").is_some());
		assert!(obj.get("address").is_none());
	}

	// Test serde JSON serialization with flatten
	let json = serde_json::to_string(&person).unwrap();
	assert!(json.contains("\"name\""));
	assert!(json.contains("\"street\""));
	assert!(json.contains("\"city\""));
	assert!(!json.contains("\"address\""));

	// Verify the JSON structure matches
	let json_value: serde_json::Value = serde_json::from_str(&json).unwrap();
	assert_eq!(json_value["name"], "Bob");
	assert_eq!(json_value["street"], "123 Main St");
	assert_eq!(json_value["city"], "Anytown");

	// Test deserialization
	let deserialized: PersonWithFlatten = serde_json::from_str(&json).unwrap();
	assert_eq!(deserialized, person);

	// Test SurrealValue round-trip
	let surreal_converted = PersonWithFlatten::from_value(surreal_value).unwrap();
	assert_eq!(surreal_converted, person);
}

#[test]
fn test_complex_serde_surreal_compatibility() {
	let person = ComplexPerson {
		name: "Charlie".to_string(),
		age: 35,
		address: AddressWithSerde {
			street: "456 Oak Ave".to_string(),
			city: "Springfield".to_string(),
		},
	};

	// Test SurrealValue conversion
	let surreal_value = person.clone().into_value();
	if let Value::Object(obj) = &surreal_value {
		assert!(obj.get("full_name").is_some());
		assert!(obj.get("age").is_some());
		assert!(obj.get("street").is_some());
		assert!(obj.get("city").is_some());
		assert!(obj.get("name").is_none());
		assert!(obj.get("address").is_none());
	}

	// Test serde JSON serialization
	let json = serde_json::to_string(&person).unwrap();

	// Verify JSON structure
	let json_value: serde_json::Value = serde_json::from_str(&json).unwrap();
	assert_eq!(json_value["full_name"], "Charlie");
	assert_eq!(json_value["age"], 35);
	assert_eq!(json_value["street"], "456 Oak Ave");
	assert_eq!(json_value["city"], "Springfield");

	// Test round-trips
	let deserialized: ComplexPerson = serde_json::from_str(&json).unwrap();
	assert_eq!(deserialized, person);

	let surreal_converted = ComplexPerson::from_value(surreal_value).unwrap();
	assert_eq!(surreal_converted, person);
}

#[test]
fn test_interoperability_json_to_surreal() {
	// Create a JSON object
	let json = r#"{
        "full_name": "Diana",
        "age": 40,
        "street": "789 Pine Rd",
        "city": "Riverside"
    }"#;

	// Deserialize from JSON using serde
	let person: ComplexPerson = serde_json::from_str(json).unwrap();

	// Convert to SurrealValue
	let surreal_value = person.clone().into_value();

	// Convert back from SurrealValue
	let from_surreal = ComplexPerson::from_value(surreal_value.clone()).unwrap();
	assert_eq!(from_surreal, person);

	// Serialize back to JSON
	let json_again = serde_json::to_string(&from_surreal).unwrap();
	let json_value: serde_json::Value = serde_json::from_str(&json_again).unwrap();
	let original_json_value: serde_json::Value = serde_json::from_str(json).unwrap();

	// The JSON should be equivalent (though order might differ)
	assert_eq!(json_value["full_name"], original_json_value["full_name"]);
	assert_eq!(json_value["age"], original_json_value["age"]);
	assert_eq!(json_value["street"], original_json_value["street"]);
	assert_eq!(json_value["city"], original_json_value["city"]);
}

#[test]
fn test_surreal_to_json_value_compatibility() {
	// Create a SurrealDB Object manually
	let mut obj = Object::new();
	obj.insert("full_name".to_string(), Value::String("Eve".to_string()));
	obj.insert("age".to_string(), Value::from_t(45i64));
	obj.insert("street".to_string(), Value::String("321 Elm St".to_string()));
	obj.insert("city".to_string(), Value::String("Metropolis".to_string()));
	let surreal_value = Value::Object(obj);

	// Convert from SurrealValue to our type
	let person = ComplexPerson::from_value(surreal_value).unwrap();

	// Serialize to JSON
	let json = serde_json::to_string(&person).unwrap();

	// Verify the JSON has the expected structure
	let json_value: serde_json::Value = serde_json::from_str(&json).unwrap();
	assert_eq!(json_value["full_name"], "Eve");
	assert_eq!(json_value["age"], 45);
	assert_eq!(json_value["street"], "321 Elm St");
	assert_eq!(json_value["city"], "Metropolis");
}
