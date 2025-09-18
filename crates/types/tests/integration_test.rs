use surrealdb_types::{Kind, KindLiteral, Object, SurrealValue, Value};

#[derive(SurrealValue, Debug, PartialEq)]
struct Person {
	name: String,
	age: i64,
}

#[derive(SurrealValue, Debug, PartialEq)]
struct Point(i64, i64);

#[derive(SurrealValue, Debug, PartialEq)]
struct UnitStruct;

// Test rename functionality
#[derive(SurrealValue, Debug, PartialEq)]
struct PersonRenamed {
	#[surreal(rename = "full_name")]
	name: String,
	#[surreal(rename = "years_old")]
	age: i64,
}

// Test flatten functionality
#[derive(SurrealValue, Debug, PartialEq)]
struct Address {
	street: String,
	city: String,
}

#[derive(SurrealValue, Debug, PartialEq)]
struct PersonFlattened {
	name: String,
	#[surreal(flatten)]
	address: Address,
}

// Test combined functionality (rename and flatten)
#[derive(SurrealValue, Debug, PartialEq)]
struct PersonCombined {
	#[surreal(rename = "full_name")]
	name: String,
	age: i64,
	#[surreal(flatten)]
	address: Address,
}

#[test]
fn test_derive_from_main_crate() {
	// Test named struct
	let person = Person {
		name: "Alice".to_string(),
		age: 30,
	};

	let value = person.into_value();
	assert!(matches!(value, Value::Object(_)));

	let converted = Person::from_value(value).unwrap();
	assert_eq!(converted.name, "Alice");
	assert_eq!(converted.age, 30);

	// Test kind
	let person_kind = Person::kind_of();
	assert!(matches!(person_kind, Kind::Literal(KindLiteral::Object(_))));

	// Test unnamed struct
	let point = Point(10, 20);
	let value = point.into_value();
	assert!(matches!(value, Value::Array(_)));

	let converted = Point::from_value(value).unwrap();
	assert_eq!(converted.0, 10);
	assert_eq!(converted.1, 20);

	// Test unit struct
	let unit = UnitStruct;
	let value = unit.into_value();
	assert!(matches!(value, Value::Object(_)));

	let _converted = UnitStruct::from_value(value).unwrap();
}

#[test]
fn test_rename_fields() {
	let person = PersonRenamed {
		name: "Alice".to_string(),
		age: 30,
	};

	// Convert to value and check field names are renamed
	let value = person.into_value();
	if let Value::Object(obj) = &value {
		assert!(obj.get("full_name").is_some());
		assert!(obj.get("years_old").is_some());
		assert!(obj.get("name").is_none());
		assert!(obj.get("age").is_none());

		if let Some(Value::String(name)) = obj.get("full_name") {
			assert_eq!(name, "Alice");
		}

		if let Some(Value::Number(age)) = obj.get("years_old") {
			assert_eq!(*age, 30.into());
		}
	} else {
		panic!("Expected Object value");
	}

	// Convert back from value
	let converted = PersonRenamed::from_value(value).unwrap();
	assert_eq!(converted.name, "Alice");
	assert_eq!(converted.age, 30);
}

#[test]
fn test_flatten_fields() {
	let address = Address {
		street: "123 Main St".to_string(),
		city: "Anytown".to_string(),
	};

	let person = PersonFlattened {
		name: "Bob".to_string(),
		address,
	};

	// Convert to value and check fields are flattened
	let value = person.into_value();
	if let Value::Object(obj) = &value {
		assert!(obj.get("name").is_some());
		assert!(obj.get("street").is_some());
		assert!(obj.get("city").is_some());
		assert!(obj.get("address").is_none());

		if let Some(Value::String(name)) = obj.get("name") {
			assert_eq!(name, "Bob");
		}

		if let Some(Value::String(street)) = obj.get("street") {
			assert_eq!(street, "123 Main St");
		}

		if let Some(Value::String(city)) = obj.get("city") {
			assert_eq!(city, "Anytown");
		}
	} else {
		panic!("Expected Object value");
	}

	// Convert back from value
	let converted = PersonFlattened::from_value(value).unwrap();
	assert_eq!(converted.name, "Bob");
	assert_eq!(converted.address.street, "123 Main St");
	assert_eq!(converted.address.city, "Anytown");
}

#[test]
fn test_combined_functionality() {
	let address = Address {
		street: "456 Oak Ave".to_string(),
		city: "Springfield".to_string(),
	};

	let person = PersonCombined {
		name: "Diana".to_string(),
		age: 35,
		address,
	};

	// Convert to value and check both rename and flatten
	let value = person.into_value();
	if let Value::Object(obj) = &value {
		assert!(obj.get("full_name").is_some());
		assert!(obj.get("age").is_some());
		assert!(obj.get("street").is_some());
		assert!(obj.get("city").is_some());
		assert!(obj.get("name").is_none());
		assert!(obj.get("address").is_none());

		if let Some(Value::String(name)) = obj.get("full_name") {
			assert_eq!(name, "Diana");
		}

		if let Some(Value::Number(age)) = obj.get("age") {
			assert_eq!(*age, 35.into());
		}

		if let Some(Value::String(street)) = obj.get("street") {
			assert_eq!(street, "456 Oak Ave");
		}

		if let Some(Value::String(city)) = obj.get("city") {
			assert_eq!(city, "Springfield");
		}
	} else {
		panic!("Expected Object value");
	}

	// Convert back from value
	let converted = PersonCombined::from_value(value).unwrap();
	assert_eq!(converted.name, "Diana");
	assert_eq!(converted.age, 35);
	assert_eq!(converted.address.street, "456 Oak Ave");
	assert_eq!(converted.address.city, "Springfield");
}

#[test]
fn test_is_value_with_attributes() {
	// Test renamed fields
	let mut obj = Object::new();
	obj.insert("full_name".to_string(), Value::String("Test".to_string()));
	obj.insert("years_old".to_string(), Value::Number(30.into()));
	let value = Value::Object(obj);
	assert!(PersonRenamed::is_value(&value));

	// Test flattened fields
	let mut obj = Object::new();
	obj.insert("name".to_string(), Value::String("Test".to_string()));
	obj.insert("street".to_string(), Value::String("123 Main St".to_string()));
	obj.insert("city".to_string(), Value::String("Anytown".to_string()));
	let value = Value::Object(obj);
	assert!(PersonFlattened::is_value(&value));

	// Test combined functionality
	let mut obj = Object::new();
	obj.insert("full_name".to_string(), Value::String("Test".to_string()));
	obj.insert("age".to_string(), Value::Number(30.into()));
	obj.insert("street".to_string(), Value::String("123 Main St".to_string()));
	obj.insert("city".to_string(), Value::String("Anytown".to_string()));
	let value = Value::Object(obj);
	assert!(PersonCombined::is_value(&value));
}
