use surrealdb_types::{Kind, KindLiteral, SurrealValue, Value};

#[derive(SurrealValue, Debug, PartialEq)]
struct Person {
	name: String,
	age: i64,
}

#[derive(SurrealValue, Debug, PartialEq)]
struct Point(i64, i64);

#[derive(SurrealValue, Debug, PartialEq)]
struct UnitStruct;

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
