mod enum_mixed_with_value;
mod enum_tagged_tag;
mod enum_tagged_tag_content;
mod enum_tagged_variant;
mod enum_unit_value;
mod enum_untagged;

use surrealdb_types::{Array, Object, SurrealValue, Value};

////////////////////////////////////////////////////
///////////////// Simple struct ////////////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
struct Person {
	name: String,
	age: i64,
}

#[test]
fn test_simple_struct() {
	// Test named struct
	let person = Person {
		name: "Alice".to_string(),
		age: 30,
	};

	// Test into_value
	let value = person.into_value();
	if let Value::Object(obj) = &value {
		assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
		assert_eq!(obj.get("age"), Some(&Value::Number(30.into())));
	} else {
		panic!("Expected Object value");
	}

	// Test from_value
	let converted = Person::from_value(value.clone()).unwrap();
	assert_eq!(converted.name, "Alice");
	assert_eq!(converted.age, 30);

	// Test kind_of
	let person_kind = Person::kind_of();
	assert_eq!(format!("{:?}", person_kind), r#"Literal(Object({"age": Int, "name": String}))"#);

	// Test is_value
	assert!(Person::is_value(&value));
	assert!(value.is::<Person>());
	assert!(!Person::is_value(&Value::None));
	assert!(!Person::is_value(&Value::Object(Object::new())));
}

////////////////////////////////////////////////////
/////// Simple struct with renamed fields //////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
struct PersonRenamed {
	#[surreal(rename = "full_name")]
	name: String,
	#[surreal(rename = "years_old")]
	age: i64,
}

#[test]
fn test_simple_struct_with_renamed_fields() {
	let person = PersonRenamed {
		name: "Alice".to_string(),
		age: 30,
	};

	// Test into_value
	let value = person.into_value();
	if let Value::Object(obj) = &value {
		assert_eq!(obj.get("full_name"), Some(&Value::String("Alice".to_string())));
		assert_eq!(obj.get("years_old"), Some(&Value::Number(30.into())));
		assert_eq!(obj.get("name"), None);
		assert_eq!(obj.get("age"), None);
	} else {
		panic!("Expected Object value");
	}

	// Test from_value
	let converted = PersonRenamed::from_value(value.clone()).unwrap();
	assert_eq!(converted.name, "Alice");
	assert_eq!(converted.age, 30);

	// Test kind_of
	let person_kind = PersonRenamed::kind_of();
	assert_eq!(
		format!("{:?}", person_kind),
		r#"Literal(Object({"full_name": String, "years_old": Int}))"#
	);

	// Test is_value
	assert!(PersonRenamed::is_value(&value));
	assert!(value.is::<PersonRenamed>());
	assert!(!PersonRenamed::is_value(&Value::None));
	assert!(!PersonRenamed::is_value(&Value::Object(Object::new())));
}

////////////////////////////////////////////////////
/////////// Simple single field struct /////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
struct StringWrapper(String);

#[test]
fn test_simple_single_field_struct() {
	let str = StringWrapper("Alice".to_string());

	// Test into_value
	let value = str.into_value();
	assert_eq!(value, Value::String("Alice".to_string()));

	// Test from_value
	let converted = StringWrapper::from_value(value.clone()).unwrap();
	assert_eq!(converted.0, "Alice");

	// Test kind_of
	let person_kind = StringWrapper::kind_of();
	assert_eq!(format!("{:?}", person_kind), r#"Literal(Array([String]))"#);

	// Test is_value
	assert!(StringWrapper::is_value(&value));
	assert!(value.is::<StringWrapper>());
	assert!(!StringWrapper::is_value(&Value::None));
}

////////////////////////////////////////////////////
/////// Simple single field as tuple struct ////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tuple)]
struct StringWrapperTuple(String);

#[test]
fn test_simple_single_field_tuple_struct() {
	let str = StringWrapperTuple("Alice".to_string());

	// Test into_value
	let value = str.into_value();
	assert_eq!(value, Value::Array(Array::from(vec![Value::String("Alice".to_string())])));

	// Test from_value
	let converted = StringWrapperTuple::from_value(value.clone()).unwrap();
	assert_eq!(converted.0, "Alice");

	// Test kind_of
	let person_kind = StringWrapperTuple::kind_of();
	assert_eq!(format!("{:?}", person_kind), r#"Literal(Array([String]))"#);

	// Test is_value
	assert!(StringWrapperTuple::is_value(&value));
	assert!(value.is::<StringWrapperTuple>());
	assert!(!StringWrapperTuple::is_value(&Value::None));
	assert!(!StringWrapperTuple::is_value(&Value::Array(Array::new())));
}

////////////////////////////////////////////////////
/////////// Simple multi-field struct //////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
struct Point(i64, i64);

#[test]
fn test_simple_multi_field_struct() {
	let point = Point(1, 2);

	// Test into_value
	let value = point.into_value();
	assert_eq!(
		value,
		Value::Array(Array::from(vec![Value::Number(1.into()), Value::Number(2.into())]))
	);

	// Test from_value
	let converted = Point::from_value(value.clone()).unwrap();
	assert_eq!(converted.0, 1);
	assert_eq!(converted.1, 2);

	// Test kind_of
	let point_kind = Point::kind_of();
	assert_eq!(format!("{:?}", point_kind), r#"Literal(Array([Int, Int]))"#);

	// Test is_value
	assert!(Point::is_value(&value));
	assert!(value.is::<Point>());
	assert!(!Point::is_value(&Value::None));
	assert!(!Point::is_value(&Value::Array(Array::new())));
}

////////////////////////////////////////////////////
////////////////// Unit struct /////////////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
struct UnitStruct;

#[test]
fn test_unit_struct() {
	// Test into_value
	let value = UnitStruct.into_value();
	assert_eq!(value, Value::Object(Object::new()));

	// Test from_value
	let converted = UnitStruct::from_value(value.clone()).unwrap();
	assert_eq!(converted, UnitStruct);

	// Test kind_of
	let unit_kind = UnitStruct::kind_of();
	assert_eq!(format!("{:?}", unit_kind), r#"Literal(Object({}))"#);

	// Test is_value
	assert!(UnitStruct::is_value(&value));
	assert!(value.is::<UnitStruct>());
	assert!(!UnitStruct::is_value(&Value::None));
	assert!(UnitStruct::is_value(&Value::Object(Object::new())));
}

////////////////////////////////////////////////////
/////////////// Unit struct with value /////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(value = true)]
struct UnitStructWithValue;

#[test]
fn test_unit_struct_with_value() {
	// Test into_value
	let value = UnitStructWithValue.into_value();
	assert_eq!(value, Value::Bool(true));

	// Test from_value
	let converted = UnitStructWithValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, UnitStructWithValue);

	// Test kind_of
	let unit_kind = UnitStructWithValue::kind_of();
	assert_eq!(format!("{:?}", unit_kind), r#"Literal(Bool(true))"#);

	// Test is_value
	assert!(UnitStructWithValue::is_value(&value));
	assert!(value.is::<UnitStructWithValue>());
	assert!(!UnitStructWithValue::is_value(&Value::None));
	assert!(!UnitStructWithValue::is_value(&Value::Object(Object::new())));
}
