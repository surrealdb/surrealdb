mod enum_mixed_with_value;
mod enum_tagged_skip_content;
mod enum_tagged_tag;
mod enum_tagged_tag_content;
mod enum_tagged_variant;
mod enum_unit_value;
mod enum_untagged;
mod struct_flatten;
mod wrap;

use rstest::rstest;
use surrealdb_types::{Array, Object, SurrealValue, Uuid, Value, object};

////////////////////////////////////////////////////
///////////////// Simple struct ////////////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
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
#[surreal(crate = "surrealdb_types")]
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
/////// Struct with raw identifier fields //////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
struct RawIdentStruct {
	r#type: String,
	name: String,
}

#[test]
fn test_raw_identifier_field_uses_unescaped_name() {
	let val = RawIdentStruct {
		r#type: "span".to_string(),
		name: "test".to_string(),
	};

	let value = val.into_value();
	if let Value::Object(obj) = &value {
		assert_eq!(obj.get("type"), Some(&Value::String("span".to_string())));
		assert!(obj.get("r#type").is_none(), "key must be 'type', not 'r#type'");
		assert_eq!(obj.get("name"), Some(&Value::String("test".to_string())));
	} else {
		panic!("Expected Object value");
	}

	let converted = RawIdentStruct::from_value(value.clone()).unwrap();
	assert_eq!(converted.r#type, "span");
	assert_eq!(converted.name, "test");

	let kind = RawIdentStruct::kind_of();
	let debug = format!("{:?}", kind);
	assert!(debug.contains(r#""type": String"#), "kind_of should use 'type' not 'r#type': {debug}");
	assert!(!debug.contains("r#type"), "kind_of must not contain 'r#type': {debug}");

	assert!(RawIdentStruct::is_value(&value));
	assert!(value.is::<RawIdentStruct>());
	assert!(!RawIdentStruct::is_value(&Value::None));
	assert!(!RawIdentStruct::is_value(&Value::Object(Object::new())));
}

////////////////////////////////////////////////////
///// Enum with raw identifier variant names ///////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(tag = "kind")]
#[allow(non_camel_case_types)]
enum RawIdentEnum {
	r#type {
		name: String,
	},
	Normal {
		name: String,
	},
}

#[test]
fn test_raw_identifier_enum_variant_uses_unescaped_name() {
	let val = RawIdentEnum::r#type {
		name: "test".to_string(),
	};

	let value = val.into_value();
	if let Value::Object(obj) = &value {
		assert_eq!(obj.get("kind"), Some(&Value::String("type".to_string())));
		assert_eq!(obj.get("name"), Some(&Value::String("test".to_string())));
	} else {
		panic!("Expected Object value");
	}

	let converted = RawIdentEnum::from_value(value.clone()).unwrap();
	assert_eq!(
		converted,
		RawIdentEnum::r#type {
			name: "test".to_string()
		}
	);

	assert!(RawIdentEnum::is_value(&value));
	assert!(value.is::<RawIdentEnum>());
}

////////////////////////////////////////////////////
/////////// Simple single field struct /////////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
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
	assert_eq!(format!("{:?}", person_kind), r#"String"#);

	// Test is_value
	assert!(StringWrapper::is_value(&value));
	assert!(value.is::<StringWrapper>());
	assert!(!StringWrapper::is_value(&Value::None));
}

////////////////////////////////////////////////////
/////// Simple single field as tuple struct ////////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
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
#[surreal(crate = "surrealdb_types")]
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
#[surreal(crate = "surrealdb_types")]
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
#[surreal(crate = "surrealdb_types")]
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

////////////////////////////////////////////////////
////////////////// RouterRequest ///////////////////
////////////////////////////////////////////////////

#[derive(Clone, Debug, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
pub(crate) struct RouterRequest {
	id: Option<i64>,
	method: String,
	params: Option<Value>,
	#[allow(dead_code)]
	transaction: Option<Uuid>,
}

#[test]
fn test_router_request() {
	let request = RouterRequest {
		id: Some(1234),
		method: "request".to_string(),
		params: Some(Value::String("request".to_string())),
		transaction: Some(Uuid::nil()),
	};

	let value = request.into_value();
	let obj = value.clone().into_object().unwrap();

	assert_eq!(obj.get("id"), Some(&Value::Number(1234.into())));
	assert_eq!(obj.get("method"), Some(&Value::String("request".to_string())));
	assert_eq!(obj.get("params"), Some(&Value::String("request".to_string())));
	assert_eq!(obj.get("transaction"), Some(&Value::Uuid(Uuid::nil())));

	let converted = RouterRequest::from_value(value).unwrap();
	assert_eq!(converted.id, Some(1234));
	assert_eq!(converted.method, "request");
	assert_eq!(converted.params, Some(Value::String("request".to_string())));
	assert_eq!(converted.transaction, Some(Uuid::nil()));
}

#[derive(Clone, Debug, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
struct TestOptional {
	id: i64,
	name: Option<String>,
}

#[test]
fn test_test_optional() {
	#[derive(Clone, Debug, SurrealValue)]
	#[surreal(crate = "surrealdb_types")]
	struct TestOptionalNoOption {
		id: i64,
	}

	let value = TestOptionalNoOption {
		id: 1,
	}
	.into_value();
	assert_eq!(value, Value::Object(object! { id: 1 }));

	let converted = TestOptionalNoOption::from_value(value.clone()).unwrap();
	assert_eq!(converted.id, 1);

	let optional_value = TestOptional::from_value(value).unwrap();
	assert_eq!(optional_value.id, 1);
	assert_eq!(optional_value.name, None);
}

#[derive(Clone, Debug, SurrealValue, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(default)]
struct TestDefault {
	str: String,
	boolean: bool,
	optional: Option<String>,
}

impl Default for TestDefault {
	fn default() -> Self {
		TestDefault {
			str: "default".to_string(),
			boolean: true,
			optional: None,
		}
	}
}

#[rstest]
#[case(Value::Object(object! {}), TestDefault::default())]
#[case(Value::Object(object! { str: "test".to_string() }), TestDefault { str: "test".to_string(), boolean: true, optional: None })]
#[case(Value::Object(object! { str: "test".to_string(), boolean: false }), TestDefault { str: "test".to_string(), boolean: false, optional: None })]
#[case(Value::Object(object! { str: "test".to_string(), boolean: false, optional: Some("test".to_string()) }), TestDefault { str: "test".to_string(), boolean: false, optional: Some("test".to_string()) })]
fn test_test_default(#[case] value: Value, #[case] expected: TestDefault) {
	let parsed = TestDefault::from_value(value).unwrap();
	assert_eq!(parsed, expected);
}

////////////////////////////////////////////////////
//////// Per-field #[surreal(default)] //////////////
////////////////////////////////////////////////////

/// Used by #[surreal(default = "default_code_for_test")] in tests.
fn default_code_for_test() -> i64 {
	-32000
}

#[derive(Clone, Debug, SurrealValue, PartialEq)]
#[surreal(crate = "surrealdb_types")]
struct StructWithFieldDefaults {
	#[surreal(default = "default_code_for_test")]
	code: i64,
	#[surreal(default)]
	optional: Option<String>,
	message: String,
}

#[test]
fn test_per_field_default_missing_optional_fields() {
	// Only message present: code and optional get their defaults
	let value = Value::Object(object! { message: "Something went wrong".to_string() });
	let parsed = StructWithFieldDefaults::from_value(value).unwrap();
	assert_eq!(parsed.code, -32000, "code should use default_code_for_test()");
	assert_eq!(parsed.optional, None, "optional should use Default::default()");
	assert_eq!(parsed.message, "Something went wrong");
}

#[test]
fn test_per_field_default_all_fields_present() {
	let value = Value::Object(object! {
		code: 123,
		optional: Some("detail".to_string()),
		message: "Bad input".to_string(),
	});
	let parsed = StructWithFieldDefaults::from_value(value).unwrap();
	assert_eq!(parsed.code, 123);
	assert_eq!(parsed.optional, Some("detail".to_string()));
	assert_eq!(parsed.message, "Bad input");
}

#[test]
fn test_per_field_default_required_field_missing_fails() {
	// Missing required field "message" should fail
	let value = Value::Object(object! { code: 1 });
	let result = StructWithFieldDefaults::from_value(value);
	assert!(result.is_err());
}

#[test]
fn test_per_field_default_roundtrip() {
	let s = StructWithFieldDefaults {
		code: -32000,
		optional: None,
		message: "hello".to_string(),
	};
	let value = s.clone().into_value();
	let parsed = StructWithFieldDefaults::from_value(value).unwrap();
	assert_eq!(parsed, s);
}

// -------------------------------------------------
// Recursive enum (issue #6829)
// -------------------------------------------------

#[derive(Clone, Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
enum RecursiveEnum {
	Leaf(String),
	BoxChild(Box<RecursiveEnum>),
	VecChildren(Vec<RecursiveEnum>),
}

#[test]
fn test_recursive_enum_kind_of_does_not_stack_overflow() {
	let kind = RecursiveEnum::kind_of();
	let debug = format!("{kind:?}");
	assert!(debug.contains("Any"), "Recursive references should resolve to Kind::Any");
}

#[test]
fn test_recursive_enum_roundtrip() {
	let value = RecursiveEnum::BoxChild(Box::new(RecursiveEnum::Leaf("hello".to_string())));
	let converted = value.clone().into_value();
	let parsed = RecursiveEnum::from_value(converted).unwrap();
	assert_eq!(parsed, value);

	let nested = RecursiveEnum::VecChildren(vec![
		RecursiveEnum::Leaf("a".to_string()),
		RecursiveEnum::BoxChild(Box::new(RecursiveEnum::Leaf("b".to_string()))),
	]);
	let converted = nested.clone().into_value();
	let parsed = RecursiveEnum::from_value(converted).unwrap();
	assert_eq!(parsed, nested);
}

// -------------------------------------------------
// Recursive struct (issue #6829)
// -------------------------------------------------

#[derive(Clone, Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
struct RecursiveStruct {
	name: String,
	children: Vec<RecursiveStruct>,
}

#[test]
fn test_recursive_struct_kind_of_does_not_stack_overflow() {
	let kind = RecursiveStruct::kind_of();
	let debug = format!("{kind:?}");
	assert!(debug.contains("Any"), "Recursive references should resolve to Kind::Any");
}

#[test]
fn test_recursive_struct_roundtrip() {
	let value = RecursiveStruct {
		name: "root".to_string(),
		children: vec![
			RecursiveStruct {
				name: "child1".to_string(),
				children: vec![],
			},
			RecursiveStruct {
				name: "child2".to_string(),
				children: vec![RecursiveStruct {
					name: "grandchild".to_string(),
					children: vec![],
				}],
			},
		],
	};
	let converted = value.clone().into_value();
	let parsed = RecursiveStruct::from_value(converted).unwrap();
	assert_eq!(parsed, value);
}

// -------------------------------------------------
// Generic struct cross-monomorphization
// -------------------------------------------------

#[derive(Clone, Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
struct GenericWrapper<T: SurrealValue + Clone + std::fmt::Debug + PartialEq> {
	inner: T,
	nested: Option<Box<GenericWrapper<String>>>,
}

#[test]
fn test_generic_kind_of_no_false_recursion_across_monomorphizations() {
	// Each monomorphization should correctly compute its non-recursive fields.
	// The compile-time detection marks fields containing the type name as
	// Kind::Any, so the `nested` field (which contains GenericWrapper) is
	// always Any regardless of the type parameter.
	let i64_kind = GenericWrapper::<i64>::kind_of();
	let i64_debug = format!("{i64_kind:?}");
	assert!(
		i64_debug.contains("Int"),
		"inner field of GenericWrapper<i64> should be Int: {i64_debug}"
	);
	assert!(
		i64_debug.contains("Any"),
		"nested field should be Any (self-referential): {i64_debug}"
	);

	// Calling GenericWrapper<String>::kind_of() independently should produce
	// String for the inner field, confirming each monomorphization is correct.
	let string_kind = GenericWrapper::<String>::kind_of();
	let string_debug = format!("{string_kind:?}");
	assert!(
		string_debug.contains("String"),
		"inner field of GenericWrapper<String> should be String: {string_debug}"
	);
	assert!(
		string_debug.contains("Any"),
		"nested field should be Any (self-referential): {string_debug}"
	);
}

// -------------------------------------------------
// Module-qualified field is not self-referential
// -------------------------------------------------

mod other {
	use super::*;

	#[derive(Clone, Debug, PartialEq, SurrealValue)]
	#[surreal(crate = "surrealdb_types")]
	pub struct Shared {
		pub value: i64,
	}
}

/// A struct whose name matches the last segment of a module-qualified field
/// type (`other::Shared`). The field must NOT be treated as self-referential
/// because the fully-qualified path points to a different type.
#[derive(Clone, Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
struct Shared {
	name: String,
	foreign: other::Shared,
}

#[test]
fn test_module_qualified_same_name_is_not_self_referential() {
	let kind = Shared::kind_of();
	let debug = format!("{kind:?}");
	// `other::Shared` is a different type than `Shared` — its kind should be
	// fully computed, not short-circuited to Kind::Any.
	assert!(debug.contains("Int"), "foreign field should have its kind fully computed: {debug}");
	assert!(
		!debug.contains("Any"),
		"no field should be Any since nothing is self-referential: {debug}"
	);
}

// -------------------------------------------------
// Self keyword in field types
// -------------------------------------------------

#[derive(Clone, Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
struct SelfRefStruct {
	name: String,
	child: Option<Box<Self>>,
}

#[test]
fn test_self_keyword_is_detected_as_recursive() {
	let kind = SelfRefStruct::kind_of();
	let debug = format!("{kind:?}");
	assert!(
		debug.contains("Any"),
		"Self-referential field via `Self` keyword should resolve to Kind::Any: {debug}"
	);
}

// -------------------------------------------------
// self::Type path in field types
// -------------------------------------------------

#[derive(Clone, Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
struct SelfModuleRef {
	name: String,
	child: Option<Box<self::SelfModuleRef>>,
}

#[test]
fn test_self_module_path_is_detected_as_recursive() {
	let kind = SelfModuleRef::kind_of();
	let debug = format!("{kind:?}");
	assert!(
		debug.contains("Any"),
		"Self-referential field via `self::Type` path should resolve to Kind::Any: {debug}"
	);
}
