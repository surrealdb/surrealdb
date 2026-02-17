use surrealdb_types::{Number, SurrealValue, Value, array, object};

////////////////////////////////////////////////////
////// Enum with tag + content + skip_content_if ///
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
enum TestSkipContent {
	UnitA,
	UnitB,
	Named {
		name: String,
		count: i64,
	},
	Single(String),
	Tuple(String, i64),
}

// -- Unit variants --

#[test]
fn test_skip_content_unit_serialization() {
	let val = TestSkipContent::UnitA.into_value();
	// Unit variant should produce { kind: "UnitA" } with NO details field
	let expected = Value::Object(object! { kind: "UnitA" });
	assert_eq!(val, expected);

	// Verify details key is absent
	let obj = val.into_object().unwrap();
	assert!(!obj.contains_key("details"));
}

#[test]
fn test_skip_content_unit_roundtrip() {
	let val = Value::Object(object! { kind: "UnitA" });
	let parsed = TestSkipContent::from_value(val).unwrap();
	assert_eq!(parsed, TestSkipContent::UnitA);
}

#[test]
fn test_skip_content_unit_roundtrip_with_empty_details() {
	// Backward compat: accept { kind: "UnitB", details: {} } too
	let val = Value::Object(object! { kind: "UnitB", details: Value::Object(object! {}) });
	let parsed = TestSkipContent::from_value(val).unwrap();
	assert_eq!(parsed, TestSkipContent::UnitB);
}

#[test]
fn test_skip_content_unit_is_value() {
	// Without details
	assert!(TestSkipContent::is_value(&Value::Object(object! { kind: "UnitA" })));
	// With empty details
	assert!(TestSkipContent::is_value(&Value::Object(
		object! { kind: "UnitA", details: Value::Object(object! {}) }
	)));
	// Wrong kind
	assert!(!TestSkipContent::is_value(&Value::Object(object! { kind: "NotAVariant" })));
}

// -- Named struct variants --

#[test]
fn test_skip_content_named_serialization() {
	let val = TestSkipContent::Named {
		name: "test".into(),
		count: 42,
	}
	.into_value();
	let expected = Value::Object(object! {
		kind: "Named",
		details: Value::Object(object! {
			name: "test",
			count: 42i64
		})
	});
	assert_eq!(val, expected);
}

#[test]
fn test_skip_content_named_roundtrip() {
	let original = TestSkipContent::Named {
		name: "test".into(),
		count: 42,
	};
	let val = original.into_value();
	let parsed = TestSkipContent::from_value(val).unwrap();
	assert_eq!(
		parsed,
		TestSkipContent::Named {
			name: "test".into(),
			count: 42
		}
	);
}

#[test]
fn test_skip_content_named_is_value() {
	assert!(TestSkipContent::is_value(&Value::Object(object! {
		kind: "Named",
		details: Value::Object(object! { name: "x", count: 1i64 })
	})));
}

// -- Single-field newtype variants --

#[test]
fn test_skip_content_single_serialization() {
	let val = TestSkipContent::Single("hello".into()).into_value();
	let expected = Value::Object(object! {
		kind: "Single",
		details: "hello"
	});
	assert_eq!(val, expected);
}

#[test]
fn test_skip_content_single_roundtrip() {
	let original = TestSkipContent::Single("hello".into());
	let val = original.into_value();
	let parsed = TestSkipContent::from_value(val).unwrap();
	assert_eq!(parsed, TestSkipContent::Single("hello".into()));
}

#[test]
fn test_skip_content_single_is_value() {
	assert!(TestSkipContent::is_value(&Value::Object(object! {
		kind: "Single",
		details: "hello"
	})));
}

// -- Tuple variants --

#[test]
fn test_skip_content_tuple_serialization() {
	let val = TestSkipContent::Tuple("x".into(), 1).into_value();
	let expected = Value::Object(object! {
		kind: "Tuple",
		details: Value::Array(array![Value::String("x".into()), Value::Number(Number::Int(1))])
	});
	assert_eq!(val, expected);
}

#[test]
fn test_skip_content_tuple_roundtrip() {
	let original = TestSkipContent::Tuple("x".into(), 1);
	let val = original.into_value();
	let parsed = TestSkipContent::from_value(val).unwrap();
	assert_eq!(parsed, TestSkipContent::Tuple("x".into(), 1));
}

#[test]
fn test_skip_content_tuple_is_value() {
	assert!(TestSkipContent::is_value(&Value::Object(object! {
		kind: "Tuple",
		details: Value::Array(array![Value::String("x".into()), Value::Number(Number::Int(1))])
	})));
}

// -- Rejection --

#[test]
fn test_skip_content_rejects_missing_kind() {
	let val = Value::Object(object! { details: "something" });
	assert!(TestSkipContent::from_value(val).is_err());
}

#[test]
fn test_skip_content_rejects_unknown_kind() {
	let val = Value::Object(object! { kind: "Unknown" });
	assert!(TestSkipContent::from_value(val).is_err());
}

////////////////////////////////////////////////////
//// Default fallback when content is missing //////
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq, Default)]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
enum TestSkipContentWithDefault {
	#[default]
	UnitA,
	Named {
		name: String,
		count: i64,
	},
	Single(String),
}

#[test]
fn test_skip_content_named_missing_content_uses_default() {
	// When skip_content_if is set and content is missing for a named struct variant,
	// deserialization should fall back to Default::default() for all fields
	let val = Value::Object(object! { kind: "Named" });
	let parsed = TestSkipContentWithDefault::from_value(val).unwrap();
	assert_eq!(
		parsed,
		TestSkipContentWithDefault::Named {
			name: String::default(),
			count: i64::default()
		}
	);
}

#[test]
fn test_skip_content_single_missing_content_uses_default() {
	// When skip_content_if is set and content is missing for a newtype variant,
	// deserialization should fall back to Default::default() for the inner type
	let val = Value::Object(object! { kind: "Single" });
	let parsed = TestSkipContentWithDefault::from_value(val).unwrap();
	assert_eq!(parsed, TestSkipContentWithDefault::Single(String::default()));
}

#[test]
fn test_skip_content_named_with_content_still_works() {
	// When content IS present, it should still be used normally
	let val = Value::Object(object! {
		kind: "Named",
		details: Value::Object(object! { name: "hello", count: 5i64 })
	});
	let parsed = TestSkipContentWithDefault::from_value(val).unwrap();
	assert_eq!(
		parsed,
		TestSkipContentWithDefault::Named {
			name: "hello".into(),
			count: 5
		}
	);
}

////////////////////////////////////////////////////
//// Enum WITHOUT skip_content_if (old behavior) ///
////////////////////////////////////////////////////

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "kind", content = "details")]
enum TestNoSkipContent {
	UnitA,
	Named {
		name: String,
	},
}

#[test]
fn test_no_skip_content_unit_still_has_empty_details() {
	let val = TestNoSkipContent::UnitA.into_value();
	// Without skip_content_if, unit variants SHOULD still include details: {}
	let expected = Value::Object(object! {
		kind: "UnitA",
		details: Value::Object(object! {})
	});
	assert_eq!(val, expected);
}

#[test]
fn test_no_skip_content_named_roundtrip() {
	let original = TestNoSkipContent::Named {
		name: "test".into(),
	};
	let val = original.into_value();
	let expected = Value::Object(object! {
		kind: "Named",
		details: Value::Object(object! { name: "test" })
	});
	assert_eq!(val, expected);

	let parsed = TestNoSkipContent::from_value(val).unwrap();
	assert_eq!(
		parsed,
		TestNoSkipContent::Named {
			name: "test".into()
		}
	);
}
