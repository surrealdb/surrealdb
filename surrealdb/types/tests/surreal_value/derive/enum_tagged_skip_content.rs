use surrealdb_types::{Number, SurrealValue, Value, array, object};

// -------------------------------------------------
// Enum with tag + content + skip_content_if
// -------------------------------------------------

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

// -------------------------------------------------
// Default fallback when content is missing
// -------------------------------------------------

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
	// With skip_content_if, newtype variants fall back to Default::default()
	// when content is missing (String::default() == "")
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

// -------------------------------------------------
// Invalid content should error, not silently default
// -------------------------------------------------

#[test]
fn test_skip_content_named_invalid_content_errors() {
	// Content is present but is a String instead of an Object -- should error,
	// not silently fall back to defaults.
	let val = Value::Object(object! { kind: "Named", details: "not_an_object" });
	assert!(
		TestSkipContentWithDefault::from_value(val).is_err(),
		"Named variant with non-Object content should error, not silently default"
	);
}

// -------------------------------------------------
// Enum WITHOUT skip_content_if (old behavior)
// -------------------------------------------------

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

// -------------------------------------------------
// Newtype with Option<T> and skip_content_if (ErrorDetails pattern)
// -------------------------------------------------

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
enum TestSkipContentOption {
	WithInner(Option<String>),
	AnotherUnit,
}

#[test]
fn test_skip_content_option_some_serialization() {
	let val = TestSkipContentOption::WithInner(Some("hello".into())).into_value();
	let expected = Value::Object(object! { kind: "WithInner", details: "hello" });
	assert_eq!(val, expected);
}

#[test]
fn test_skip_content_option_none_serialization() {
	let val = TestSkipContentOption::WithInner(None).into_value();
	let expected = Value::Object(object! { kind: "WithInner" });
	assert_eq!(val, expected);
}

#[test]
fn test_skip_content_option_some_roundtrip() {
	let original = TestSkipContentOption::WithInner(Some("hello".into()));
	let val = original.into_value();
	let parsed = TestSkipContentOption::from_value(val).unwrap();
	assert_eq!(parsed, TestSkipContentOption::WithInner(Some("hello".into())));
}

#[test]
fn test_skip_content_option_none_roundtrip() {
	let original = TestSkipContentOption::WithInner(None);
	let val = original.into_value();
	let parsed = TestSkipContentOption::from_value(val).unwrap();
	assert_eq!(parsed, TestSkipContentOption::WithInner(None));
}

#[test]
fn test_skip_content_option_none_from_bare_kind() {
	let val = Value::Object(object! { kind: "WithInner" });
	let parsed = TestSkipContentOption::from_value(val).unwrap();
	assert_eq!(parsed, TestSkipContentOption::WithInner(None));
}

#[test]
fn test_skip_content_option_is_value() {
	assert!(TestSkipContentOption::is_value(&Value::Object(
		object! { kind: "WithInner", details: "hello" }
	)));
	assert!(TestSkipContentOption::is_value(&Value::Object(object! { kind: "WithInner" })));
}

// -------------------------------------------------
// Per-variant #[surreal(skip_content)] (no enum-level attr)
// -------------------------------------------------

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "kind", content = "details")]
enum TestPerVariantSkip {
	#[surreal(skip_content)]
	UnitSkipped,
	UnitNotSkipped,
	Named {
		name: String,
	},
}

#[test]
fn test_per_variant_skip_unit_serialization() {
	let val = TestPerVariantSkip::UnitSkipped.into_value();
	let expected = Value::Object(object! { kind: "UnitSkipped" });
	assert_eq!(val, expected);
	let obj = val.into_object().unwrap();
	assert!(!obj.contains_key("details"));
}

#[test]
fn test_per_variant_skip_unit_roundtrip() {
	let val = Value::Object(object! { kind: "UnitSkipped" });
	let parsed = TestPerVariantSkip::from_value(val).unwrap();
	assert_eq!(parsed, TestPerVariantSkip::UnitSkipped);
}

#[test]
fn test_per_variant_no_skip_unit_still_has_details() {
	let val = TestPerVariantSkip::UnitNotSkipped.into_value();
	let expected = Value::Object(object! {
		kind: "UnitNotSkipped",
		details: Value::Object(object! {})
	});
	assert_eq!(val, expected);
}

#[test]
fn test_per_variant_skip_named_unaffected() {
	let original = TestPerVariantSkip::Named {
		name: "test".into(),
	};
	let val = original.into_value();
	let expected = Value::Object(object! {
		kind: "Named",
		details: Value::Object(object! { name: "test" })
	});
	assert_eq!(val, expected);

	let parsed = TestPerVariantSkip::from_value(val).unwrap();
	assert_eq!(
		parsed,
		TestPerVariantSkip::Named {
			name: "test".into()
		}
	);
}

#[test]
fn test_per_variant_skip_is_value() {
	assert!(TestPerVariantSkip::is_value(&Value::Object(object! { kind: "UnitSkipped" })));
	assert!(TestPerVariantSkip::is_value(&Value::Object(
		object! { kind: "UnitNotSkipped", details: Value::Object(object! {}) }
	)));
}

// -------------------------------------------------
// Per-variant skip_content overrides enum-level skip_content_if
// -------------------------------------------------

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
enum TestOverrideSkip {
	#[surreal(skip_content)]
	AlwaysSkipped(Option<String>),
	ConditionallySkipped(Option<String>),
}

#[test]
fn test_override_always_skipped_with_some() {
	// Even with Some content, skip_content (Always) means no content key
	let val = TestOverrideSkip::AlwaysSkipped(Some("hello".into())).into_value();
	let obj = val.into_object().unwrap();
	assert_eq!(obj.get("kind"), Some(&Value::String("AlwaysSkipped".into())));
	assert!(!obj.contains_key("details"));
}

#[test]
fn test_override_conditionally_skipped_with_some() {
	// With enum-level skip_content_if, Some content is NOT empty so it's included
	let val = TestOverrideSkip::ConditionallySkipped(Some("hello".into())).into_value();
	let obj = val.into_object().unwrap();
	assert_eq!(obj.get("kind"), Some(&Value::String("ConditionallySkipped".into())));
	assert_eq!(obj.get("details"), Some(&Value::String("hello".into())));
}

#[test]
fn test_override_conditionally_skipped_with_none() {
	// None is empty, so skip_content_if skips it
	let val = TestOverrideSkip::ConditionallySkipped(None).into_value();
	let obj = val.into_object().unwrap();
	assert_eq!(obj.get("kind"), Some(&Value::String("ConditionallySkipped".into())));
	assert!(!obj.contains_key("details"));
}
