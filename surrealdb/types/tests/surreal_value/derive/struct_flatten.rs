use surrealdb_types::{SurrealValue, Value, object};

// -------------------------------------------------
// Basic flatten: inner struct merged into parent
// -------------------------------------------------

#[derive(SurrealValue, Debug, PartialEq)]
struct Inner {
	x: i64,
	y: i64,
}

#[derive(SurrealValue, Debug, PartialEq)]
struct Outer {
	name: String,
	#[surreal(flatten)]
	coords: Inner,
}

#[test]
fn test_flatten_serialization() {
	let val = Outer {
		name: "point".into(),
		coords: Inner {
			x: 1,
			y: 2,
		},
	}
	.into_value();

	// Flatten should merge Inner's fields into Outer's object
	let expected = Value::Object(object! {
		name: "point",
		x: 1i64,
		y: 2i64
	});
	assert_eq!(val, expected);
}

#[test]
fn test_flatten_deserialization() {
	let val = Value::Object(object! {
		name: "point",
		x: 1i64,
		y: 2i64
	});
	let parsed = Outer::from_value(val).unwrap();
	assert_eq!(
		parsed,
		Outer {
			name: "point".into(),
			coords: Inner {
				x: 1,
				y: 2,
			},
		}
	);
}

#[test]
fn test_flatten_roundtrip() {
	let original = Outer {
		name: "test".into(),
		coords: Inner {
			x: 10,
			y: 20,
		},
	};
	let val = original.into_value();
	let parsed = Outer::from_value(val).unwrap();
	assert_eq!(
		parsed,
		Outer {
			name: "test".into(),
			coords: Inner {
				x: 10,
				y: 20,
			},
		}
	);
}

// -------------------------------------------------
// Flatten with an enum (like Error + ErrorDetails)
// -------------------------------------------------

#[derive(SurrealValue, Debug, PartialEq, Clone)]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
enum Status {
	Active,
	Suspended {
		reason: String,
	},
}

#[derive(SurrealValue, Debug, PartialEq)]
struct Record {
	id: i64,
	#[surreal(flatten)]
	status: Status,
}

#[test]
fn test_flatten_enum_unit_variant() {
	let val = Record {
		id: 1,
		status: Status::Active,
	}
	.into_value();

	// Status::Active serializes as { kind: "Active" } which merges into Record
	let expected = Value::Object(object! {
		id: 1i64,
		kind: "Active"
	});
	assert_eq!(val, expected);
}

#[test]
fn test_flatten_enum_struct_variant() {
	let val = Record {
		id: 2,
		status: Status::Suspended {
			reason: "violation".into(),
		},
	}
	.into_value();

	// Status::Suspended serializes as { kind: "Suspended", details: { reason: "..." } }
	let expected = Value::Object(object! {
		id: 2i64,
		kind: "Suspended",
		details: Value::Object(object! { reason: "violation" })
	});
	assert_eq!(val, expected);
}

#[test]
fn test_flatten_enum_roundtrip_unit() {
	let original = Record {
		id: 1,
		status: Status::Active,
	};
	let val = original.into_value();
	let parsed = Record::from_value(val).unwrap();
	assert_eq!(
		parsed,
		Record {
			id: 1,
			status: Status::Active,
		}
	);
}

#[test]
fn test_flatten_enum_roundtrip_struct() {
	let original = Record {
		id: 2,
		status: Status::Suspended {
			reason: "violation".into(),
		},
	};
	let val = original.into_value();
	let parsed = Record::from_value(val).unwrap();
	assert_eq!(
		parsed,
		Record {
			id: 2,
			status: Status::Suspended {
				reason: "violation".into(),
			},
		}
	);
}

// -------------------------------------------------
// Flatten with is_value check
// -------------------------------------------------

#[test]
fn test_flatten_is_value() {
	assert!(Outer::is_value(&Value::Object(object! {
		name: "test",
		x: 1i64,
		y: 2i64
	})));

	// Missing regular field
	assert!(!Outer::is_value(&Value::Object(object! {
		x: 1i64,
		y: 2i64
	})));
}

// -------------------------------------------------
// Non-flatten still works normally
// -------------------------------------------------

#[derive(SurrealValue, Debug, PartialEq)]
struct NoFlatten {
	name: String,
	inner: Inner,
}

#[test]
fn test_no_flatten_nests_normally() {
	let val = NoFlatten {
		name: "test".into(),
		inner: Inner {
			x: 1,
			y: 2,
		},
	}
	.into_value();

	// Without flatten, inner is nested under "inner" key
	let expected = Value::Object(object! {
		name: "test",
		inner: Value::Object(object! { x: 1i64, y: 2i64 })
	});
	assert_eq!(val, expected);
}
