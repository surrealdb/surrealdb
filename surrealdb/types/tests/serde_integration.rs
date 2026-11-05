#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb_types::{
	Datetime, Number, Object, RecordId, RecordIdKey, SerdeWrapper, SurrealValue, Value,
};

#[derive(Clone, Debug, SurrealValue, Serialize, Deserialize, PartialEq, Eq)]
#[surreal(crate = "surrealdb_types")]
struct Requirement {
	name: String,
	intent: Option<String>,
	example: Option<String>,
	schema: String,
}

#[derive(Clone, Debug, SurrealValue, Serialize, Deserialize, PartialEq, Eq)]
#[surreal(crate = "surrealdb_types")]
#[serde(tag = "type", rename_all = "lowercase")]
enum Resource {
	Document {
		key: String,
		property: Option<String>,
	},
	Text {
		value: String,
	},
}

#[derive(Clone, Debug, SurrealValue, Serialize, Deserialize, PartialEq, Eq)]
#[surreal(crate = "surrealdb_types")]
struct Pipe {
	id: String,
	task_id: String,
	workspace_id: String,
	source: Option<Resource>,
	output: Requirement,
	blocked_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, SurrealValue, Serialize, Deserialize, PartialEq, Eq)]
#[surreal(crate = "surrealdb_types")]
struct PipeRecord {
	id: Option<RecordId>,
	task: RecordId,
	workspace: RecordId,
	source: Option<Resource>,
	output: Requirement,
	blocked_at: Option<DateTime<Utc>>,
}

impl PipeRecord {
	fn new(output: Requirement, task_id: &str, workspace_id: &str) -> Self {
		Self {
			id: None,
			task: RecordId::new("task", task_id),
			workspace: RecordId::new("workspace", workspace_id),
			source: Some(Resource::Document {
				key: "brief-001".to_string(),
				property: Some("body".to_string()),
			}),
			output,
			blocked_at: Some(DateTime::from(Datetime::MIN_UTC)),
		}
	}
}

impl TryFrom<PipeRecord> for Pipe {
	type Error = String;

	fn try_from(value: PipeRecord) -> Result<Self, Self::Error> {
		let id = match value.id {
			Some(record_id) => record_key_to_string(record_id.key)?,
			None => return Err("missing id".to_string()),
		};
		let task_id = record_key_to_string(value.task.key)?;
		let workspace_id = record_key_to_string(value.workspace.key)?;

		Ok(Self {
			id,
			task_id,
			workspace_id,
			source: value.source,
			output: value.output,
			blocked_at: value.blocked_at,
		})
	}
}

impl From<Pipe> for PipeRecord {
	fn from(value: Pipe) -> Self {
		Self {
			id: Some(RecordId::new("pipe", value.id)),
			task: RecordId::new("task", value.task_id),
			workspace: RecordId::new("workspace", value.workspace_id),
			source: value.source,
			output: value.output,
			blocked_at: value.blocked_at,
		}
	}
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ApiResource {
	Document {
		key: String,
		property: Option<String>,
	},
	Text {
		value: String,
	},
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ApiPayload {
	title: String,
	resource: ApiResource,
	attempts: u128,
	meta: BTreeMap<String, bool>,
}

fn record_key_to_string(key: RecordIdKey) -> Result<String, String> {
	match key {
		RecordIdKey::String(v) => Ok(v),
		RecordIdKey::Number(v) => Ok(v.to_string()),
		other @ (RecordIdKey::Uuid(_)
		| RecordIdKey::Array(_)
		| RecordIdKey::Object(_)
		| RecordIdKey::Range(_)) => Err(format!("unsupported record key in test: {other:?}")),
	}
}

fn base_requirement() -> Requirement {
	Requirement {
		name: "summary".to_string(),
		intent: Some("human readable brief".to_string()),
		example: Some("Summarize customer call".to_string()),
		schema: r#"{ "type": "string" }"#.to_string(),
	}
}

#[test]
fn legacy_pipe_record_roundtrip_value_and_json() {
	let record = PipeRecord::new(base_requirement(), "task-01", "workspace-01");
	let value = record.clone().into_value();

	let Value::Object(object) = &value else {
		panic!("expected object value");
	};
	assert!(object.contains_key("task"));
	assert!(object.contains_key("workspace"));
	assert!(object.contains_key("source"));
	assert!(object.contains_key("blocked_at"));

	let restored = PipeRecord::from_value(value).expect("pipe record should roundtrip");
	assert_eq!(restored, record);

	let json = serde_json::to_value(&record).expect("json conversion should work");
	assert_eq!(json["source"]["type"], "document");
	assert_eq!(json["source"]["key"], "brief-001");
	assert_eq!(json["source"]["property"], "body");
	assert!(json["blocked_at"].is_string());
}

#[test]
fn legacy_pipe_domain_conversion_uses_record_ids() {
	let pipe = Pipe {
		id: "pipe-42".to_string(),
		task_id: "task-77".to_string(),
		workspace_id: "workspace-88".to_string(),
		source: Some(Resource::Text {
			value: "input context".to_string(),
		}),
		output: base_requirement(),
		blocked_at: Some(DateTime::from(Datetime::MIN_UTC)),
	};

	let record: PipeRecord = pipe.clone().into();
	assert_eq!(record.id, Some(RecordId::new("pipe", "pipe-42")));
	assert_eq!(record.task, RecordId::new("task", "task-77"));
	assert_eq!(record.workspace, RecordId::new("workspace", "workspace-88"));

	let pipe_back = Pipe::try_from(record).expect("conversion should succeed");
	assert_eq!(pipe_back, pipe);
}

#[test]
fn serde_wrapper_supports_tagged_enums_large_integers_and_map_keys() {
	let payload = ApiPayload {
		title: "ingestion".to_string(),
		resource: ApiResource::Document {
			key: "doc:1".to_string(),
			property: Some("content".to_string()),
		},
		attempts: (i64::MAX as u128) + 10,
		meta: BTreeMap::from([("n-1".to_string(), true), ("2".to_string(), false)]),
	};

	let wrapper_value = SerdeWrapper(payload).into_value();
	let Value::Object(object) = &wrapper_value else {
		panic!("wrapper should serialize payload as object");
	};

	assert_eq!(object.get("title"), Some(&Value::String("ingestion".to_string())));
	assert!(matches!(object.get("attempts"), Some(Value::Number(Number::Decimal(_)))));
	assert!(matches!(object.get("meta"), Some(Value::Object(_))));

	let restored = SerdeWrapper::<ApiPayload>::from_value(wrapper_value)
		.expect("wrapper should deserialize payload");

	assert_eq!(
		restored.0.resource,
		ApiResource::Document {
			key: "doc:1".to_string(),
			property: Some("content".to_string())
		}
	);
	assert_eq!(restored.0.attempts, (i64::MAX as u128) + 10);
	assert_eq!(
		restored.0.meta,
		BTreeMap::from([("n-1".to_string(), true), ("2".to_string(), false)])
	);
}

#[test]
fn wrapper_deserializes_unit_and_tagged_enum_forms() {
	#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
	enum Kind {
		Alpha,
		Beta {
			stage: u8,
		},
	}

	let unit = SerdeWrapper(Kind::Alpha).into_value();
	assert_eq!(unit, Value::String("Alpha".to_string()));
	let unit_back =
		SerdeWrapper::<Kind>::from_value(unit).expect("unit variant should deserialize");
	assert_eq!(unit_back.0, Kind::Alpha);

	let tagged = SerdeWrapper(Kind::Beta {
		stage: 3,
	})
	.into_value();
	let Value::Object(obj) = &tagged else {
		panic!("struct enum variant should be object");
	};
	assert!(obj.contains_key("Beta"));

	let tagged_back =
		SerdeWrapper::<Kind>::from_value(tagged).expect("struct variant should deserialize");
	assert_eq!(
		tagged_back.0,
		Kind::Beta {
			stage: 3
		}
	);
}

#[test]
fn object_deserializer_rejects_enum_maps_with_multiple_keys() {
	#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
	enum MultiVariant {
		First {
			value: String,
		},
		Second {
			value: String,
		},
	}

	let mut bad = Object::new();
	bad.insert(
		"First",
		Value::Object(Object::from(BTreeMap::from([(
			"key".to_string(),
			Value::String("doc-1".to_string()),
		)]))),
	);
	bad.insert(
		"Second",
		Value::Object(Object::from(BTreeMap::from([(
			"value".to_string(),
			Value::String("hello".to_string()),
		)]))),
	);

	let err = match SerdeWrapper::<MultiVariant>::from_value(Value::Object(bad)) {
		Ok(_) => panic!("enum map with two keys should fail"),
		Err(err) => err,
	};
	let message = err.to_string();
	assert!(message.contains("map with a single key"), "{message}");
}

#[test]
fn serde_json_and_surreal_value_align_for_renamed_fields() {
	#[derive(Clone, Debug, SurrealValue, Serialize, Deserialize, PartialEq, Eq)]
	#[surreal(crate = "surrealdb_types")]
	struct Person {
		#[surreal(rename = "full_name")]
		#[serde(rename = "full_name")]
		name: String,
		age: i64,
	}

	let person = Person {
		name: "Alice".to_string(),
		age: 30,
	};

	let value = person.clone().into_value();
	let Value::Object(object) = &value else {
		panic!("person should serialize as object");
	};
	assert!(object.contains_key("full_name"));
	assert!(!object.contains_key("name"));
	assert_eq!(object.get("age"), Some(&Value::Number(Number::Int(30))));

	let json = serde_json::to_value(&person).expect("json conversion should work");
	assert_eq!(json["full_name"], "Alice");
	assert_eq!(json["age"], 30);

	let from_value = Person::from_value(value).expect("value conversion should work");
	assert_eq!(from_value, person);
}

#[test]
fn wrapper_serializes_bytes_as_value_bytes_and_roundtrips() {
	#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
	struct BinaryPayload {
		name: String,
		data: Vec<u8>,
	}

	let payload = BinaryPayload {
		name: "blob".to_string(),
		data: vec![1, 2, 3, 4, 5],
	};

	let value = SerdeWrapper(payload).into_value();
	let Value::Object(object) = &value else {
		panic!("binary payload should serialize as object");
	};
	assert!(matches!(object.get("data"), Some(Value::Array(_))));
	let Value::Array(data) = object.get("data").expect("data should be present") else {
		panic!("data should be an array");
	};
	assert_eq!(data.len(), 5);
	assert_eq!(data[0], Value::Number(Number::Int(1)));

	let restored = SerdeWrapper::<BinaryPayload>::from_value(value)
		.expect("binary payload should deserialize");
	assert_eq!(
		restored.0,
		BinaryPayload {
			name: "blob".to_string(),
			data: vec![1, 2, 3, 4, 5]
		}
	);
}

#[test]
fn wrapper_reports_error_for_numeric_map_keys() {
	#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
	struct NumericKeyMap {
		meta: BTreeMap<i64, bool>,
	}

	let value = SerdeWrapper(NumericKeyMap {
		meta: BTreeMap::from([(-1, true)]),
	})
	.into_value();

	let err = match SerdeWrapper::<NumericKeyMap>::from_value(value) {
		Ok(_) => panic!("numeric key map should fail roundtrip"),
		Err(err) => err,
	};
	let message = err.to_string();
	assert!(message.contains("expected i64"), "{message}");
}

#[test]
fn wrapper_roundtrip_for_nested_arrays_and_objects() {
	#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
	struct Nested {
		items: Vec<ApiResource>,
		labels: BTreeMap<String, Vec<String>>,
	}

	let nested = Nested {
		items: vec![
			ApiResource::Text {
				value: "hello".to_string(),
			},
			ApiResource::Document {
				key: "doc-9".to_string(),
				property: None,
			},
		],
		labels: BTreeMap::from([(
			"priority".to_string(),
			vec!["p0".to_string(), "urgent".to_string()],
		)]),
	};

	let value = SerdeWrapper(nested).into_value();
	let Value::Object(obj) = &value else {
		panic!("nested payload should serialize as object");
	};
	assert!(matches!(obj.get("items"), Some(Value::Array(_))));
	assert!(matches!(obj.get("labels"), Some(Value::Object(_))));

	let restored =
		SerdeWrapper::<Nested>::from_value(value).expect("nested payload should deserialize");
	assert_eq!(
		restored.0,
		Nested {
			items: vec![
				ApiResource::Text {
					value: "hello".to_string()
				},
				ApiResource::Document {
					key: "doc-9".to_string(),
					property: None
				}
			],
			labels: BTreeMap::from([(
				"priority".to_string(),
				vec!["p0".to_string(), "urgent".to_string()]
			)])
		}
	);
}

/// An IP address is stored as its text and read back from it, whichever
/// family it is, through serde and through `SurrealValue` alike.
#[test]
fn ip_addresses_roundtrip_as_strings() {
	use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

	#[derive(Clone, Debug, SurrealValue, Serialize, Deserialize, PartialEq)]
	#[surreal(crate = "surrealdb_types")]
	struct Peer {
		ip: IpAddr,
		v4: Ipv4Addr,
		v6: Ipv6Addr,
	}

	let peer = Peer {
		ip: "127.0.0.1".parse().unwrap(),
		v4: Ipv4Addr::new(10, 0, 0, 1),
		v6: "::1".parse().unwrap(),
	};
	let value = SerdeWrapper(peer.clone()).into_value();
	let Value::Object(obj) = &value else {
		panic!("peer should serialize as object");
	};
	assert_eq!(obj.get("ip"), Some(&Value::String("127.0.0.1".to_string())));
	assert_eq!(obj.get("v4"), Some(&Value::String("10.0.0.1".to_string())));
	assert_eq!(obj.get("v6"), Some(&Value::String("::1".to_string())));
	assert_eq!(peer.clone().into_value(), value);

	let restored = SerdeWrapper::<Peer>::from_value(value.clone()).expect("serde should read it");
	assert_eq!(restored.0, peer);
	assert_eq!(Peer::from_value(value).expect("SurrealValue should read it"), peer);

	assert!(IpAddr::from_value(Value::String("not an address".to_string())).is_err());
	assert!(Ipv4Addr::from_value(Value::String("::1".to_string())).is_err());
}

#[test]
fn unsigned_integers_wider_than_i64_roundtrip() {
	// An integer above `i64::MAX` is stored as a whole-number Decimal and
	// deserializes back into the field it came from.
	#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
	struct Nums {
		u64_max: u64,
		just_over_i64: u64,
		i64_min: i64,
		u32_max: u32,
	}

	let input = Nums {
		u64_max: u64::MAX,
		just_over_i64: (i64::MAX as u64) + 1,
		i64_min: i64::MIN,
		u32_max: u32::MAX,
	};

	let value = SerdeWrapper(input).into_value();
	let Value::Object(object) = &value else {
		panic!("numbers should serialize as an object");
	};

	// Stored intact, as a Decimal, rather than wrapped to a negative Int.
	assert!(matches!(object.get("u64_max"), Some(Value::Number(Number::Decimal(_)))));
	// A value that still fits stays an Int.
	assert_eq!(object.get("u32_max"), Some(&Value::Number(Number::Int(u32::MAX as i64))));

	let SerdeWrapper(output): SerdeWrapper<Nums> =
		SerdeWrapper::from_value(value).expect("wide integers should deserialize");
	assert_eq!(output.u64_max, u64::MAX);
	assert_eq!(output.just_over_i64, (i64::MAX as u64) + 1);
	assert_eq!(output.i64_min, i64::MIN);
	assert_eq!(output.u32_max, u32::MAX);
}

#[test]
fn surreal_value_integers_wider_than_i64_roundtrip() {
	// An integer above `i64::MAX` is stored as a whole-number Decimal and
	// converts back into the field it came from.
	#[derive(Debug, SurrealValue, PartialEq, Eq)]
	#[surreal(crate = "surrealdb_types")]
	struct Nums {
		u64_max: u64,
		just_over_i64: u64,
		usize_max: usize,
		i64_min: i64,
		u32_max: u32,
	}

	let input = Nums {
		u64_max: u64::MAX,
		just_over_i64: (i64::MAX as u64) + 1,
		usize_max: usize::MAX,
		i64_min: i64::MIN,
		u32_max: u32::MAX,
	};

	let value = input.into_value();
	let Value::Object(object) = &value else {
		panic!("numbers should convert to an object");
	};

	// Stored intact, as a Decimal, rather than wrapped to a negative Int.
	assert!(matches!(object.get("u64_max"), Some(Value::Number(Number::Decimal(_)))));
	assert!(matches!(object.get("usize_max"), Some(Value::Number(Number::Decimal(_)))));
	assert_eq!(object.get("u32_max"), Some(&Value::Number(Number::Int(u32::MAX as i64))));

	let output = Nums::from_value(value).expect("wide integers should convert back");
	assert_eq!(output.u64_max, u64::MAX);
	assert_eq!(output.just_over_i64, (i64::MAX as u64) + 1);
	assert_eq!(output.usize_max, usize::MAX);
	assert_eq!(output.i64_min, i64::MIN);
	assert_eq!(output.u32_max, u32::MAX);

	// A whole-number Decimal converts into every integer type it fits in, i64
	// included.
	let five = Value::Number(Number::Decimal(rust_decimal::Decimal::from(5)));
	assert_eq!(i64::from_value(five.clone()).unwrap(), 5);
	assert_eq!(i32::from_value(five.clone()).unwrap(), 5);
	assert!(five.is_i64_and(|n| *n == 5));

	// A fractional Decimal is refused, and the error says why.
	let fraction = Value::Number(Number::Decimal(rust_decimal::Decimal::new(15, 1)));
	let error = u64::from_value(fraction.clone()).unwrap_err().to_string();
	assert!(error.contains("fractional part"), "{error}");
	assert!(i64::from_value(fraction).is_err());

	// A whole Decimal that does not fit is refused.
	let too_big = Value::Number(Number::Decimal(rust_decimal::Decimal::from(u64::MAX)));
	assert!(u32::from_value(too_big.clone()).is_err());
	assert_eq!(too_big.as_u64(), Some(u64::MAX));
}

#[test]
fn fractional_decimals_deserialize_without_truncation() {
	// A fractional Decimal is offered to serde as `f64`: a float or an untyped
	// target keeps the fraction, and an integer target refuses it.
	let price = Value::Number(Number::Decimal(rust_decimal::Decimal::new(1999, 2)));

	let SerdeWrapper(as_f64): SerdeWrapper<f64> = SerdeWrapper::from_value(price.clone())
		.expect("a fractional Decimal should deserialize as f64");
	assert_eq!(as_f64, 19.99);

	let SerdeWrapper(as_json): SerdeWrapper<serde_json::Value> =
		SerdeWrapper::from_value(price.clone())
			.expect("a fractional Decimal should deserialize as JSON");
	assert_eq!(as_json, serde_json::json!(19.99));

	assert!(SerdeWrapper::<i128>::from_value(price.clone()).is_err());
	assert!(SerdeWrapper::<u64>::from_value(price.clone()).is_err());

	// A Decimal target keeps the exact value, including digits an f64 cannot
	// hold. An untyped target receives such a value as its decimal text rather
	// than a rounded float.
	let SerdeWrapper(as_decimal): SerdeWrapper<rust_decimal::Decimal> =
		SerdeWrapper::from_value(price).expect("a Decimal should deserialize as a Decimal");
	assert_eq!(as_decimal, rust_decimal::Decimal::new(1999, 2));

	let precise: rust_decimal::Decimal = "0.1234567890123456789012345678".parse().unwrap();
	let precise_value = Value::Number(Number::Decimal(precise));
	let SerdeWrapper(as_decimal): SerdeWrapper<rust_decimal::Decimal> =
		SerdeWrapper::from_value(precise_value.clone())
			.expect("a precise Decimal should deserialize as a Decimal");
	assert_eq!(as_decimal, precise);

	let SerdeWrapper(as_json): SerdeWrapper<serde_json::Value> =
		SerdeWrapper::from_value(precise_value)
			.expect("a precise Decimal should deserialize as JSON");
	assert_eq!(as_json, serde_json::json!("0.1234567890123456789012345678"));
}
