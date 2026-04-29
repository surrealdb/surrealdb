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
		other => Err(format!("unsupported record key in test: {other:?}")),
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
