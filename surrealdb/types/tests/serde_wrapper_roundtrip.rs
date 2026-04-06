use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use surrealdb_types::{Datetime, Duration, SerdeWrapper, SurrealValue, Uuid, Value};

fn utc_ts(secs: i64, nanos: u32) -> chrono::DateTime<chrono::Utc> {
	Datetime::from_timestamp(secs, nanos).expect("valid timestamp").into_inner()
}

#[test]
fn wrapper_upstream_scalar_types_use_native_variants() {
	let uuid = uuid::Uuid::new_v4();
	let datetime = utc_ts(1_706_660_400, 111_000_000);
	let duration = std::time::Duration::new(123, 456);

	assert_eq!(SerdeWrapper(uuid).into_value(), Value::Uuid(Uuid::from(uuid)));
	assert_eq!(SerdeWrapper(datetime).into_value(), Value::Datetime(Datetime::from(datetime)));
	assert_eq!(SerdeWrapper(duration).into_value(), Value::Duration(Duration::from(duration)));

	assert_eq!(
		SerdeWrapper::<uuid::Uuid>::from_value(Value::Uuid(Uuid::from(uuid)))
			.expect("uuid roundtrip")
			.0,
		uuid
	);
	assert_eq!(
		SerdeWrapper::<chrono::DateTime<chrono::Utc>>::from_value(Value::Datetime(Datetime::from(
			datetime,
		)))
		.expect("datetime roundtrip")
		.0,
		datetime
	);
	assert_eq!(
		SerdeWrapper::<std::time::Duration>::from_value(Value::Duration(Duration::from(duration)))
			.expect("duration roundtrip")
			.0,
		duration
	);
}

#[test]
fn wrapper_upstream_container_types_use_native_variants_and_roundtrip() {
	let ids = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
	let events = vec![utc_ts(1_706_770_100, 1), utc_ts(1_706_770_101, 2)];
	let retries = vec![std::time::Duration::from_secs(1), std::time::Duration::from_millis(2500)];
	let optional_uuid = Some(uuid::Uuid::new_v4());
	let optional_event = Some(utc_ts(1_706_770_120, 3));
	let optional_retry = Some(std::time::Duration::new(7, 8));

	let id_map = BTreeMap::from([
		("a".to_string(), uuid::Uuid::new_v4()),
		("b".to_string(), uuid::Uuid::new_v4()),
	]);
	let event_map = BTreeMap::from([
		("start".to_string(), utc_ts(1_706_770_200, 4)),
		("end".to_string(), utc_ts(1_706_770_201, 5)),
	]);
	let retry_map = BTreeMap::from([
		("short".to_string(), std::time::Duration::from_secs(2)),
		("long".to_string(), std::time::Duration::from_secs(20)),
	]);

	let ids_value = SerdeWrapper(ids.clone()).into_value();
	let events_value = SerdeWrapper(events.clone()).into_value();
	let retries_value = SerdeWrapper(retries.clone()).into_value();
	assert!(matches!(
		ids_value,
		Value::Array(ref values) if values.iter().all(|v| matches!(v, Value::Uuid(_)))
	));
	assert!(matches!(
		events_value,
		Value::Array(ref values) if values.iter().all(|v| matches!(v, Value::Datetime(_)))
	));
	assert!(matches!(
		retries_value,
		Value::Array(ref values) if values.iter().all(|v| matches!(v, Value::Duration(_)))
	));

	assert_eq!(
		SerdeWrapper::<Vec<uuid::Uuid>>::from_value(ids_value).expect("uuid vec roundtrip").0,
		ids
	);
	assert_eq!(
		SerdeWrapper::<Vec<chrono::DateTime<chrono::Utc>>>::from_value(events_value)
			.expect("datetime vec roundtrip")
			.0,
		events
	);
	assert_eq!(
		SerdeWrapper::<Vec<std::time::Duration>>::from_value(retries_value)
			.expect("duration vec roundtrip")
			.0,
		retries
	);

	let optional_uuid_value = SerdeWrapper(optional_uuid).into_value();
	let optional_event_value = SerdeWrapper(optional_event).into_value();
	let optional_retry_value = SerdeWrapper(optional_retry).into_value();
	assert!(matches!(optional_uuid_value, Value::Uuid(_)));
	assert!(matches!(optional_event_value, Value::Datetime(_)));
	assert!(matches!(optional_retry_value, Value::Duration(_)));
	assert_eq!(
		SerdeWrapper::<Option<uuid::Uuid>>::from_value(optional_uuid_value)
			.expect("uuid option roundtrip")
			.0,
		optional_uuid
	);
	assert_eq!(
		SerdeWrapper::<Option<chrono::DateTime<chrono::Utc>>>::from_value(optional_event_value)
			.expect("datetime option roundtrip")
			.0,
		optional_event
	);
	assert_eq!(
		SerdeWrapper::<Option<std::time::Duration>>::from_value(optional_retry_value)
			.expect("duration option roundtrip")
			.0,
		optional_retry
	);

	let id_map_value = SerdeWrapper(id_map.clone()).into_value();
	let event_map_value = SerdeWrapper(event_map.clone()).into_value();
	let retry_map_value = SerdeWrapper(retry_map.clone()).into_value();
	assert!(matches!(
		id_map_value,
		Value::Object(ref values) if values.values().all(|v| matches!(v, Value::Uuid(_)))
	));
	assert!(matches!(
		event_map_value,
		Value::Object(ref values) if values.values().all(|v| matches!(v, Value::Datetime(_)))
	));
	assert!(matches!(
		retry_map_value,
		Value::Object(ref values) if values.values().all(|v| matches!(v, Value::Duration(_)))
	));
	assert_eq!(
		SerdeWrapper::<BTreeMap<String, uuid::Uuid>>::from_value(id_map_value)
			.expect("uuid map roundtrip")
			.0,
		id_map
	);
	assert_eq!(
		SerdeWrapper::<BTreeMap<String, chrono::DateTime<chrono::Utc>>>::from_value(
			event_map_value
		)
		.expect("datetime map roundtrip")
		.0,
		event_map
	);
	assert_eq!(
		SerdeWrapper::<BTreeMap<String, std::time::Duration>>::from_value(retry_map_value)
			.expect("duration map roundtrip")
			.0,
		retry_map
	);
}

#[test]
fn wrapper_upstream_option_none_uses_value_none() {
	let none_uuid: Option<uuid::Uuid> = None;
	let none_datetime: Option<chrono::DateTime<chrono::Utc>> = None;
	let none_duration: Option<std::time::Duration> = None;

	assert_eq!(SerdeWrapper(none_uuid).into_value(), Value::None);
	assert_eq!(SerdeWrapper(none_datetime).into_value(), Value::None);
	assert_eq!(SerdeWrapper(none_duration).into_value(), Value::None);
}

#[test]
fn wrapper_hashmap_types_use_native_variants_and_roundtrip() {
	let id_map = HashMap::from([
		("a".to_string(), uuid::Uuid::new_v4()),
		("b".to_string(), uuid::Uuid::new_v4()),
	]);
	let event_map = HashMap::from([
		("start".to_string(), utc_ts(1_706_770_200, 4)),
		("end".to_string(), utc_ts(1_706_770_201, 5)),
	]);
	let retry_map = HashMap::from([
		("short".to_string(), std::time::Duration::from_secs(2)),
		("long".to_string(), std::time::Duration::from_secs(20)),
	]);

	let id_map_value = SerdeWrapper(id_map.clone()).into_value();
	let event_map_value = SerdeWrapper(event_map.clone()).into_value();
	let retry_map_value = SerdeWrapper(retry_map.clone()).into_value();
	assert!(matches!(
		id_map_value,
		Value::Object(ref values) if values.values().all(|v| matches!(v, Value::Uuid(_)))
	));
	assert!(matches!(
		event_map_value,
		Value::Object(ref values) if values.values().all(|v| matches!(v, Value::Datetime(_)))
	));
	assert!(matches!(
		retry_map_value,
		Value::Object(ref values) if values.values().all(|v| matches!(v, Value::Duration(_)))
	));
	assert_eq!(
		SerdeWrapper::<HashMap<String, uuid::Uuid>>::from_value(id_map_value)
			.expect("uuid hashmap roundtrip")
			.0,
		id_map
	);
	assert_eq!(
		SerdeWrapper::<HashMap<String, chrono::DateTime<chrono::Utc>>>::from_value(event_map_value)
			.expect("datetime hashmap roundtrip")
			.0,
		event_map
	);
	assert_eq!(
		SerdeWrapper::<HashMap<String, std::time::Duration>>::from_value(retry_map_value)
			.expect("duration hashmap roundtrip")
			.0,
		retry_map
	);
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
enum UntaggedEvent {
	Timed {
		ts: chrono::DateTime<chrono::Utc>,
	},
	Named {
		name: String,
	},
}

#[test]
fn deserialize_any_handles_native_datetime_in_untagged_enum() {
	let ts = utc_ts(1_706_660_400, 0);
	let value = Value::Object(surrealdb_types::Object::from(BTreeMap::from([(
		"ts".to_string(),
		Value::Datetime(Datetime::from(ts)),
	)])));

	let result = UntaggedEvent::deserialize(value).expect("untagged datetime should deserialize");
	assert_eq!(
		result,
		UntaggedEvent::Timed {
			ts
		}
	);
}

#[test]
fn deserialize_any_handles_native_uuid() {
	let uuid = uuid::Uuid::new_v4();
	let value = Value::Uuid(Uuid::from(uuid));

	let result = String::deserialize(value);
	assert!(result.is_err(), "uuid is not a plain string, type mismatch expected");

	let value = Value::Uuid(Uuid::from(uuid));
	let result =
		serde_json::Value::deserialize(value).expect("serde_json::Value uses deserialize_any");
	assert_eq!(result, serde_json::Value::String(uuid.to_string()));
}

#[test]
fn deserialize_any_handles_native_duration() {
	let dur = std::time::Duration::new(42, 7);
	let value = Value::Duration(Duration::from(dur));

	let result =
		serde_json::Value::deserialize(value).expect("serde_json::Value uses deserialize_any");
	let obj = result.as_object().expect("duration should deserialize to object");
	assert_eq!(obj.get("secs"), Some(&serde_json::Value::from(42u64)));
	assert_eq!(obj.get("nanos"), Some(&serde_json::Value::from(7u32)));
}

#[test]
fn deserialize_any_handles_native_record_id() {
	let rid = surrealdb_types::RecordId::new("person", "alice");
	let value = Value::RecordId(rid);

	let result =
		serde_json::Value::deserialize(value).expect("serde_json::Value uses deserialize_any");
	let obj = result.as_object().expect("record_id should deserialize to object");
	assert_eq!(obj.get("table"), Some(&serde_json::Value::String("person".to_string())));
}
