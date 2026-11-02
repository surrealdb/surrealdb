//! How a SurrealDB result is shown to an SQL client.

use surrealdb_types::{Number, RecordIdKey, Set, ToSql, Value};

/// Replace every record id in `value`, at any depth, by its key.
///
/// SQL has no table-qualified values: a primary key or a foreign key is the
/// bare key, so a client shown `person:1` could neither compare it with the `1`
/// it inserted nor join on it. Arrays and objects are changed in place, so the
/// parts of a result that hold no record id are not reallocated. A set that
/// holds a record id becomes an array of its members' keys: records of different
/// tables can have one key, and a set would keep only one of them. Every other
/// value is left as it is, so a value that already holds keys comes back
/// unchanged.
pub fn show_record_ids_as_keys(mut value: Value) -> Value {
	show_in_place(&mut value);
	value
}

fn show_in_place(value: &mut Value) {
	match value {
		Value::RecordId(_) => {
			if let Value::RecordId(record) = std::mem::replace(value, Value::None) {
				*value = key_value(record.key);
			}
		}
		Value::Array(array) => array.iter_mut().for_each(show_in_place),
		Value::Object(object) => object.values_mut().for_each(show_in_place),
		Value::Set(set) if set.iter().any(holds_record_id) => {
			let mut members: Vec<Value> = std::mem::replace(set, Set::new()).into_iter().collect();
			members.iter_mut().for_each(show_in_place);
			*value = Value::Array(members.into());
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Set(_) => {}
	}
}

/// Whether `value` is, or holds at any depth, a record id.
fn holds_record_id(value: &Value) -> bool {
	match value {
		Value::RecordId(_) => true,
		Value::Array(array) => array.iter().any(holds_record_id),
		Value::Object(object) => object.values().any(holds_record_id),
		Value::Set(set) => set.iter().any(holds_record_id),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_) => false,
	}
}

/// A record id key as a plain value; a range key, which has no plain value,
/// becomes its text.
fn key_value(key: RecordIdKey) -> Value {
	match key {
		RecordIdKey::Number(number) => Value::Number(Number::Int(number)),
		RecordIdKey::String(text) => Value::String(text),
		RecordIdKey::Uuid(uuid) => Value::Uuid(uuid),
		RecordIdKey::Array(array) => show_record_ids_as_keys(Value::Array(array)),
		RecordIdKey::Object(object) => show_record_ids_as_keys(Value::Object(object)),
		range @ RecordIdKey::Range(_) => Value::String(range.to_sql()),
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{Array, Object, RecordId};

	use super::*;

	fn record(table: &str, key: RecordIdKey) -> Value {
		Value::RecordId(RecordId::new(table, key))
	}

	fn int(i: i64) -> Value {
		Value::Number(Number::Int(i))
	}

	fn array(values: Vec<Value>) -> Value {
		Value::Array(Array::from(values))
	}

	#[test]
	fn a_record_id_shows_as_its_key() {
		assert_eq!(show_record_ids_as_keys(record("person", RecordIdKey::Number(1))), int(1));
		assert_eq!(
			show_record_ids_as_keys(record("tag", RecordIdKey::String("rust".into()))),
			Value::String("rust".into())
		);
	}

	#[test]
	fn record_ids_inside_arrays_objects_and_sets_show_as_keys() {
		let mut object = Object::new();
		object.insert("owner".to_string(), record("person", RecordIdKey::Number(7)));
		object.insert("n".to_string(), int(3));
		let nested = array(vec![
			record("person", RecordIdKey::Number(1)),
			array(vec![record("person", RecordIdKey::Number(2))]),
			Value::Object(object),
		]);
		let mut expected_object = Object::new();
		expected_object.insert("owner".to_string(), int(7));
		expected_object.insert("n".to_string(), int(3));
		assert_eq!(
			show_record_ids_as_keys(nested),
			array(vec![int(1), array(vec![int(2)]), Value::Object(expected_object)])
		);
		let set = Value::Set(Set::from(vec![record("person", RecordIdKey::Number(5))]));
		assert_eq!(show_record_ids_as_keys(set), array(vec![int(5)]));
	}

	#[test]
	fn records_of_different_tables_with_one_key_keep_every_member_of_a_set() {
		let set = Value::Set(Set::from(vec![
			record("person", RecordIdKey::Number(1)),
			record("department", RecordIdKey::Number(1)),
		]));
		assert_eq!(show_record_ids_as_keys(set), array(vec![int(1), int(1)]));
	}

	#[test]
	fn a_value_without_record_ids_is_unchanged() {
		let value = array(vec![int(1), Value::String("a".into()), Value::None, Value::Null]);
		assert_eq!(show_record_ids_as_keys(value.clone()), value);
		let set = Value::Set(Set::from(vec![int(1), Value::String("a".into())]));
		assert_eq!(show_record_ids_as_keys(set.clone()), set);
	}

	#[test]
	fn a_composite_key_shows_as_its_members() {
		let key =
			RecordIdKey::Array(Array::from(vec![int(1), record("person", RecordIdKey::Number(9))]));
		assert_eq!(show_record_ids_as_keys(record("pair", key)), array(vec![int(1), int(9)]));
	}
}
