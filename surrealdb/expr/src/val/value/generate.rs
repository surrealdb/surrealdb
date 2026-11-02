#![deny(clippy::wildcard_enum_match_arm)]

use anyhow::Result;
use surrealdb_types::ToSql;

use crate::expr::Error;
use crate::val::{Number, RecordId, RecordIdKey, TableName, Value};

impl Value {
	pub fn generate(self, tb: TableName, retable: bool) -> Result<RecordId> {
		match self {
			// There is a number for the id field. An integer of any width is the key it
			// states. A float is accepted only if it round-trips exactly to an i64 (finite,
			// whole, in range): lossy conversions are rejected — see
			// `Number::as_int_lossless`. A decimal is not a key.
			Value::Number(id) if !matches!(id, Number::Decimal(_)) => {
				let key = id.as_int_lossless().map(RecordIdKey::Number).ok_or_else(|| {
					anyhow::Error::new(Error::IdInvalid {
						value: Value::Number(id).to_sql(),
					})
				})?;
				Ok(RecordId {
					table: tb,
					key,
				})
			}
			// There is a string for the id field
			Value::String(id) if !id.is_empty() => Ok(RecordId {
				table: tb,
				key: id.into(),
			}),
			// There is an object for the id field
			Value::Object(id) => Ok(RecordId {
				table: tb,
				key: id.into(),
			}),
			// There is an array for the id field
			Value::Array(id) => Ok(RecordId {
				table: tb,
				key: id.into(),
			}),
			// There is a UUID for the id field
			Value::Uuid(id) => Ok(RecordId {
				table: tb,
				key: id.into(),
			}),
			// There is no record id field
			Value::None => Ok(RecordId {
				table: tb,
				key: RecordIdKey::rand(),
			}),
			// There is a record id defined
			Value::RecordId(id) => {
				if retable {
					// Let's re-table this record id
					Ok(RecordId {
						table: tb,
						key: id.key,
					})
				} else {
					// Let's use the specified record id
					if tb == id.table {
						// The record is from the same table
						Ok(id)
					} else {
						// The record id is from another table
						Ok(RecordId {
							table: tb,
							key: id.key,
						})
					}
				}
			}
			// Any other value is wrong
			id @ (Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Set(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => Err(anyhow::Error::new(Error::IdInvalid {
				value: id.to_sql(),
			})),
		}
	}
}
