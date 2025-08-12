use anyhow::Result;

use crate::err::Error;
use crate::val::{RecordId, RecordIdKey, Strand, Value};

impl Value {
	pub(crate) fn generate(self, tb: Strand, retable: bool) -> Result<RecordId> {
		match self {
			// There is a floating point number for the id field
			Value::Number(id) if id.is_float() => Ok(RecordId {
				table: tb.into_string(),
				key: RecordIdKey::Number(id.as_int()),
			}),
			// There is an integer number for the id field
			Value::Number(id) if id.is_int() => Ok(RecordId {
				table: tb.into_string(),
				key: RecordIdKey::Number(id.as_int()),
			}),
			// There is a string for the id field
			Value::Strand(id) if !id.is_empty() => Ok(RecordId {
				table: tb.into_string(),
				key: id.into(),
			}),
			// There is an object for the id field
			Value::Object(id) => Ok(RecordId {
				table: tb.into_string(),
				key: id.into(),
			}),
			// There is an array for the id field
			Value::Array(id) => Ok(RecordId {
				table: tb.into_string(),
				key: id.into(),
			}),
			// There is a UUID for the id field
			Value::Uuid(id) => Ok(RecordId {
				table: tb.into_string(),
				key: id.into(),
			}),
			// There is no record id field
			Value::None => Ok(RecordId {
				table: tb.into_string(),
				key: RecordIdKey::rand(),
			}),
			// There is a record id defined
			Value::RecordId(id) => {
				if retable {
					// Let's re-table this record id
					Ok(RecordId {
						table: tb.into_string(),
						key: id.key,
					})
				} else {
					// Let's use the specified record id
					if *tb == id.table {
						// The record is from the same table
						Ok(id)
					} else {
						// The record id is from another table
						Ok(RecordId {
							table: tb.into_string(),
							key: id.key,
						})
					}
				}
			}
			// Any other value is wrong
			id => Err(anyhow::Error::new(Error::IdInvalid {
				value: id.to_string(),
			})),
		}
	}
}
