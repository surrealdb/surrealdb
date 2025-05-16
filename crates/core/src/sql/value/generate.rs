use crate::err::Error;
use crate::sql::id::Id;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub(crate) fn generate(self, tb: &Table, retable: bool) -> Result<Thing, Error> {
		match self {
			// There is a floating point number for the id field
			SqlValue::Number(id) if id.is_float() => Ok(Thing {
				tb: tb.0.to_string(),
				id: id.as_int().into(),
			}),
			// There is an integer number for the id field
			SqlValue::Number(id) if id.is_int() => Ok(Thing {
				tb: tb.0.to_string(),
				id: id.as_int().into(),
			}),
			// There is a string for the id field
			SqlValue::Strand(id) if !id.is_empty() => Ok(Thing {
				tb: tb.0.to_string(),
				id: id.into(),
			}),
			// There is an object for the id field
			SqlValue::Object(id) => Ok(Thing {
				tb: tb.0.to_string(),
				id: id.into(),
			}),
			// There is an array for the id field
			SqlValue::Array(id) => Ok(Thing {
				tb: tb.0.to_string(),
				id: id.into(),
			}),
			// There is a UUID for the id field
			SqlValue::Uuid(id) => Ok(Thing {
				tb: tb.0.to_string(),
				id: id.into(),
			}),
			// There is no record id field
			SqlValue::None => Ok(Thing {
				tb: tb.0.to_string(),
				id: Id::rand(),
			}),
			// There is a record id defined
			SqlValue::Thing(id) => match retable {
				// Let's re-table this record id
				true => Ok(Thing {
					tb: tb.0.to_string(),
					id: id.id,
				}),
				// Let's use the specified record id
				false => match tb.0 == id.tb {
					// The record is from the same table
					true => Ok(id),
					// The record id is from another table
					false => Ok(Thing {
						tb: tb.0.to_string(),
						id: id.id,
					}),
				},
			},
			// Any other value is wrong
			id => Err(Error::IdInvalid {
				value: id.to_string(),
			}),
		}
	}
}
