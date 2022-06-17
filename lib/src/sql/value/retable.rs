use crate::err::Error;
use crate::sql::id::Id;
use crate::sql::paths::ID;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;

impl Value {
	pub fn retable(&self, val: &Table) -> Result<Thing, Error> {
		// Fetch the id from the document
		let id = match self.pick(&*ID) {
			Value::Number(id) if id.is_float() => Thing {
				tb: val.to_string(),
				id: Id::Number(id.as_int()),
			},
			Value::Number(id) if id.is_int() => Thing {
				tb: val.to_string(),
				id: Id::Number(id.as_int()),
			},
			Value::Strand(id) => Thing {
				tb: val.to_string(),
				id: Id::String(id.0),
			},
			Value::Thing(id) => Thing {
				tb: val.to_string(),
				id: id.id,
			},
			Value::None => Thing {
				tb: val.to_string(),
				id: Id::rand(),
			},
			id => {
				return Err(Error::IdInvalid {
					value: id.to_string(),
				})
			}
		};
		// Return the record id
		Ok(id)
	}
}
