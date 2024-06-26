mod since;

use crate::err::Error;
use crate::sql::statements::show::ShowSince;
use crate::sql::statements::show::ShowStatement;
use crate::sql::value::serde::ser;
use crate::sql::Table;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = ShowStatement;
	type Error = Error;

	type SerializeSeq = Impossible<ShowStatement, Error>;
	type SerializeTuple = Impossible<ShowStatement, Error>;
	type SerializeTupleStruct = Impossible<ShowStatement, Error>;
	type SerializeTupleVariant = Impossible<ShowStatement, Error>;
	type SerializeMap = Impossible<ShowStatement, Error>;
	type SerializeStruct = SerializeShowStatement;
	type SerializeStructVariant = Impossible<ShowStatement, Error>;

	const EXPECTED: &'static str = "a struct `ShowStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeShowStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeShowStatement {
	table: Option<Table>,
	since: Option<ShowSince>,
	limit: Option<u32>,
}

impl serde::ser::SerializeStruct for SerializeShowStatement {
	type Ok = ShowStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"table" => {
				self.table = value.serialize(ser::table::opt::Serializer.wrap())?;
			}
			"since" => {
				self.since = Some(value.serialize(since::Serializer.wrap())?);
			}
			"limit" => {
				self.limit = value.serialize(ser::primitive::u32::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `ShowStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match self.since {
			Some(since) => Ok(ShowStatement {
				since,
				table: self.table,
				limit: self.limit,
			}),
			None => Err(Error::custom("`ShowStatement` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	impl Default for ShowSince {
		fn default() -> Self {
			ShowSince::Timestamp(Default::default())
		}
	}

	#[allow(clippy::derivable_impls)]
	impl Default for ShowStatement {
		fn default() -> Self {
			ShowStatement {
				table: None,
				since: Default::default(),
				limit: None,
			}
		}
	}

	#[test]
	fn default() {
		let stmt = ShowStatement::default();
		let value: ShowStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_table() {
		let stmt = ShowStatement {
			table: Some(Default::default()),
			..Default::default()
		};
		let value: ShowStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_limit() {
		let stmt = ShowStatement {
			limit: Some(Default::default()),
			..Default::default()
		};
		let value: ShowStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
