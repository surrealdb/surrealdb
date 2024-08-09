use crate::err::Error;
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Idioms;
use crate::sql::Strand;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineIndexStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineIndexStatement, Error>;
	type SerializeTuple = Impossible<DefineIndexStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineIndexStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineIndexStatement, Error>;
	type SerializeMap = Impossible<DefineIndexStatement, Error>;
	type SerializeStruct = SerializeDefineIndexStatement;
	type SerializeStructVariant = Impossible<DefineIndexStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineIndexStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineIndexStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineIndexStatement {
	name: Ident,
	what: Ident,
	cols: Idioms,
	index: Index,
	comment: Option<Strand>,
	if_not_exists: bool,
	overwrite: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineIndexStatement {
	type Ok = DefineIndexStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"what" => {
				self.what = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"cols" => {
				self.cols = Idioms(value.serialize(ser::idiom::vec::Serializer.wrap())?);
			}
			"index" => {
				self.index = value.serialize(ser::index::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			"overwrite" => {
				self.overwrite = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineIndexStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineIndexStatement {
			name: self.name,
			what: self.what,
			cols: self.cols,
			index: self.index,
			comment: self.comment,
			if_not_exists: self.if_not_exists,
			overwrite: self.overwrite,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineIndexStatement::default();
		let value: DefineIndexStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
