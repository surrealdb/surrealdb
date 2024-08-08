use crate::err::Error;
use crate::sql::statements::DefineFunctionStatement;
use crate::sql::value::serde::ser;
use crate::sql::Block;
use crate::sql::Ident;
use crate::sql::Kind;
use crate::sql::Permission;
use crate::sql::Strand;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineFunctionStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineFunctionStatement, Error>;
	type SerializeTuple = Impossible<DefineFunctionStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineFunctionStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineFunctionStatement, Error>;
	type SerializeMap = Impossible<DefineFunctionStatement, Error>;
	type SerializeStruct = SerializeDefineFunctionStatement;
	type SerializeStructVariant = Impossible<DefineFunctionStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineFunctionStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineFunctionStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineFunctionStatement {
	name: Ident,
	args: Vec<(Ident, Kind)>,
	block: Block,
	comment: Option<Strand>,
	permissions: Permission,
	if_not_exists: bool,
	overwrite: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineFunctionStatement {
	type Ok = DefineFunctionStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"args" => {
				self.args = value.serialize(IdentKindVecSerializer.wrap())?;
			}
			"block" => {
				self.block = Block(value.serialize(ser::block::entry::vec::Serializer.wrap())?);
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"permissions" => {
				self.permissions = value.serialize(ser::permission::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			"overwrite" => {
				self.overwrite = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineFunctionStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineFunctionStatement {
			name: self.name,
			args: self.args,
			block: self.block,
			comment: self.comment,
			permissions: self.permissions,
			if_not_exists: self.if_not_exists,
			overwrite: self.overwrite,
		})
	}
}

type IdentKindTuple = (Ident, Kind);

struct IdentKindVecSerializer;

impl ser::Serializer for IdentKindVecSerializer {
	type Ok = Vec<IdentKindTuple>;
	type Error = Error;

	type SerializeSeq = SerializeIdentKindVec;
	type SerializeTuple = Impossible<Vec<IdentKindTuple>, Error>;
	type SerializeTupleStruct = Impossible<Vec<IdentKindTuple>, Error>;
	type SerializeTupleVariant = Impossible<Vec<IdentKindTuple>, Error>;
	type SerializeMap = Impossible<Vec<IdentKindTuple>, Error>;
	type SerializeStruct = Impossible<Vec<IdentKindTuple>, Error>;
	type SerializeStructVariant = Impossible<Vec<IdentKindTuple>, Error>;

	const EXPECTED: &'static str = "a `Vec<(Ident, Kind)>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeIdentKindVec(Vec::with_capacity(len.unwrap_or_default())))
	}
}

struct SerializeIdentKindVec(Vec<IdentKindTuple>);

impl serde::ser::SerializeSeq for SerializeIdentKindVec {
	type Ok = Vec<IdentKindTuple>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(IdentKindTupleSerializer.wrap())?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

struct IdentKindTupleSerializer;

impl ser::Serializer for IdentKindTupleSerializer {
	type Ok = IdentKindTuple;
	type Error = Error;

	type SerializeSeq = Impossible<IdentKindTuple, Error>;
	type SerializeTuple = SerializeIdentKindTuple;
	type SerializeTupleStruct = Impossible<IdentKindTuple, Error>;
	type SerializeTupleVariant = Impossible<IdentKindTuple, Error>;
	type SerializeMap = Impossible<IdentKindTuple, Error>;
	type SerializeStruct = Impossible<IdentKindTuple, Error>;
	type SerializeStructVariant = Impossible<IdentKindTuple, Error>;

	const EXPECTED: &'static str = "an `(Ident, Kind)`";

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeIdentKindTuple::default())
	}
}

#[derive(Default)]
struct SerializeIdentKindTuple {
	index: usize,
	tuple: IdentKindTuple,
}

impl serde::ser::SerializeTuple for SerializeIdentKindTuple {
	type Ok = IdentKindTuple;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.tuple.0 = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			1 => {
				self.tuple.1 = value.serialize(ser::kind::Serializer.wrap())?;
			}
			index => {
				return Err(Error::custom(format!(
					"unexpected tuple index `{index}` for `(Ident, Kind)`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.tuple)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineFunctionStatement::default();
		let value: DefineFunctionStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
