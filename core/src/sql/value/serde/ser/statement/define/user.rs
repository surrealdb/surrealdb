use crate::err::Error;
use crate::sql::statements::DefineUserStatement;
use crate::sql::value::serde::ser;
use crate::sql::Base;
use crate::sql::Duration;
use crate::sql::Ident;
use crate::sql::Strand;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineUserStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineUserStatement, Error>;
	type SerializeTuple = Impossible<DefineUserStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineUserStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineUserStatement, Error>;
	type SerializeMap = Impossible<DefineUserStatement, Error>;
	type SerializeStruct = SerializeDefineUserStatement;
	type SerializeStructVariant = Impossible<DefineUserStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineUserStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineUserStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineUserStatement {
	name: Ident,
	base: Base,
	hash: String,
	code: String,
	roles: Vec<Ident>,
	session: Option<Duration>,
	comment: Option<Strand>,
	if_not_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineUserStatement {
	type Ok = DefineUserStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"base" => {
				self.base = value.serialize(ser::base::Serializer.wrap())?;
			}
			"hash" => {
				self.hash = value.serialize(ser::string::Serializer.wrap())?;
			}
			"code" => {
				self.code = value.serialize(ser::string::Serializer.wrap())?;
			}
			"roles" => {
				self.roles = value.serialize(ser::ident::vec::Serializer.wrap())?;
			}
			"session" => {
				self.session =
					value.serialize(ser::duration::opt::Serializer.wrap())?.map(Into::into);
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineUserStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineUserStatement {
			name: self.name,
			base: self.base,
			hash: self.hash,
			code: self.code,
			roles: self.roles,
			session: self.session,
			comment: self.comment,
			if_not_exists: self.if_not_exists,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineUserStatement::default();
		let value: DefineUserStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
