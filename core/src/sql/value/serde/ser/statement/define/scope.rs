use crate::err::Error;
use crate::sql::statements::DefineScopeStatement;
use crate::sql::value::serde::ser;
use crate::sql::Duration;
use crate::sql::Ident;
use crate::sql::Strand;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineScopeStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineScopeStatement, Error>;
	type SerializeTuple = Impossible<DefineScopeStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineScopeStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineScopeStatement, Error>;
	type SerializeMap = Impossible<DefineScopeStatement, Error>;
	type SerializeStruct = SerializeDefineScopeStatement;
	type SerializeStructVariant = Impossible<DefineScopeStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineScopeStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineScopeStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineScopeStatement {
	name: Ident,
	code: String,
	session: Option<Duration>,
	signup: Option<Value>,
	signin: Option<Value>,
	comment: Option<Strand>,
	if_not_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineScopeStatement {
	type Ok = DefineScopeStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"code" => {
				self.code = value.serialize(ser::string::Serializer.wrap())?;
			}
			"session" => {
				self.session =
					value.serialize(ser::duration::opt::Serializer.wrap())?.map(Into::into);
			}
			"signup" => {
				self.signup = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"signin" => {
				self.signin = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineScopeStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineScopeStatement {
			name: self.name,
			code: self.code,
			session: self.session,
			signup: self.signup,
			signin: self.signin,
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
		let stmt = DefineScopeStatement::default();
		let value: DefineScopeStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
