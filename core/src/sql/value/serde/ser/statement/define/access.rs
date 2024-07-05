use crate::err::Error;
use crate::sql::access::AccessDuration;
use crate::sql::access_type::AccessType;
use crate::sql::statements::DefineAccessStatement;
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
	type Ok = DefineAccessStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineAccessStatement, Error>;
	type SerializeTuple = Impossible<DefineAccessStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineAccessStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineAccessStatement, Error>;
	type SerializeMap = Impossible<DefineAccessStatement, Error>;
	type SerializeStruct = SerializeDefineAccessStatement;
	type SerializeStructVariant = Impossible<DefineAccessStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineAccessStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineAccessStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineAccessStatement {
	name: Ident,
	base: Base,
	kind: AccessType,
	duration: AccessDuration,
	comment: Option<Strand>,
	if_not_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineAccessStatement {
	type Ok = DefineAccessStatement;
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
			"kind" => {
				self.kind = value.serialize(ser::access_type::Serializer.wrap())?;
			}
			"duration" => {
				self.duration = value.serialize(SerializerDuration.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineAccessStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineAccessStatement {
			name: self.name,
			base: self.base,
			kind: self.kind,
			duration: self.duration,
			comment: self.comment,
			if_not_exists: self.if_not_exists,
		})
	}
}

pub struct SerializerDuration;

impl ser::Serializer for SerializerDuration {
	type Ok = AccessDuration;
	type Error = Error;

	type SerializeSeq = Impossible<AccessDuration, Error>;
	type SerializeTuple = Impossible<AccessDuration, Error>;
	type SerializeTupleStruct = Impossible<AccessDuration, Error>;
	type SerializeTupleVariant = Impossible<AccessDuration, Error>;
	type SerializeMap = Impossible<AccessDuration, Error>;
	type SerializeStruct = SerializeDuration;
	type SerializeStructVariant = Impossible<AccessDuration, Error>;

	const EXPECTED: &'static str = "a struct `AccessDuration`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDuration::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDuration {
	pub grant: Option<Duration>,
	pub token: Option<Duration>,
	pub session: Option<Duration>,
}

impl serde::ser::SerializeStruct for SerializeDuration {
	type Ok = AccessDuration;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"grant" => {
				self.grant =
					value.serialize(ser::duration::opt::Serializer.wrap())?.map(Into::into);
			}
			"token" => {
				self.token =
					value.serialize(ser::duration::opt::Serializer.wrap())?.map(Into::into);
			}
			"session" => {
				self.session =
					value.serialize(ser::duration::opt::Serializer.wrap())?.map(Into::into);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `AccessDuration::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(AccessDuration {
			grant: self.grant,
			token: self.token,
			session: self.session,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineAccessStatement::default();
		let value: DefineAccessStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
