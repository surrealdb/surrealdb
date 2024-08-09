use crate::err::Error;
use crate::sql::statements::DefineUserStatement;
use crate::sql::user::UserDuration;
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
	duration: UserDuration,
	comment: Option<Strand>,
	if_not_exists: bool,
	overwrite: bool,
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
			"duration" => {
				self.duration = value.serialize(SerializerDuration.wrap())?;
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
			duration: self.duration,
			comment: self.comment,
			if_not_exists: self.if_not_exists,
			overwrite: self.overwrite,
		})
	}
}
pub struct SerializerDuration;

impl ser::Serializer for SerializerDuration {
	type Ok = UserDuration;
	type Error = Error;

	type SerializeSeq = Impossible<UserDuration, Error>;
	type SerializeTuple = Impossible<UserDuration, Error>;
	type SerializeTupleStruct = Impossible<UserDuration, Error>;
	type SerializeTupleVariant = Impossible<UserDuration, Error>;
	type SerializeMap = Impossible<UserDuration, Error>;
	type SerializeStruct = SerializeDuration;
	type SerializeStructVariant = Impossible<UserDuration, Error>;

	const EXPECTED: &'static str = "a struct `UserDuration`";

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
	pub token: Option<Duration>,
	pub session: Option<Duration>,
}

impl serde::ser::SerializeStruct for SerializeDuration {
	type Ok = UserDuration;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"token" => {
				self.token =
					value.serialize(ser::duration::opt::Serializer.wrap())?.map(Into::into);
			}
			"session" => {
				self.session =
					value.serialize(ser::duration::opt::Serializer.wrap())?.map(Into::into);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `UserDuration::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(UserDuration {
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
		let stmt = DefineUserStatement::default();
		let value: DefineUserStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_durations() {
		let stmt = DefineUserStatement {
			duration: UserDuration {
				token: Some(Duration::from_mins(15)),
				session: Some(Duration::from_mins(90)),
			},
			..Default::default()
		};
		let value: DefineUserStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
