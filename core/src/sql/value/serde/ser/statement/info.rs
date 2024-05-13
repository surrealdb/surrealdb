use crate::err::Error;
use crate::sql::statements::InfoStatement;
use crate::sql::value::serde::ser;
use crate::sql::Base;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;
use std::fmt::{Display, Formatter};

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = InfoStatement;
	type Error = Error;

	type SerializeSeq = Impossible<InfoStatement, Error>;
	type SerializeTuple = Impossible<InfoStatement, Error>;
	type SerializeTupleStruct = Impossible<InfoStatement, Error>;
	type SerializeTupleVariant = SerializeInfoStatement;
	type SerializeMap = Impossible<InfoStatement, Error>;
	type SerializeStruct = Impossible<InfoStatement, Error>;
	type SerializeStructVariant = Impossible<InfoStatement, Error>;

	const EXPECTED: &'static str = "an enum `InfoStatement`";

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Root" => {
				Ok(InfoStatement::Root(value.serialize(ser::primitive::bool::Serializer.wrap())?))
			}
			"Ns" => {
				Ok(InfoStatement::Ns(value.serialize(ser::primitive::bool::Serializer.wrap())?))
			}
			"Db" => {
				Ok(InfoStatement::Db(value.serialize(ser::primitive::bool::Serializer.wrap())?))
			}
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}

	#[inline]
	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		match variant {
			"Tb" => Ok(SerializeInfoStatement::with(Which::Tb)),
			"User" => Ok(SerializeInfoStatement::with(Which::User)),
			variant => Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`"))),
		}
	}
}

#[derive(Clone, Copy)]
enum Which {
	Tb,
	User,
}

impl Display for Which {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Which::Tb => {
				write!(f, "Tb")
			}
			Which::User => {
				write!(f, "User")
			}
		}
	}
}

pub(super) struct SerializeInfoStatement {
	index: usize,
	which: Which,
	tuple: (Option<Ident>, Option<Base>, bool),
}

impl SerializeInfoStatement {
	fn with(which: Which) -> Self {
		Self {
			index: 0,
			which,
			tuple: (None, None, false),
		}
	}
}

impl serde::ser::SerializeTupleVariant for SerializeInfoStatement {
	type Ok = InfoStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		use Which::*;
		match (self.which, self.index) {
			(_, 0) => {
				self.tuple.0 = Some(Ident(value.serialize(ser::string::Serializer.wrap())?));
			}
			(Tb, 1) => {
				self.tuple.2 = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			(User, 1) => {
				self.tuple.1 = value.serialize(ser::base::opt::Serializer.wrap())?;
			}
			(User, 2) => {
				self.tuple.2 = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			(_, index) => {
				return Err(Error::custom(format!(
					"unexpected `InfoStatement::{}` index `{index}`",
					self.which
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		use Which::*;
		match (self.which, self.tuple.0) {
			(Tb, Some(ident)) => Ok(InfoStatement::Tb(ident, self.tuple.2)),
			(Tb, None) => Err(Error::custom("`InfoStatement::Tb` missing required value(s)")),
			(User, Some(ident)) => Ok(InfoStatement::User(ident, self.tuple.1, self.tuple.2)),
			(User, None) => Err(Error::custom("`InfoStatement::User` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn root() {
		let stmt = InfoStatement::Root(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn ns() {
		let stmt = InfoStatement::Ns(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn db() {
		let stmt = InfoStatement::Db(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn tb() {
		let stmt = InfoStatement::Tb(Default::default(), Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn user() {
		let stmt = InfoStatement::User(Default::default(), Default::default(), Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}
}
