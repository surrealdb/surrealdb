use crate::err::Error;
use crate::sql::statements::InfoStatement;
use crate::sql::value::serde::ser;
use crate::sql::Base;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

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
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Root" => Ok(InfoStatement::Root),
			"Ns" => Ok(InfoStatement::Ns),
			"Db" => Ok(InfoStatement::Db),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}

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
			"Sc" => Ok(InfoStatement::Sc(Ident(value.serialize(ser::string::Serializer.wrap())?))),
			"Tb" => Ok(InfoStatement::Tb(Ident(value.serialize(ser::string::Serializer.wrap())?))),
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
			"User" => Ok(SerializeInfoStatement::default()),
			variant => Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`"))),
		}
	}
}

#[derive(Default)]
pub(super) struct SerializeInfoStatement {
	index: usize,
	tuple: (Option<Ident>, Option<Base>),
}

impl serde::ser::SerializeTupleVariant for SerializeInfoStatement {
	type Ok = InfoStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.tuple.0 = Some(Ident(value.serialize(ser::string::Serializer.wrap())?));
			}
			1 => {
				self.tuple.1 = value.serialize(ser::base::opt::Serializer.wrap())?;
			}
			index => {
				return Err(Error::custom(format!(
					"unexpected `InfoStatement::User` index `{index}`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match self.tuple.0 {
			Some(ident) => Ok(InfoStatement::User(ident, self.tuple.1)),
			None => Err(Error::custom("`InfoStatement::User` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn root() {
		let stmt = InfoStatement::Root;
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn ns() {
		let stmt = InfoStatement::Ns;
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn db() {
		let stmt = InfoStatement::Db;
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn sc() {
		let stmt = InfoStatement::Sc(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn tb() {
		let stmt = InfoStatement::Tb(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn user() {
		let stmt = InfoStatement::User(Default::default(), Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}
}
