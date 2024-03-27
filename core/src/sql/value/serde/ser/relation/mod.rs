use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Kind;
use crate::sql::Relation;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Relation;
	type Error = Error;

	type SerializeSeq = Impossible<Relation, Error>;
	type SerializeTuple = Impossible<Relation, Error>;
	type SerializeTupleStruct = Impossible<Relation, Error>;
	type SerializeTupleVariant = Impossible<Relation, Error>;
	type SerializeMap = Impossible<Relation, Error>;
	type SerializeStruct = SerializeRelation;
	type SerializeStructVariant = Impossible<Relation, Error>;

	const EXPECTED: &'static str = "a struct `Relation`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRelation::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRelation {
	from: Option<Kind>,
	to: Option<Kind>,
}

impl serde::ser::SerializeStruct for SerializeRelation {
	type Ok = Relation;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"from" => {
				self.from = value.serialize(ser::kind::opt::Serializer.wrap())?;
			}
			"to" => {
				self.to = value.serialize(ser::kind::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Relation::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(Relation {
			from: self.from,
			to: self.to,
		})
	}
}
