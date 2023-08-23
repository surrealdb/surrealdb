mod opt;

use crate::err::Error;
use crate::iam::{Actor, Auth};
use crate::sql::value::serde::ser;
use crate::sql::Model;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Auth;
	type Error = Error;

	type SerializeSeq = Impossible<Auth, Error>;
	type SerializeTuple = Impossible<Auth, Error>;
	type SerializeTupleStruct = Impossible<Auth, Error>;
	type SerializeTupleVariant = Impossible<Auth, Error>;
	type SerializeMap = Impossible<Auth, Error>;
	type SerializeStruct = SerializeAuthModel;
	type SerializeStructVariant = Impossible<Auth, Error>;

	const EXPECTED: &'static str = "an 'Auth' struct";
}

pub(super) struct SerializeAuthModel {
	actor: Actor,
}

enum Inner {
	Count(Option<String>, Option<u64>),
	Range(Option<String>, Option<u64>, Option<u64>),
}

impl serde::ser::SerializeStruct for SerializeAuthModel {
	type Ok = Auth;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"actor" => {
				self.actor = value.serialize(ser::actor::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Auth::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Auth {
			actor: self.actor,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::Serialize;

	#[test]
	fn count() {
		let model = Model::Count(Default::default(), Default::default());
		let serialized = model.serialize(Serializer.wrap()).unwrap();
		assert_eq!(model, serialized);
	}

	#[test]
	fn range() {
		let model = Model::Range(Default::default(), 1, 2);
		let serialized = model.serialize(Serializer.wrap()).unwrap();
		assert_eq!(model, serialized);
	}
}
