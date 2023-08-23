use crate::err::Error;
use crate::iam::{Actor, Resource, Role};
use crate::sql::value::serde::ser;
use crate::sql::Model;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Actor;
	type Error = Error;

	type SerializeSeq = Impossible<Actor, Error>;
	type SerializeTuple = Impossible<Actor, Error>;
	type SerializeTupleStruct = Impossible<Actor, Error>;
	type SerializeTupleVariant = Impossible<Actor, Error>;
	type SerializeMap = Impossible<Actor, Error>;
	type SerializeStruct = SerializeActorModel;
	type SerializeStructVariant = Impossible<Actor, Error>;

	const EXPECTED: &'static str = "an 'Actor' struct";
}

pub(super) struct SerializeActorModel {
	res: Resource,
	roles: Vec<Role>,
}

impl serde::ser::SerializeStruct for SerializeActorModel {
	type Ok = Actor;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"res" => {
				self.actor = value.serialize(ser::resource::Serializer.wrap())?;
			}
			"roles" => {
				self.roles = value.serialize(ser::role::vec::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Actor::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Actor {
			res: self.res,
			roles: self.roles,
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
