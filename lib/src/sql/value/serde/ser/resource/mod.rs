use crate::err::Error;
use crate::iam::{Actor, Level, Resource, ResourceKind, Role};
use crate::sql::value::serde::ser;
use crate::sql::Model;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Resource;
	type Error = Error;

	type SerializeSeq = Impossible<Resource, Error>;
	type SerializeTuple = Impossible<Resource, Error>;
	type SerializeTupleStruct = Impossible<Resource, Error>;
	type SerializeTupleVariant = Impossible<Resource, Error>;
	type SerializeMap = Impossible<Resource, Error>;
	type SerializeStruct = SerializeResourceModel;
	type SerializeStructVariant = Impossible<Resource, Error>;

	const EXPECTED: &'static str = "a 'Resource' struct";
}

pub(super) struct SerializeResourceModel {
	id: String,
	kind: ResourceKind,
	level: Level,
}

impl serde::ser::SerializeStruct for SerializeResourceModel {
	type Ok = Resource;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"id" => {
				self.id = value.serialize(ser::string::Serializer.wrap())?;
			}
			"kind" => {
				self.kind = value.serialize(ser::resource_kind::Serializer.wrap())?;
			}
			"level" => {
				self.level = value.serialize(ser::level::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Resource::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Resource {
			id: self.id,
			kind: self.kind,
			level: self.level,
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
