use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Permission;
use crate::sql::Permissions;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub mod opt;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Permissions;
	type Error = Error;

	type SerializeSeq = Impossible<Permissions, Error>;
	type SerializeTuple = Impossible<Permissions, Error>;
	type SerializeTupleStruct = Impossible<Permissions, Error>;
	type SerializeTupleVariant = Impossible<Permissions, Error>;
	type SerializeMap = Impossible<Permissions, Error>;
	type SerializeStruct = SerializePermissions;
	type SerializeStructVariant = Impossible<Permissions, Error>;

	const EXPECTED: &'static str = "a struct `Permissions`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializePermissions::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializePermissions {
	select: Permission,
	create: Permission,
	update: Permission,
	delete: Permission,
}

impl serde::ser::SerializeStruct for SerializePermissions {
	type Ok = Permissions;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"select" => {
				self.select = value.serialize(ser::permission::Serializer.wrap())?;
			}
			"create" => {
				self.create = value.serialize(ser::permission::Serializer.wrap())?;
			}
			"update" => {
				self.update = value.serialize(ser::permission::Serializer.wrap())?;
			}
			"delete" => {
				self.delete = value.serialize(ser::permission::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Permissions::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(Permissions {
			select: self.select,
			create: self.create,
			update: self.update,
			delete: self.delete,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = Permissions::default();
		let value: Permissions = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
