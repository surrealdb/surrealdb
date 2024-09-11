use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Dir;
use crate::sql::Edges;
use crate::sql::Tables;
use crate::sql::Thing;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Serialize;

#[derive(Default)]
pub(super) struct SerializeEdges {
	dir: Option<Dir>,
	from: Option<Thing>,
	what: Option<Tables>,
}

impl serde::ser::SerializeStruct for SerializeEdges {
	type Ok = Edges;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"dir" => {
				self.dir = Some(value.serialize(ser::dir::Serializer.wrap())?);
			}
			"from" => {
				self.from = Some(value.serialize(ser::thing::Serializer.wrap())?);
			}
			"what" => {
				self.what = Some(Tables(value.serialize(ser::table::vec::Serializer.wrap())?));
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Edges::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.dir, self.from, self.what) {
			(Some(dir), Some(from), Some(what)) => Ok(Edges {
				dir,
				from,
				what,
			}),
			_ => Err(Error::custom("`Edges` missing required field(s)")),
		}
	}
}
