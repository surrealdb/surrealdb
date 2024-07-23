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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::thing;
	use serde::ser::Impossible;
	use serde::Serialize;

	pub(super) struct Serializer;

	impl ser::Serializer for Serializer {
		type Ok = Edges;
		type Error = Error;

		type SerializeSeq = Impossible<Edges, Error>;
		type SerializeTuple = Impossible<Edges, Error>;
		type SerializeTupleStruct = Impossible<Edges, Error>;
		type SerializeTupleVariant = Impossible<Edges, Error>;
		type SerializeMap = Impossible<Edges, Error>;
		type SerializeStruct = SerializeEdges;
		type SerializeStructVariant = Impossible<Edges, Error>;

		const EXPECTED: &'static str = "a struct `Edges`";

		#[inline]
		fn serialize_struct(
			self,
			_name: &'static str,
			_len: usize,
		) -> Result<Self::SerializeStruct, Error> {
			Ok(SerializeEdges::default())
		}
	}

	#[test]
	fn edges() {
		let edges = Edges {
			dir: Dir::Both,
			from: thing("foo:bar").unwrap(),
			what: Tables(Vec::new()),
		};
		let serialized = edges.serialize(Serializer.wrap()).unwrap();
		assert_eq!(edges, serialized);
	}
}
