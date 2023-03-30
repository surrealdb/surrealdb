use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Cond;
use crate::sql::Dir;
use crate::sql::Graph;
use crate::sql::Idiom;
use crate::sql::Tables;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Graph;
	type Error = Error;

	type SerializeSeq = Impossible<Graph, Error>;
	type SerializeTuple = Impossible<Graph, Error>;
	type SerializeTupleStruct = Impossible<Graph, Error>;
	type SerializeTupleVariant = Impossible<Graph, Error>;
	type SerializeMap = Impossible<Graph, Error>;
	type SerializeStruct = SerializeGraph;
	type SerializeStructVariant = Impossible<Graph, Error>;

	const EXPECTED: &'static str = "a struct `Graph`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeGraph::default())
	}
}

#[derive(Default)]
pub(super) struct SerializeGraph {
	dir: Option<Dir>,
	what: Option<Tables>,
	cond: Option<Cond>,
	alias: Option<Idiom>,
}

impl serde::ser::SerializeStruct for SerializeGraph {
	type Ok = Graph;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"dir" => {
				self.dir = Some(value.serialize(ser::dir::Serializer.wrap())?);
			}
			"what" => {
				self.what = Some(Tables(value.serialize(ser::table::vec::Serializer.wrap())?));
			}
			"cond" => {
				self.cond = value.serialize(ser::cond::opt::Serializer.wrap())?;
			}
			"alias" => {
				self.alias = value.serialize(ser::part::vec::opt::Serializer.wrap())?.map(Idiom);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Graph::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.dir, self.what) {
			(Some(dir), Some(what)) => Ok(Graph {
				dir,
				what,
				cond: self.cond,
				alias: self.alias,
			}),
			_ => Err(Error::custom("`Graph` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use serde::Serialize;

	#[test]
	fn default() {
		let graph = Graph::default();
		let serialized = serialize_internal(|| graph.serialize(Serializer.wrap())).unwrap();
		assert_eq!(graph, serialized);
	}

	#[test]
	fn with_cond() {
		let graph = Graph {
			cond: Some(Default::default()),
			..Default::default()
		};
		let serialized = serialize_internal(|| graph.serialize(Serializer.wrap())).unwrap();
		assert_eq!(graph, serialized);
	}

	#[test]
	fn with_alias() {
		let graph = Graph {
			alias: Some(Default::default()),
			..Default::default()
		};
		let serialized = serialize_internal(|| graph.serialize(Serializer.wrap())).unwrap();
		assert_eq!(graph, serialized);
	}
}
