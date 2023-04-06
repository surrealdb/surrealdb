use crate::err::Error;
use crate::sql::field::Fields;
use crate::sql::group::Groups;
use crate::sql::limit::Limit;
use crate::sql::order::Orders;
use crate::sql::split::Splits;
use crate::sql::start::Start;
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
	expr: Option<Fields>,
	what: Option<Tables>,
	cond: Option<Cond>,
	split: Option<Splits>,
	group: Option<Groups>,
	order: Option<Orders>,
	limit: Option<Limit>,
	start: Option<Start>,
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
			"expr" => {
				self.expr = Some(value.serialize(ser::fields::Serializer.wrap())?);
			}
			"what" => {
				self.what = Some(Tables(value.serialize(ser::table::vec::Serializer.wrap())?));
			}
			"cond" => {
				self.cond = value.serialize(ser::cond::opt::Serializer.wrap())?;
			}
			"split" => {
				self.split = value.serialize(ser::split::vec::opt::Serializer.wrap())?.map(Splits);
			}
			"group" => {
				self.group = value.serialize(ser::group::vec::opt::Serializer.wrap())?.map(Groups);
			}
			"order" => {
				self.order = value.serialize(ser::order::vec::opt::Serializer.wrap())?.map(Orders);
			}
			"limit" => {
				self.limit = value.serialize(ser::limit::opt::Serializer.wrap())?;
			}
			"start" => {
				self.start = value.serialize(ser::start::opt::Serializer.wrap())?;
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
		match (self.dir, self.expr, self.what) {
			(Some(dir), Some(expr), Some(what)) => Ok(Graph {
				dir,
				expr,
				what,
				cond: self.cond,
				split: self.split,
				group: self.group,
				order: self.order,
				limit: self.limit,
				start: self.start,
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
