use crate::err::Error;
use crate::sql::index::{Distance, HnswParams, VectorType};
use crate::sql::value::serde::ser;
use crate::sql::Number;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = HnswParams;
	type Error = Error;

	type SerializeSeq = Impossible<HnswParams, Error>;
	type SerializeTuple = Impossible<HnswParams, Error>;
	type SerializeTupleStruct = Impossible<HnswParams, Error>;
	type SerializeTupleVariant = Impossible<HnswParams, Error>;
	type SerializeMap = Impossible<HnswParams, Error>;
	type SerializeStruct = SerializeMTree;
	type SerializeStructVariant = Impossible<HnswParams, Error>;

	const EXPECTED: &'static str = "a struct `HnswParams`";

	#[inline]
	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self.wrap())
	}

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeMTree::default())
	}
}

#[derive(Default)]
pub(super) struct SerializeMTree {
	dimension: u16,
	distance: Distance,
	vector_type: VectorType,
	m: u8,
	m0: u8,
	ef_construction: u16,
	ml: Number,
	extend_candidates: bool,
	keep_pruned_connections: bool,
}
impl serde::ser::SerializeStruct for SerializeMTree {
	type Ok = HnswParams;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"dimension" => {
				self.dimension = value.serialize(ser::primitive::u16::Serializer.wrap())?;
			}
			"distance" => {
				self.distance = value.serialize(ser::distance::Serializer.wrap())?;
			}
			"vector_type" => {
				self.vector_type = value.serialize(ser::vectortype::Serializer.wrap())?;
			}
			"m" => {
				self.m = value.serialize(ser::primitive::u8::Serializer.wrap())?;
			}
			"m0" => {
				self.m0 = value.serialize(ser::primitive::u8::Serializer.wrap())?;
			}
			"ef_construction" => {
				self.ef_construction = value.serialize(ser::primitive::u16::Serializer.wrap())?;
			}
			"extend_candidates" => {
				self.extend_candidates =
					value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			"keep_pruned_connections" => {
				self.keep_pruned_connections =
					value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			"ml" => {
				self.ml = value.serialize(ser::number::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `HnswParams {{ {key} }}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(HnswParams {
			dimension: self.dimension,
			distance: self.distance,
			vector_type: self.vector_type,
			m: self.m,
			m0: self.m0,
			ef_construction: self.ef_construction,
			extend_candidates: self.extend_candidates,
			keep_pruned_connections: self.keep_pruned_connections,
			ml: self.ml,
		})
	}
}

#[test]
fn hnsw_params() {
	let params = HnswParams {
		dimension: 1,
		distance: Default::default(),
		vector_type: Default::default(),
		m: 2,
		m0: 3,
		ef_construction: 4,
		extend_candidates: true,
		keep_pruned_connections: true,
		ml: 5.0.into(),
	};
	let serialized = params.serialize(Serializer.wrap()).unwrap();
	assert_eq!(params, serialized);
}
