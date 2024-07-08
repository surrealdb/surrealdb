use crate::err::Error;
use crate::sql::index::{Distance, Distance1, MTreeParams, VectorType};
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = MTreeParams;
	type Error = Error;

	type SerializeSeq = Impossible<MTreeParams, Error>;
	type SerializeTuple = Impossible<MTreeParams, Error>;
	type SerializeTupleStruct = Impossible<MTreeParams, Error>;
	type SerializeTupleVariant = Impossible<MTreeParams, Error>;
	type SerializeMap = Impossible<MTreeParams, Error>;
	type SerializeStruct = SerializeMTree;
	type SerializeStructVariant = Impossible<MTreeParams, Error>;

	const EXPECTED: &'static str = "a struct `MTreeParams`";

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
	capacity: u16,
	doc_ids_order: u32,
	doc_ids_cache: u32,
	mtree_cache: u32,
}
impl serde::ser::SerializeStruct for SerializeMTree {
	type Ok = MTreeParams;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"dimension" => {
				self.dimension = value.serialize(ser::primitive::u16::Serializer.wrap())?;
			}
			"_distance" => {
				self.distance = value.serialize(ser::distance::Serializer.wrap())?;
			}
			"distance" => {
				self.distance = value.serialize(ser::distance::Serializer.wrap())?;
			}
			"vector_type" => {
				self.vector_type = value.serialize(ser::vectortype::Serializer.wrap())?;
			}
			"capacity" => {
				self.capacity = value.serialize(ser::primitive::u16::Serializer.wrap())?;
			}
			"doc_ids_order" => {
				self.doc_ids_order = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			"doc_ids_cache" => {
				self.doc_ids_cache = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			"mtree_cache" => {
				self.mtree_cache = value.serialize(ser::primitive::u32::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `MTreeParams {{ {key} }}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(MTreeParams {
			dimension: self.dimension,
			_distance: Distance1::Euclidean,
			distance: self.distance,
			vector_type: self.vector_type,
			capacity: self.capacity,
			doc_ids_order: self.doc_ids_order,
			doc_ids_cache: self.doc_ids_cache,
			mtree_cache: self.mtree_cache,
		})
	}
}

#[test]
fn mtree_params() {
	let params = MTreeParams::new(1, Default::default(), Default::default(), 2, 3, 4, 5);
	let serialized = params.serialize(Serializer.wrap()).unwrap();
	assert_eq!(params, serialized);
}
