mod hnswparams;
mod mtreeparams;
mod searchparams;

use crate::err::Error;
use crate::sql::index::Index;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Index;
	type Error = Error;

	type SerializeSeq = Impossible<Index, Error>;
	type SerializeTuple = Impossible<Index, Error>;
	type SerializeTupleStruct = Impossible<Index, Error>;
	type SerializeTupleVariant = Impossible<Index, Error>;
	type SerializeMap = Impossible<Index, Error>;
	type SerializeStruct = Impossible<Index, Error>;
	type SerializeStructVariant = Impossible<Index, Error>;

	const EXPECTED: &'static str = "an enum `Index`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Idx" => Ok(Index::Idx),
			"Uniq" => Ok(Index::Uniq),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Search" => Ok(Index::Search(value.serialize(searchparams::Serializer.wrap())?)),
			"MTree" => Ok(Index::MTree(value.serialize(mtreeparams::Serializer.wrap())?)),
			"Hnsw" => Ok(Index::Hnsw(value.serialize(hnswparams::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::index::{Distance, HnswParams, MTreeParams, SearchParams, VectorType};
	use crate::sql::value::serde::ser::Serializer;
	use crate::sql::Scoring;

	#[test]
	fn idx() {
		let idx = Index::Idx;
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}

	#[test]
	fn uniq() {
		let idx = Index::Uniq;
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}

	#[test]
	fn search() {
		let idx = Index::Search(SearchParams {
			az: Default::default(),
			hl: Default::default(),
			sc: Scoring::Bm {
				k1: Default::default(),
				b: Default::default(),
			},
			doc_ids_order: 1,
			doc_lengths_order: 2,
			postings_order: 3,
			terms_order: 4,
			doc_ids_cache: 5,
			doc_lengths_cache: 6,
			postings_cache: 7,
			terms_cache: 8,
		});
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}

	#[test]
	fn mtree() {
		let idx = Index::MTree(MTreeParams {
			dimension: 1,
			_distance: Default::default(),
			distance: Distance::Manhattan,
			vector_type: VectorType::I16,
			capacity: 2,
			doc_ids_order: 3,
			doc_ids_cache: 4,
			mtree_cache: 5,
		});
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}

	#[test]
	fn hnsw() {
		let idx = Index::Hnsw(HnswParams {
			dimension: 1,
			distance: Distance::Manhattan,
			vector_type: VectorType::I16,
			m: 2,
			m0: 3,
			ef_construction: 4,
			extend_candidates: true,
			keep_pruned_connections: true,
			ml: 5.into(),
		});
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}
}
