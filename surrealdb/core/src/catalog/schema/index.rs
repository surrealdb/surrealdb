use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};

use anyhow::Result;
use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned, revisioned,
};
use storekey::{BorrowDecode, Encode};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Idiom};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql;
use crate::sql::statements::define::DefineKind;
use crate::val::{Array, Number, TableName, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, BorrowDecode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[repr(transparent)]
pub struct IndexId(pub u32);

impl_kv_value_revisioned!(IndexId);

impl Revisioned for IndexId {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for IndexId {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.0, writer)
	}
}

impl DeserializeRevisioned for IndexId {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		DeserializeRevisioned::deserialize_revisioned(reader).map(IndexId)
	}
}

impl SkipRevisioned for IndexId {
	#[inline]
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		<u32 as SkipRevisioned>::skip_revisioned(reader)
	}
}

impl revision::WalkRevisioned for IndexId {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, IndexId, R>;

	#[inline]
	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl From<u32> for IndexId {
	fn from(value: u32) -> Self {
		IndexId(value)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct IndexDefinition {
	pub(crate) index_id: IndexId,
	pub(crate) name: Strand,
	pub(crate) table_name: TableName,
	pub(crate) cols: Vec<Idiom>,
	pub(crate) index: Index,
	pub(crate) comment: Option<String>,
	/// Whether this index has been marked for removal via `REMOVE INDEX`.
	/// Indexes marked for removal are excluded from query planning and document
	/// indexing, and any in-progress index builds are cancelled.
	pub(crate) prepare_remove: bool,
}

impl_kv_value_revisioned!(IndexDefinition);

impl IndexDefinition {
	pub(crate) fn to_sql_definition(&self) -> sql::DefineIndexStatement {
		sql::DefineIndexStatement {
			kind: DefineKind::Default,
			name: sql::Expr::Idiom(sql::Idiom::field(self.name.clone())),
			what: sql::Expr::Table(self.table_name.clone()),
			cols: self.cols.iter().cloned().map(|x| sql::Expr::Idiom(x.into())).collect(),
			index: self.index.to_sql_definition(),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
			concurrently: false,
		}
	}

	/// Checks if this index has been marked for removal and returns an error if so.
	///
	/// This method is used during index building to detect when an index has been
	/// marked for removal via `REMOVE INDEX`, allowing the build process to be
	/// cancelled gracefully.
	///
	/// # Errors
	///
	/// Returns `Error::IndexingBuildingCancelled` if `prepare_remove` is `true`.
	pub(crate) fn expect_not_prepare_remove(&self) -> Result<()> {
		if self.prepare_remove {
			Err(anyhow::Error::new(Error::IndexingBuildingCancelled {
				reason: "Prepare remove.".to_string(),
			}))
		} else {
			Ok(())
		}
	}
}

impl InfoStructure for IndexDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.into(),
			"table" => Value::String(self.table_name.into()),
			"cols" => Value::Array(Array(self.cols.into_iter().map(|x| x.structure()).collect())),
			"index" => self.index.structure(),
			"comment", if let Some(v) = self.comment => v.into(),
			"prepare_remove", if self.prepare_remove => self.prepare_remove.into()
		})
	}
}

impl ToSql for IndexDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum Index {
	/// (Basic) non unique
	#[default]
	Idx,
	/// Unique index
	Uniq,
	/// HNSW index for distance-based metrics
	Hnsw(HnswParams),
	/// Index with Full-Text search capabilities
	FullText(FullTextParams),
	/// Count index
	Count(Option<Cond>),
	/// DiskANN index for distance-based metrics
	#[revision(start = 2)]
	DiskAnn(DiskAnnParams),
}

impl Index {
	pub fn to_sql_definition(&self) -> sql::index::Index {
		match self {
			Self::Idx => sql::index::Index::Idx,
			Self::Uniq => sql::index::Index::Uniq,
			Self::Hnsw(params) => sql::index::Index::Hnsw(params.clone().into()),
			Self::DiskAnn(params) => sql::index::Index::DiskAnn(params.clone().into()),
			Self::FullText(params) => sql::index::Index::FullText(params.clone().into()),
			Self::Count(cond) => sql::index::Index::Count(cond.clone().map(Into::into)),
		}
	}

	/// Returns true if this index type can be used for ORDER BY optimization.
	/// Only indexes storing values in lexicographic order (Idx, Uniq) support ordered iteration.
	pub fn supports_order(&self) -> bool {
		matches!(self, Self::Idx | Self::Uniq)
	}
}

impl InfoStructure for Index {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

impl ToSql for Index {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

/// Full-Text search parameters.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FullTextParams {
	/// The analyzer to use.
	pub analyzer: Strand,
	/// Whether to highlight the search results.
	pub highlight: bool,
	/// The scoring to use.
	pub scoring: Scoring,
}

/// Scoring for Full-Text search.
#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub enum Scoring {
	/// BestMatching25 scoring.
	///
	/// <https://en.wikipedia.org/wiki/Okapi_BM25>
	Bm {
		/// The k~1~ parameter.
		k1: f32,
		/// The b parameter.
		b: f32,
	},
	/// VectorSearch scoring.
	Vs,
}

impl Eq for Scoring {}

impl PartialEq for Scoring {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(
				Scoring::Bm {
					k1,
					b,
				},
				Scoring::Bm {
					k1: other_k1,
					b: other_b,
				},
			) => k1.to_bits() == other_k1.to_bits() && b.to_bits() == other_b.to_bits(),
			(Scoring::Vs, Scoring::Vs) => true,
			_ => false,
		}
	}
}

impl Hash for Scoring {
	fn hash<H: Hasher>(&self, state: &mut H) {
		match self {
			Scoring::Bm {
				k1,
				b,
			} => {
				k1.to_bits().hash(state);
				b.to_bits().hash(state);
			}
			Scoring::Vs => 0.hash(state),
		}
	}
}

impl Default for Scoring {
	fn default() -> Self {
		Self::Bm {
			k1: 1.2,
			b: 0.75,
		}
	}
}

/// Distance metric for calculating distances between vectors.
#[revisioned(revision = 2)]
#[derive(Clone, Default, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Distance {
	/// Chebyshev distance.
	///
	/// <https://en.wikipedia.org/wiki/Chebyshev_distance>
	Chebyshev,
	/// Cosine distance.
	///
	/// <https://en.wikipedia.org/wiki/Cosine_similarity>
	Cosine,
	/// Euclidean distance.
	///
	/// <https://en.wikipedia.org/wiki/Euclidean_distance>
	#[default]
	Euclidean,
	/// Hamming distance.
	///
	/// <https://en.wikipedia.org/wiki/Hamming_distance>
	Hamming,
	/// Jaccard distance.
	///
	/// <https://en.wikipedia.org/wiki/Jaccard_index>
	Jaccard,
	/// Manhattan distance.
	///
	/// <https://en.wikipedia.org/wiki/Manhattan_distance>
	Manhattan,
	/// Minkowski distance.
	///
	/// <https://en.wikipedia.org/wiki/Minkowski_distance>
	Minkowski(Number),
	/// Pearson distance.
	///
	/// <https://en.wikipedia.org/wiki/Pearson_correlation_coefficient>
	Pearson,
	/// Cosine distance for already-normalized vectors.
	#[revision(start = 2)]
	CosineNormalized,
	/// Inner product similarity, transformed as a distance score.
	#[revision(start = 2)]
	InnerProduct,
}

impl Distance {
	pub(crate) fn compute(&self, v1: &Vec<Number>, v2: &Vec<Number>) -> Result<Number> {
		use crate::fnc::util::math::ToFloat;
		use crate::fnc::util::math::vector::{
			ChebyshevDistance, CosineDistance, EuclideanDistance, HammingDistance,
			JaccardSimilarity, ManhattanDistance, MinkowskiDistance, PearsonSimilarity,
			check_same_dimension,
		};
		match self {
			Self::Cosine => v1.cosine_distance(v2),
			Self::CosineNormalized => {
				check_same_dimension("vector::distance::cosine_normalized", v1, v2)?;
				Ok((1.0
					- v1.iter()
						.zip(v2.iter())
						.map(|(a, b)| a.to_float() * b.to_float())
						.sum::<f64>())
				.into())
			}
			Self::Chebyshev => v1.chebyshev_distance(v2),
			Self::Euclidean => v1.euclidean_distance(v2),
			Self::Hamming => v1.hamming_distance(v2),
			Self::InnerProduct => {
				check_same_dimension("vector::distance::inner_product", v1, v2)?;
				Ok((-v1
					.iter()
					.zip(v2.iter())
					.map(|(a, b)| a.to_float() * b.to_float())
					.sum::<f64>())
				.into())
			}
			Self::Jaccard => v1.jaccard_similarity(v2),
			Self::Manhattan => v1.manhattan_distance(v2),
			Self::Minkowski(r) => v1.minkowski_distance(v2, r),
			Self::Pearson => v1.pearson_similarity(v2),
		}
	}
}

impl ToSql for Distance {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Chebyshev => f.push_str("CHEBYSHEV"),
			Self::Cosine => f.push_str("COSINE"),
			Self::CosineNormalized => f.push_str("COSINE_NORMALIZED"),
			Self::Euclidean => f.push_str("EUCLIDEAN"),
			Self::Hamming => f.push_str("HAMMING"),
			Self::InnerProduct => f.push_str("INNER_PRODUCT"),
			Self::Jaccard => f.push_str("JACCARD"),
			Self::Manhattan => f.push_str("MANHATTAN"),
			Self::Minkowski(order) => write_sql!(f, fmt, "MINKOWSKI {}", order),
			Self::Pearson => f.push_str("PEARSON"),
		}
	}
}

/// Vector type for storing vectors.
#[revisioned(revision = 2)]
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Hash)]
pub enum VectorType {
	/// 64-bit floating point.
	F64,
	/// 32-bit floating point.
	#[default]
	F32,
	/// 64-bit signed integer.
	I64,
	/// 32-bit signed integer.
	I32,
	/// 16-bit signed integer.
	I16,
	/// 16-bit floating point.
	#[revision(start = 2)]
	F16,
	/// 8-bit signed integer.
	#[revision(start = 2)]
	I8,
	/// 8-bit unsigned integer.
	#[revision(start = 2)]
	U8,
}

impl Display for VectorType {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::F64 => f.write_str("F64"),
			Self::F16 => f.write_str("F16"),
			Self::F32 => f.write_str("F32"),
			Self::I64 => f.write_str("I64"),
			Self::I32 => f.write_str("I32"),
			Self::I16 => f.write_str("I16"),
			Self::I8 => f.write_str("I8"),
			Self::U8 => f.write_str("U8"),
		}
	}
}

/// HNSW index parameters.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct HnswParams {
	/// The dimension of the index.
	pub dimension: u16,
	/// The distance metric to use.
	pub distance: Distance,
	/// The vector type to use.
	pub vector_type: VectorType,
	/// The m parameter.
	pub m: u8,
	/// The m0 parameter.
	pub m0: u8,
	/// The ml parameter.
	pub ml: Number,
	/// The ef_construction parameter.
	pub ef_construction: u16,
	/// Whether to extend candidates.
	pub extend_candidates: bool,
	/// Whether to keep pruned connections.
	pub keep_pruned_connections: bool,
	/// Whether to use vector hash for vector retrieval.
	#[revision(start = 2)]
	pub use_hashed_vector: bool,
}

/// DiskANN index parameters.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DiskAnnParams {
	/// The dimension of the index.
	pub dimension: u16,
	/// The distance metric to use.
	pub distance: Distance,
	/// The vector type to use.
	pub vector_type: VectorType,
	/// Target graph degree.
	pub degree: u16,
	/// Construction search list size.
	pub l_build: u16,
	/// DiskANN pruning alpha.
	pub alpha: Number,
	/// Whether to use vector hashes for vector retrieval.
	pub use_hashed_vector: bool,
}

#[cfg(test)]
mod tests {
	use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};

	use super::*;

	#[revisioned(revision = 1)]
	#[derive(Clone, Debug, Eq, PartialEq, Hash)]
	enum OldIndex {
		Idx,
		Uniq,
		Hnsw(HnswParams),
		FullText(FullTextParams),
		Count(Option<Cond>),
	}

	#[revisioned(revision = 1)]
	#[derive(Clone, Debug, Eq, PartialEq, Hash)]
	enum OldDistance {
		Chebyshev,
		Cosine,
		Euclidean,
		Hamming,
		Jaccard,
		Manhattan,
		Minkowski(Number),
		Pearson,
	}

	#[revisioned(revision = 1)]
	#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
	enum OldVectorType {
		F64,
		F32,
		I64,
		I32,
		I16,
	}

	fn revision_encode<T: SerializeRevisioned>(value: &T) -> Vec<u8> {
		let mut bytes = Vec::new();
		SerializeRevisioned::serialize_revisioned(value, &mut bytes).unwrap();
		bytes
	}

	fn revision_decode<T: DeserializeRevisioned>(bytes: &[u8]) -> T {
		DeserializeRevisioned::deserialize_revisioned(&mut &*bytes).unwrap()
	}

	#[test]
	fn revision_1_index_variants_keep_their_main_discriminants() {
		let index = revision_decode::<Index>(&revision_encode(&OldIndex::Count(None)));
		assert_eq!(index, Index::Count(None));
	}

	#[test]
	fn revision_1_distance_variants_keep_their_main_discriminants() {
		let distance = revision_decode::<Distance>(&revision_encode(&OldDistance::Pearson));
		assert_eq!(distance, Distance::Pearson);
	}

	#[test]
	fn revision_1_vector_type_variants_keep_their_main_discriminants() {
		let vector_type = revision_decode::<VectorType>(&revision_encode(&OldVectorType::I16));
		assert_eq!(vector_type, VectorType::I16);
	}
}
