use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};

use anyhow::Result;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned, revisioned};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{ToSql, write_sql};

use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Idiom};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::val::{Array, Number, Value};

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
	pub(crate) name: String,
	pub(crate) table_name: String,
	pub(crate) cols: Vec<Idiom>,
	pub(crate) index: Index,
	pub(crate) comment: Option<String>,
	pub(crate) decommissioned: bool,
}

impl_kv_value_revisioned!(IndexDefinition);

impl IndexDefinition {
	pub(crate) fn to_sql_definition(&self) -> crate::sql::DefineIndexStatement {
		crate::sql::DefineIndexStatement {
			kind: DefineKind::Default,
			name: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.name.clone())),
			what: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.table_name.clone())),
			cols: self.cols.iter().cloned().map(|x| crate::sql::Expr::Idiom(x.into())).collect(),
			index: self.index.to_sql_definition(),
			comment: self
				.comment
				.clone()
				.map(|x| crate::sql::Expr::Literal(crate::sql::Literal::String(x))),
			concurrently: false,
		}
	}

	pub(crate) fn expect_not_decommissioned(&self) -> Result<()> {
		if self.decommissioned {
			Err(anyhow::Error::new(Error::IndexingBuildingCancelled {
				reason: "Decommissioned.".to_string(),
			}))
		} else {
			Ok(())
		}
	}
}

impl InfoStructure for IndexDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"what".to_string() => self.table_name.into(),
			"cols".to_string() => Value::Array(Array(self.cols.into_iter().map(|x| x.structure()).collect())),
			"index".to_string() => self.index.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
			"decommission".to_string()=> self.decommissioned.into()
		})
	}
}

impl ToSql for IndexDefinition {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}

#[revisioned(revision = 1)]
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
}

impl Index {
	pub fn to_sql_definition(&self) -> crate::sql::index::Index {
		match self {
			Self::Idx => crate::sql::index::Index::Idx,
			Self::Uniq => crate::sql::index::Index::Uniq,
			Self::Hnsw(params) => crate::sql::index::Index::Hnsw(params.clone().into()),
			Self::FullText(params) => crate::sql::index::Index::FullText(params.clone().into()),
			Self::Count(cond) => crate::sql::index::Index::Count(cond.clone().map(Into::into)),
		}
	}
}

impl InfoStructure for Index {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

impl ToSql for Index {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}

/// Full-Text search parameters.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FullTextParams {
	/// The analyzer to use.
	pub analyzer: String,
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
#[revisioned(revision = 1)]
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
}

impl Distance {
	pub(crate) fn compute(&self, v1: &Vec<Number>, v2: &Vec<Number>) -> Result<Number> {
		use crate::fnc::util::math::vector::{
			ChebyshevDistance, CosineDistance, EuclideanDistance, HammingDistance,
			JaccardSimilarity, ManhattanDistance, MinkowskiDistance, PearsonSimilarity,
		};
		match self {
			Self::Cosine => v1.cosine_distance(v2),
			Self::Chebyshev => v1.chebyshev_distance(v2),
			Self::Euclidean => v1.euclidean_distance(v2),
			Self::Hamming => v1.hamming_distance(v2),
			Self::Jaccard => v1.jaccard_similarity(v2),
			Self::Manhattan => v1.manhattan_distance(v2),
			Self::Minkowski(r) => v1.minkowski_distance(v2, r),
			Self::Pearson => v1.pearson_similarity(v2),
		}
	}
}

impl Display for Distance {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::Chebyshev => f.write_str("CHEBYSHEV"),
			Self::Cosine => f.write_str("COSINE"),
			Self::Euclidean => f.write_str("EUCLIDEAN"),
			Self::Hamming => f.write_str("HAMMING"),
			Self::Jaccard => f.write_str("JACCARD"),
			Self::Manhattan => f.write_str("MANHATTAN"),
			Self::Minkowski(order) => write!(f, "MINKOWSKI {}", order),
			Self::Pearson => f.write_str("PEARSON"),
		}
	}
}

/// Vector type for storing vectors.
#[revisioned(revision = 1)]
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
}

impl Display for VectorType {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::F64 => f.write_str("F64"),
			Self::F32 => f.write_str("F32"),
			Self::I64 => f.write_str("I64"),
			Self::I32 => f.write_str("I32"),
			Self::I16 => f.write_str("I16"),
		}
	}
}

/// HNSW index parameters.
#[revisioned(revision = 1)]
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
}
