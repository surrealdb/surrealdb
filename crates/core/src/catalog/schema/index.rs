use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};

use anyhow::Result;
use revision::revisioned;

use crate::expr::Idiom;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::sql::{Ident, ToSql};
use crate::val::{Array, Number, Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct IndexDefinition {
	pub name: String,
	pub what: String,
	pub cols: Vec<Idiom>,
	pub index: Index,
	pub comment: Option<String>,
}

impl_kv_value_revisioned!(IndexDefinition);

impl IndexDefinition {
	pub fn to_sql_definition(&self) -> crate::sql::DefineIndexStatement {
		crate::sql::DefineIndexStatement {
			kind: DefineKind::Default,
			name: unsafe { Ident::new_unchecked(self.name.clone()) },
			what: unsafe { Ident::new_unchecked(self.what.clone()) },
			cols: self.cols.iter().cloned().map(Into::into).collect(),
			index: self.index.to_sql_definition(),
			comment: self.comment.clone().map(Strand::new_lossy),
			concurrently: false,
		}
	}
}

impl InfoStructure for IndexDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"what".to_string() => self.what.into(),
			"cols".to_string() => Value::Array(Array(self.cols.into_iter().map(|x| x.structure()).collect())),
			"index".to_string() => self.index.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}

impl ToSql for IndexDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Index {
	/// (Basic) non unique
	#[default]
	Idx,
	/// Unique index
	Uniq,
	/// Index with Full-Text search capabilities
	Search(SearchParams),
	/// M-Tree index for distance based metrics
	MTree(MTreeParams),
	/// HNSW index for distance based metrics
	Hnsw(HnswParams),
	/// Index with Full-Text search capabilities supporting multiple writers
	FullText(FullTextParams),
}

impl Index {
	pub fn to_sql_definition(&self) -> crate::sql::index::Index {
		match self {
			Self::Idx => crate::sql::index::Index::Idx,
			Self::Uniq => crate::sql::index::Index::Uniq,
			Self::Search(params) => crate::sql::index::Index::Search(params.clone().into()),
			Self::MTree(params) => crate::sql::index::Index::MTree(params.clone().into()),
			Self::Hnsw(params) => crate::sql::index::Index::Hnsw(params.clone().into()),
			Self::FullText(params) => crate::sql::index::Index::FullText(params.clone().into()),
		}
	}
}

impl InfoStructure for Index {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

impl ToSql for Index {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SearchParams {
	pub az: String,
	pub hl: bool,
	pub sc: Scoring,
	pub doc_ids_order: u32,
	pub doc_lengths_order: u32,
	pub postings_order: u32,
	pub terms_order: u32,
	pub doc_ids_cache: u32,
	pub doc_lengths_cache: u32,
	pub postings_cache: u32,
	pub terms_cache: u32,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FullTextParams {
	pub analyzer: String,
	pub highlight: bool,
	pub scoring: Scoring,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub enum Scoring {
	Bm {
		k1: f32,
		b: f32,
	}, // BestMatching25
	Vs, // VectorSearch
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MTreeParams {
	pub dimension: u16,
	pub distance: Distance,
	pub vector_type: VectorType,
	pub capacity: u16,
	pub doc_ids_order: u32,
	pub doc_ids_cache: u32,
	pub mtree_cache: u32,
}

#[revisioned(revision = 1)]
#[derive(Clone, Default, Debug, Eq, PartialEq, Hash)]
pub enum Distance {
	Chebyshev,
	Cosine,
	#[default]
	Euclidean,
	Hamming,
	Jaccard,
	Manhattan,
	Minkowski(Number),
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

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Hash)]
pub enum VectorType {
	#[default]
	F64,
	F32,
	I64,
	I32,
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct HnswParams {
	pub dimension: u16,
	pub distance: Distance,
	pub vector_type: VectorType,
	pub m: u8,
	pub m0: u8,
	pub ml: Number,
	pub ef_construction: u16,
	pub extend_candidates: bool,
	pub keep_pruned_connections: bool,
}
