use revision::revisioned;
use std::hash::{Hash, Hasher};

use crate::expr::Idiom;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::Number;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct IndexDefinition {
	pub name: String,
	pub what: String,
	pub cols: Vec<Idiom>,
	pub index: Index,
	pub comment: Option<String>,
	pub concurrently: bool,
}

impl_kv_value_revisioned!(IndexDefinition);

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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct HnswParams {
	pub dimension: u16,
	pub distance: Distance,
	pub vector_type: VectorType,
	pub m: u8,
	pub m0: u8,
	pub ef_construction: u16,
	pub extend_candidates: bool,
	pub keep_pruned_connections: bool,
	pub ml: Number,
}
