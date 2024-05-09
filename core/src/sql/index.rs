use crate::err::Error;
use crate::fnc::util::math::vector::{
	ChebyshevDistance, CosineDistance, EuclideanDistance, HammingDistance, JaccardSimilarity,
	ManhattanDistance, MinkowskiDistance, PearsonSimilarity,
};
use crate::sql::ident::Ident;
use crate::sql::scoring::Scoring;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Number, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	#[revision(start = 2)]
	Hnsw(HnswParams),
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct SearchParams {
	pub az: Ident,
	pub hl: bool,
	pub sc: Scoring,
	pub doc_ids_order: u32,
	pub doc_lengths_order: u32,
	pub postings_order: u32,
	pub terms_order: u32,
	#[revision(start = 2)]
	pub doc_ids_cache: u32,
	#[revision(start = 2)]
	pub doc_lengths_cache: u32,
	#[revision(start = 2)]
	pub postings_cache: u32,
	#[revision(start = 2)]
	pub terms_cache: u32,
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct MTreeParams {
	pub dimension: u16,
	#[revision(start = 1, end = 2, convert_fn = "convert_old_distance")]
	pub _distance: Distance1, // TODO remove once 1.0 && 1.1 are EOL
	#[revision(start = 2)]
	pub distance: Distance,
	pub vector_type: VectorType,
	pub capacity: u16,
	pub doc_ids_order: u32,
	#[revision(start = 2)]
	pub doc_ids_cache: u32,
	#[revision(start = 2)]
	pub mtree_cache: u32,
}

impl MTreeParams {
	fn convert_old_distance(
		&mut self,
		_revision: u16,
		d1: Distance1,
	) -> Result<(), revision::Error> {
		self.distance = match d1 {
			Distance1::Euclidean => Distance::Euclidean,
			Distance1::Manhattan => Distance::Manhattan,
			Distance1::Cosine => Distance::Cosine,
			Distance1::Hamming => Distance::Hamming,
			Distance1::Minkowski(n) => Distance::Minkowski(n),
		};
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Distance1 {
	#[default]
	Euclidean,
	Manhattan,
	Cosine,
	Hamming,
	Minkowski(Number),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

impl HnswParams {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		dimension: u16,
		distance: Distance,
		vector_type: VectorType,
		m: u8,
		m0: u8,
		ml: Number,
		ef_construction: u16,
		extend_candidates: bool,
		keep_pruned_connections: bool,
	) -> Self {
		Self {
			dimension,
			distance,
			vector_type,
			m,
			m0,
			ef_construction,
			ml,
			extend_candidates,
			keep_pruned_connections,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	pub(crate) fn compute(&self, v1: &Vec<Number>, v2: &Vec<Number>) -> Result<Number, Error> {
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
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

impl Display for Index {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Idx => Ok(()),
			Self::Uniq => f.write_str("UNIQUE"),
			Self::Search(p) => {
				write!(
					f,
					"SEARCH ANALYZER {} {} DOC_IDS_ORDER {} DOC_LENGTHS_ORDER {} POSTINGS_ORDER {} TERMS_ORDER {} DOC_IDS_CACHE {} DOC_LENGTHS_CACHE {} POSTINGS_CACHE {} TERMS_CACHE {}",
					p.az,
					p.sc,
					p.doc_ids_order,
					p.doc_lengths_order,
					p.postings_order,
					p.terms_order,
					p.doc_ids_cache,
					p.doc_lengths_cache,
					p.postings_cache,
					p.terms_cache
				)?;
				if p.hl {
					f.write_str(" HIGHLIGHTS")?
				}
				Ok(())
			}
			Self::MTree(p) => {
				write!(
					f,
					"MTREE DIMENSION {} DIST {} TYPE {} CAPACITY {} DOC_IDS_ORDER {} DOC_IDS_CACHE {} MTREE_CACHE {}",
					p.dimension, p.distance, p.vector_type, p.capacity, p.doc_ids_order, p.doc_ids_cache, p.mtree_cache
				)
			}
			Self::Hnsw(p) => {
				write!(
					f,
					"HNSW DIMENSION {} DIST {} TYPE {} EFC {} M {} M0 {} ML {}",
					p.dimension, p.distance, p.vector_type, p.ef_construction, p.m, p.m0, p.ml
				)?;
				if p.extend_candidates {
					f.write_str(" EXTEND_CANDIDATES")?
				}
				if p.keep_pruned_connections {
					f.write_str(" KEEP_PRUNED_CONNECTIONS")?
				}
				Ok(())
			}
		}
	}
}

impl InfoStructure for Index {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
