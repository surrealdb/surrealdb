use crate::sql::ident::Ident;
use crate::sql::scoring::Scoring;

use crate::sql::Number;
use anyhow::Result;
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

impl From<Index> for crate::expr::index::Index {
	fn from(v: Index) -> Self {
		match v {
			Index::Idx => Self::Idx,
			Index::Uniq => Self::Uniq,
			Index::Search(p) => Self::Search(p.into()),
			Index::MTree(p) => Self::MTree(p.into()),
			Index::Hnsw(p) => Self::Hnsw(p.into()),
		}
	}
}

impl From<crate::expr::index::Index> for Index {
	fn from(v: crate::expr::index::Index) -> Self {
		match v {
			crate::expr::index::Index::Idx => Self::Idx,
			crate::expr::index::Index::Uniq => Self::Uniq,
			crate::expr::index::Index::Search(p) => Self::Search(p.into()),
			crate::expr::index::Index::MTree(p) => Self::MTree(p.into()),
			crate::expr::index::Index::Hnsw(p) => Self::Hnsw(p.into()),
		}
	}
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

impl From<SearchParams> for crate::expr::index::SearchParams {
	fn from(v: SearchParams) -> Self {
		crate::expr::index::SearchParams {
			az: v.az.into(),
			hl: v.hl,
			sc: v.sc.into(),
			doc_ids_order: v.doc_ids_order,
			doc_lengths_order: v.doc_lengths_order,
			postings_order: v.postings_order,
			terms_order: v.terms_order,
			doc_ids_cache: v.doc_ids_cache,
			doc_lengths_cache: v.doc_lengths_cache,
			postings_cache: v.postings_cache,
			terms_cache: v.terms_cache,
		}
	}
}
impl From<crate::expr::index::SearchParams> for SearchParams {
	fn from(v: crate::expr::index::SearchParams) -> Self {
		Self {
			az: v.az.into(),
			hl: v.hl,
			sc: v.sc.into(),
			doc_ids_order: v.doc_ids_order,
			doc_lengths_order: v.doc_lengths_order,
			postings_order: v.postings_order,
			terms_order: v.terms_order,
			doc_ids_cache: v.doc_ids_cache,
			doc_lengths_cache: v.doc_lengths_cache,
			postings_cache: v.postings_cache,
			terms_cache: v.terms_cache,
		}
	}
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
	pub fn new(
		dimension: u16,
		distance: Distance,
		vector_type: VectorType,
		capacity: u16,
		doc_ids_order: u32,
		doc_ids_cache: u32,
		mtree_cache: u32,
	) -> Self {
		Self {
			dimension,
			distance,
			vector_type,
			capacity,
			doc_ids_order,
			doc_ids_cache,
			mtree_cache,
		}
	}

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

impl From<MTreeParams> for crate::expr::index::MTreeParams {
	fn from(v: MTreeParams) -> Self {
		crate::expr::index::MTreeParams {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			capacity: v.capacity,
			doc_ids_order: v.doc_ids_order,
			doc_ids_cache: v.doc_ids_cache,
			mtree_cache: v.mtree_cache,
		}
	}
}

impl From<crate::expr::index::MTreeParams> for MTreeParams {
	fn from(v: crate::expr::index::MTreeParams) -> Self {
		Self {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			capacity: v.capacity,
			doc_ids_order: v.doc_ids_order,
			doc_ids_cache: v.doc_ids_cache,
			mtree_cache: v.mtree_cache,
		}
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
	#[expect(clippy::too_many_arguments)]
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

impl From<HnswParams> for crate::expr::index::HnswParams {
	fn from(v: HnswParams) -> Self {
		crate::expr::index::HnswParams {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			m: v.m,
			m0: v.m0,
			ef_construction: v.ef_construction,
			ml: v.ml.into(),
			extend_candidates: v.extend_candidates,
			keep_pruned_connections: v.keep_pruned_connections,
		}
	}
}

impl From<crate::expr::index::HnswParams> for HnswParams {
	fn from(v: crate::expr::index::HnswParams) -> Self {
		Self {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			m: v.m,
			m0: v.m0,
			ef_construction: v.ef_construction,
			ml: v.ml.into(),
			extend_candidates: v.extend_candidates,
			keep_pruned_connections: v.keep_pruned_connections,
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

impl From<Distance> for crate::expr::index::Distance {
	fn from(v: Distance) -> Self {
		match v {
			Distance::Chebyshev => crate::expr::index::Distance::Chebyshev,
			Distance::Cosine => crate::expr::index::Distance::Cosine,
			Distance::Euclidean => crate::expr::index::Distance::Euclidean,
			Distance::Hamming => crate::expr::index::Distance::Hamming,
			Distance::Jaccard => crate::expr::index::Distance::Jaccard,
			Distance::Manhattan => crate::expr::index::Distance::Manhattan,
			Distance::Minkowski(n) => crate::expr::index::Distance::Minkowski(n.into()),
			Distance::Pearson => crate::expr::index::Distance::Pearson,
		}
	}
}

impl From<crate::expr::index::Distance> for Distance {
	fn from(v: crate::expr::index::Distance) -> Self {
		match v {
			crate::expr::index::Distance::Chebyshev => Self::Chebyshev,
			crate::expr::index::Distance::Cosine => Self::Cosine,
			crate::expr::index::Distance::Euclidean => Self::Euclidean,
			crate::expr::index::Distance::Hamming => Self::Hamming,
			crate::expr::index::Distance::Jaccard => Self::Jaccard,
			crate::expr::index::Distance::Manhattan => Self::Manhattan,
			crate::expr::index::Distance::Minkowski(n) => Self::Minkowski(n.into()),
			crate::expr::index::Distance::Pearson => Self::Pearson,
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
					p.dimension,
					p.distance,
					p.vector_type,
					p.capacity,
					p.doc_ids_order,
					p.doc_ids_cache,
					p.mtree_cache
				)
			}
			Self::Hnsw(p) => {
				write!(
					f,
					"HNSW DIMENSION {} DIST {} TYPE {} EFC {} M {} M0 {} LM {}",
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

impl From<VectorType> for crate::expr::index::VectorType {
	fn from(v: VectorType) -> Self {
		match v {
			VectorType::F64 => Self::F64,
			VectorType::F32 => Self::F32,
			VectorType::I64 => Self::I64,
			VectorType::I32 => Self::I32,
			VectorType::I16 => Self::I16,
		}
	}
}

impl From<crate::expr::index::VectorType> for VectorType {
	fn from(v: crate::expr::index::VectorType) -> Self {
		match v {
			crate::expr::index::VectorType::F64 => Self::F64,
			crate::expr::index::VectorType::F32 => Self::F32,
			crate::expr::index::VectorType::I64 => Self::I64,
			crate::expr::index::VectorType::I32 => Self::I32,
			crate::expr::index::VectorType::I16 => Self::I16,
		}
	}
}
