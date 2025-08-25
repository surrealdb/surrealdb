use std::fmt;
use std::fmt::{Display, Formatter};

use crate::sql::ident::Ident;
use crate::sql::scoring::Scoring;
use crate::val::Number;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Index {
	/// (Basic) non unique
	Idx,
	/// Unique index
	Uniq,
	/// Index with Full-Text search capabilities - single writer
	Search(SearchParams),
	/// M-Tree index for distance-based metrics
	MTree(MTreeParams),
	/// HNSW index for distance based metrics
	Hnsw(HnswParams),
	/// Index with Full-Text search capabilities supporting multiple writers
	FullText(FullTextParams),
}

impl From<Index> for crate::catalog::Index {
	fn from(v: Index) -> Self {
		match v {
			Index::Idx => Self::Idx,
			Index::Uniq => Self::Uniq,
			Index::Search(p) => Self::Search(p.into()),
			Index::MTree(p) => Self::MTree(p.into()),
			Index::Hnsw(p) => Self::Hnsw(p.into()),
			Index::FullText(p) => Self::FullText(p.into()),
		}
	}
}

impl From<crate::catalog::Index> for Index {
	fn from(v: crate::catalog::Index) -> Self {
		match v {
			crate::catalog::Index::Idx => Self::Idx,
			crate::catalog::Index::Uniq => Self::Uniq,
			crate::catalog::Index::Search(p) => Self::Search(p.into()),
			crate::catalog::Index::MTree(p) => Self::MTree(p.into()),
			crate::catalog::Index::Hnsw(p) => Self::Hnsw(p.into()),
			crate::catalog::Index::FullText(p) => Self::FullText(p.into()),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SearchParams {
	pub az: Ident,
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

impl From<SearchParams> for crate::catalog::SearchParams {
	fn from(v: SearchParams) -> Self {
		crate::catalog::SearchParams {
			az: v.az.clone().into_string(),
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
impl From<crate::catalog::SearchParams> for SearchParams {
	fn from(v: crate::catalog::SearchParams) -> Self {
		Self {
			az: unsafe { Ident::new_unchecked(v.az) },
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FullTextParams {
	pub az: Ident,
	pub hl: bool,
	pub sc: Scoring,
}

impl From<FullTextParams> for crate::catalog::FullTextParams {
	fn from(v: FullTextParams) -> Self {
		crate::catalog::FullTextParams {
			analyzer: v.az.clone().into_string(),
			highlight: v.hl,
			scoring: v.sc.into(),
		}
	}
}
impl From<crate::catalog::FullTextParams> for FullTextParams {
	fn from(v: crate::catalog::FullTextParams) -> Self {
		Self {
			az: unsafe { Ident::new_unchecked(v.analyzer) },
			hl: v.highlight,
			sc: v.scoring.into(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct MTreeParams {
	pub dimension: u16,
	pub distance: Distance,
	pub vector_type: VectorType,
	pub capacity: u16,
	pub doc_ids_order: u32,
	pub doc_ids_cache: u32,
	pub mtree_cache: u32,
}

impl From<MTreeParams> for crate::catalog::MTreeParams {
	fn from(v: MTreeParams) -> Self {
		crate::catalog::MTreeParams {
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

impl From<crate::catalog::MTreeParams> for MTreeParams {
	fn from(v: crate::catalog::MTreeParams) -> Self {
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<HnswParams> for crate::catalog::HnswParams {
	fn from(v: HnswParams) -> Self {
		crate::catalog::HnswParams {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			m: v.m,
			m0: v.m0,
			ef_construction: v.ef_construction,
			ml: v.ml,
			extend_candidates: v.extend_candidates,
			keep_pruned_connections: v.keep_pruned_connections,
		}
	}
}

impl From<crate::catalog::HnswParams> for HnswParams {
	fn from(v: crate::catalog::HnswParams) -> Self {
		Self {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			m: v.m,
			m0: v.m0,
			ef_construction: v.ef_construction,
			ml: v.ml,
			extend_candidates: v.extend_candidates,
			keep_pruned_connections: v.keep_pruned_connections,
		}
	}
}

#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<Distance> for crate::catalog::Distance {
	fn from(v: Distance) -> Self {
		match v {
			Distance::Chebyshev => crate::catalog::Distance::Chebyshev,
			Distance::Cosine => crate::catalog::Distance::Cosine,
			Distance::Euclidean => crate::catalog::Distance::Euclidean,
			Distance::Hamming => crate::catalog::Distance::Hamming,
			Distance::Jaccard => crate::catalog::Distance::Jaccard,
			Distance::Manhattan => crate::catalog::Distance::Manhattan,
			Distance::Minkowski(n) => crate::catalog::Distance::Minkowski(n),
			Distance::Pearson => crate::catalog::Distance::Pearson,
		}
	}
}

impl From<crate::catalog::Distance> for Distance {
	fn from(v: crate::catalog::Distance) -> Self {
		match v {
			crate::catalog::Distance::Chebyshev => Self::Chebyshev,
			crate::catalog::Distance::Cosine => Self::Cosine,
			crate::catalog::Distance::Euclidean => Self::Euclidean,
			crate::catalog::Distance::Hamming => Self::Hamming,
			crate::catalog::Distance::Jaccard => Self::Jaccard,
			crate::catalog::Distance::Manhattan => Self::Manhattan,
			crate::catalog::Distance::Minkowski(n) => Self::Minkowski(n),
			crate::catalog::Distance::Pearson => Self::Pearson,
		}
	}
}

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
			Self::FullText(p) => {
				write!(f, "FULLTEXT ANALYZER {} {}", p.az, p.sc,)?;
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

impl From<VectorType> for crate::catalog::VectorType {
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

impl From<crate::catalog::VectorType> for VectorType {
	fn from(v: crate::catalog::VectorType) -> Self {
		match v {
			crate::catalog::VectorType::F64 => Self::F64,
			crate::catalog::VectorType::F32 => Self::F32,
			crate::catalog::VectorType::I64 => Self::I64,
			crate::catalog::VectorType::I32 => Self::I32,
			crate::catalog::VectorType::I16 => Self::I16,
		}
	}
}
