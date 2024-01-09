use crate::sql::ident::Ident;
use crate::sql::scoring::Scoring;
use crate::sql::Number;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
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
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
pub struct MTreeParams {
	pub dimension: u16,
	pub distance: Distance,
	pub vector_type: VectorType,
	pub capacity: u16,
	pub doc_ids_order: u32,
	#[revision(start = 2)]
	pub doc_ids_cache: u32,
	#[revision(start = 2)]
	pub mtree_cache: u32,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
pub enum Distance {
	#[default]
	Euclidean,
	Manhattan,
	Hamming,
	Minkowski(Number),
}

impl Display for Distance {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::Euclidean => f.write_str("EUCLIDEAN"),
			Self::Manhattan => f.write_str("MANHATTAN"),
			Self::Hamming => f.write_str("HAMMING"),
			Self::Minkowski(order) => write!(f, "MINKOWSKI {}", order),
		}
	}
}

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
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
		}
	}
}
