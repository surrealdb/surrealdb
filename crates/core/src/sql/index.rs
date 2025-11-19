use std::fmt;
use std::fmt::{Display, Formatter};

use crate::fmt::EscapeIdent;
use crate::sql::Cond;
use crate::sql::scoring::Scoring;
use crate::types::PublicNumber;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Index {
	/// (Basic) non unique
	Idx,
	/// Unique index
	Uniq,
	/// HNSW index for distance based metrics
	Hnsw(HnswParams),
	/// Index with Full-Text search capabilities - single writer
	FullText(FullTextParams),
	/// Count index
	Count(Option<Cond>),
}

impl From<Index> for crate::catalog::Index {
	fn from(v: Index) -> Self {
		match v {
			Index::Idx => Self::Idx,
			Index::Uniq => Self::Uniq,
			Index::Hnsw(p) => Self::Hnsw(p.into()),
			Index::FullText(p) => Self::FullText(p.into()),
			Index::Count(c) => Self::Count(c.map(Into::into)),
		}
	}
}

impl From<crate::catalog::Index> for Index {
	fn from(v: crate::catalog::Index) -> Self {
		match v {
			crate::catalog::Index::Idx => Self::Idx,
			crate::catalog::Index::Uniq => Self::Uniq,
			crate::catalog::Index::Hnsw(p) => Self::Hnsw(p.into()),
			crate::catalog::Index::FullText(p) => Self::FullText(p.into()),
			crate::catalog::Index::Count(c) => Self::Count(c.map(Into::into)),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FullTextParams {
	pub az: String,
	pub hl: bool,
	pub sc: Scoring,
}

impl From<FullTextParams> for crate::catalog::FullTextParams {
	fn from(v: FullTextParams) -> Self {
		crate::catalog::FullTextParams {
			analyzer: v.az.clone(),
			highlight: v.hl,
			scoring: v.sc.into(),
		}
	}
}
impl From<crate::catalog::FullTextParams> for FullTextParams {
	fn from(v: crate::catalog::FullTextParams) -> Self {
		Self {
			az: v.analyzer,
			hl: v.highlight,
			sc: v.scoring.into(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct HnswParams {
	pub dimension: u16,
	pub distance: Distance,
	pub vector_type: VectorType,
	pub m: u8,
	pub m0: u8,
	pub ef_construction: u16,
	pub extend_candidates: bool,
	pub keep_pruned_connections: bool,
	pub ml: PublicNumber,
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
			ml: v.ml.into(),
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
			ml: v.ml.into(),
			extend_candidates: v.extend_candidates,
			keep_pruned_connections: v.keep_pruned_connections,
		}
	}
}

#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Distance {
	Chebyshev,
	Cosine,
	#[default]
	Euclidean,
	Hamming,
	Jaccard,
	Manhattan,
	Minkowski(PublicNumber),
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
			Distance::Minkowski(n) => crate::catalog::Distance::Minkowski(n.into()),
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
			crate::catalog::Distance::Minkowski(n) => Self::Minkowski(n.into()),
			crate::catalog::Distance::Pearson => Self::Pearson,
		}
	}
}

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum VectorType {
	F64,
	#[default]
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
			Self::Count(c) => {
				f.write_str("COUNT")?;
				if let Some(v) = c {
					write!(f, " {v}")?
				}
				Ok(())
			}
			Self::FullText(p) => {
				write!(f, "FULLTEXT ANALYZER {} {}", EscapeIdent(&p.az), p.sc,)?;
				if p.hl {
					f.write_str(" HIGHLIGHTS")?
				}
				Ok(())
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
