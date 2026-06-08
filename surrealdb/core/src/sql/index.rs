use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeKwFreeIdent;
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
	/// DiskANN index for distance based metrics
	DiskAnn(DiskAnnParams),
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
			Index::DiskAnn(p) => Self::DiskAnn(p.into()),
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
			crate::catalog::Index::DiskAnn(p) => Self::DiskAnn(p.into()),
			crate::catalog::Index::FullText(p) => Self::FullText(p.into()),
			crate::catalog::Index::Count(c) => Self::Count(c.map(Into::into)),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FullTextParams {
	pub az: Strand,
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
			az: v.analyzer.clone(),
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
	pub use_hashed_vector: bool,
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
			use_hashed_vector: v.use_hashed_vector,
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
			use_hashed_vector: v.use_hashed_vector,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DiskAnnParams {
	/// The vector dimension.
	pub dimension: u16,
	/// The distance metric used by the DiskANN graph.
	pub distance: Distance,
	/// The element type used to encode vectors in the index.
	pub vector_type: VectorType,
	/// Target maximum graph degree.
	pub degree: u16,
	/// Construction search list size.
	pub l_build: u16,
	/// DiskANN pruning alpha.
	pub alpha: PublicNumber,
	/// Whether vector-document mappings are keyed by vector hash.
	pub use_hashed_vector: bool,
}

impl From<DiskAnnParams> for crate::catalog::DiskAnnParams {
	fn from(v: DiskAnnParams) -> Self {
		crate::catalog::DiskAnnParams {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			degree: v.degree,
			l_build: v.l_build,
			alpha: v.alpha.into(),
			use_hashed_vector: v.use_hashed_vector,
		}
	}
}

impl From<crate::catalog::DiskAnnParams> for DiskAnnParams {
	fn from(v: crate::catalog::DiskAnnParams) -> Self {
		Self {
			dimension: v.dimension,
			distance: v.distance.into(),
			vector_type: v.vector_type.into(),
			degree: v.degree,
			l_build: v.l_build,
			alpha: v.alpha.into(),
			use_hashed_vector: v.use_hashed_vector,
		}
	}
}

#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Distance {
	/// Chebyshev distance.
	Chebyshev,
	/// Cosine distance.
	Cosine,
	/// Euclidean distance.
	#[default]
	Euclidean,
	/// Hamming distance.
	Hamming,
	/// Jaccard distance.
	Jaccard,
	/// Manhattan distance.
	Manhattan,
	/// Minkowski distance with the supplied order.
	Minkowski(PublicNumber),
	/// Pearson similarity.
	Pearson,
	/// Cosine distance for already-normalized vectors.
	CosineNormalized,
	/// Inner product similarity, transformed into a distance score.
	InnerProduct,
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

impl From<Distance> for crate::catalog::Distance {
	fn from(v: Distance) -> Self {
		match v {
			Distance::Chebyshev => crate::catalog::Distance::Chebyshev,
			Distance::Cosine => crate::catalog::Distance::Cosine,
			Distance::CosineNormalized => crate::catalog::Distance::CosineNormalized,
			Distance::Euclidean => crate::catalog::Distance::Euclidean,
			Distance::Hamming => crate::catalog::Distance::Hamming,
			Distance::InnerProduct => crate::catalog::Distance::InnerProduct,
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
			crate::catalog::Distance::CosineNormalized => Self::CosineNormalized,
			crate::catalog::Distance::Euclidean => Self::Euclidean,
			crate::catalog::Distance::Hamming => Self::Hamming,
			crate::catalog::Distance::InnerProduct => Self::InnerProduct,
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
	F16,
	/// 8-bit signed integer.
	I8,
	/// 8-bit unsigned integer.
	U8,
}

impl ToSql for VectorType {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			Self::F64 => f.push_str("F64"),
			Self::F16 => f.push_str("F16"),
			Self::F32 => f.push_str("F32"),
			Self::I64 => f.push_str("I64"),
			Self::I32 => f.push_str("I32"),
			Self::I16 => f.push_str("I16"),
			Self::I8 => f.push_str("I8"),
			Self::U8 => f.push_str("U8"),
		}
	}
}

impl ToSql for Index {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Idx => {}
			Self::Uniq => f.push_str("UNIQUE"),
			Self::Count(c) => {
				f.push_str("COUNT");
				if let Some(v) = c {
					write_sql!(f, fmt, " {}", v)
				}
			}
			Self::FullText(p) => {
				write_sql!(f, fmt, "FULLTEXT ANALYZER {} {}", EscapeKwFreeIdent(&p.az), p.sc);
				if p.hl {
					f.push_str(" HIGHLIGHTS")
				}
			}
			Self::Hnsw(p) => {
				write_sql!(
					f,
					fmt,
					"HNSW DIMENSION {} DIST {} TYPE {} EFC {} M {} M0 {} LM {}",
					p.dimension,
					p.distance,
					p.vector_type,
					p.ef_construction,
					p.m,
					p.m0,
					p.ml
				);
				if p.extend_candidates {
					f.push_str(" EXTEND_CANDIDATES")
				}
				if p.keep_pruned_connections {
					f.push_str(" KEEP_PRUNED_CONNECTIONS")
				}
				if p.use_hashed_vector {
					f.push_str(" HASHED_VECTOR")
				}
			}
			Self::DiskAnn(p) => {
				write_sql!(
					f,
					fmt,
					"DISKANN DIMENSION {} DIST {} TYPE {} DEGREE {} L_BUILD {} ALPHA {}",
					p.dimension,
					p.distance,
					p.vector_type,
					p.degree,
					p.l_build,
					p.alpha
				);
				if p.use_hashed_vector {
					f.push_str(" HASHED_VECTOR")
				}
			}
		}
	}
}

impl From<VectorType> for crate::catalog::VectorType {
	fn from(v: VectorType) -> Self {
		match v {
			VectorType::F64 => Self::F64,
			VectorType::F16 => Self::F16,
			VectorType::F32 => Self::F32,
			VectorType::I64 => Self::I64,
			VectorType::I32 => Self::I32,
			VectorType::I16 => Self::I16,
			VectorType::I8 => Self::I8,
			VectorType::U8 => Self::U8,
		}
	}
}

impl From<crate::catalog::VectorType> for VectorType {
	fn from(v: crate::catalog::VectorType) -> Self {
		match v {
			crate::catalog::VectorType::F64 => Self::F64,
			crate::catalog::VectorType::F16 => Self::F16,
			crate::catalog::VectorType::F32 => Self::F32,
			crate::catalog::VectorType::I64 => Self::I64,
			crate::catalog::VectorType::I32 => Self::I32,
			crate::catalog::VectorType::I16 => Self::I16,
			crate::catalog::VectorType::I8 => Self::I8,
			crate::catalog::VectorType::U8 => Self::U8,
		}
	}
}
