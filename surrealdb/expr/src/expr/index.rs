//! Index parameters shared by parsing, planning, and schema compilation.

use surrealdb_strand::Strand;
use surrealdb_types::{Number as PublicNumber, SqlFormat, ToSql, write_sql};

use crate::expr::scoring::Scoring;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FullTextParams {
	pub az: Strand,
	pub hl: bool,
	pub sc: Scoring,
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
	pub ml: PublicNumber,
	pub use_hashed_vector: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DiskAnnParams {
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

#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Distance {
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
