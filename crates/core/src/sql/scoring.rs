use std::hash::{Hash, Hasher};

use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Scoring {
	Bm {
		k1: f32,
		b: f32,
	}, // BestMatching25
	#[revision(start = 2)]
	BmAccurate {
		k1: f32,
		b: f32,
	}, // BestMatching25 with accurate (non-optimized) scoring
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
			(
				Scoring::BmAccurate {
					k1,
					b,
				},
				Scoring::BmAccurate {
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
				0u8.hash(state);
				k1.to_bits().hash(state);
				b.to_bits().hash(state);
			}
			Scoring::BmAccurate {
				k1,
				b,
			} => {
				1u8.hash(state);
				k1.to_bits().hash(state);
				b.to_bits().hash(state);
			}
			Scoring::Vs => 2u8.hash(state),
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

impl ToSql for Scoring {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Bm {
				k1,
				b,
			} => write_sql!(f, sql_fmt, "BM25({},{})", k1, b),
			Self::BmAccurate {
				k1,
				b,
			} => write_sql!(f, sql_fmt, "BM25_ACCURATE({},{})", k1, b),
			Self::Vs => write_sql!(f, sql_fmt, "VS"),
		}
	}
}

impl From<Scoring> for crate::catalog::Scoring {
	fn from(v: Scoring) -> Self {
		match v {
			Scoring::Bm {
				k1,
				b,
			} => crate::catalog::Scoring::Bm {
				k1,
				b,
			},
			Scoring::BmAccurate {
				k1,
				b,
			} => crate::catalog::Scoring::BmAccurate {
				k1,
				b,
			},
			Scoring::Vs => crate::catalog::Scoring::Vs,
		}
	}
}
impl From<crate::catalog::Scoring> for Scoring {
	fn from(v: crate::catalog::Scoring) -> Self {
		match v {
			crate::catalog::Scoring::Bm {
				k1,
				b,
			} => Self::Bm {
				k1,
				b,
			},
			crate::catalog::Scoring::BmAccurate {
				k1,
				b,
			} => Self::BmAccurate {
				k1,
				b,
			},
			crate::catalog::Scoring::Vs => Self::Vs,
		}
	}
}
