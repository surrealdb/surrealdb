use std::fmt;
use std::hash::{Hash, Hasher};

use revision::revisioned;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl fmt::Display for Scoring {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Bm {
				k1,
				b,
			} => write!(f, "BM25({},{})", k1, b),
			Self::Vs => f.write_str("VS"),
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
			crate::catalog::Scoring::Vs => Self::Vs,
		}
	}
}
