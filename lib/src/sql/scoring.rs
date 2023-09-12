use crate::sql::common::{closeparentheses, commas, openparentheses};
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{cut, map_res, value};
use nom::number::complete::recognize_float;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

use super::util::expect_delimited;

#[derive(Clone, Debug, PartialOrd, Serialize, Deserialize)]
#[revisioned(revision = 1)]
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

impl Scoring {
	pub(crate) fn bm25() -> Self {
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

pub fn scoring(i: &str) -> IResult<&str, Scoring> {
	alt((
		value(Scoring::Vs, tag_no_case("VS")),
		|i| {
			let (i, _) = tag_no_case("BM25")(i)?;
			expect_delimited(
				openparentheses,
				|i| {
					let (i, k1) = cut(map_res(recognize_float, |x: &str| x.parse::<f32>()))(i)?;
					let (i, _) = cut(commas)(i)?;
					let (i, b) = cut(map_res(recognize_float, |x: &str| x.parse::<f32>()))(i)?;
					Ok((
						i,
						Scoring::Bm {
							k1,
							b,
						},
					))
				},
				closeparentheses,
			)(i)
		},
		value(Scoring::bm25(), tag_no_case("BM25")),
	))(i)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn scoring_bm_25_with_parameters() {
		let sql = "BM25(1.0,0.6)";
		let res = scoring(sql);
		let out = res.unwrap().1;
		assert_eq!("BM25(1,0.6)", format!("{}", out))
	}

	#[test]
	fn scoring_bm_25_without_parameters() {
		let sql = "BM25";
		let res = scoring(sql);
		let out = res.unwrap().1;
		assert_eq!("BM25(1.2,0.75)", format!("{}", out))
	}

	#[test]
	fn scoring_vs() {
		let sql = "VS";
		let res = scoring(sql);
		let out = res.unwrap().1;
		assert_eq!("VS", format!("{}", out))
	}
}
