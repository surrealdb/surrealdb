use crate::sql::common::{closeparentheses, commas, openparentheses};
use crate::sql::error::IResult;
use crate::sql::Error::Parser;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::number::complete::recognize_float;
use nom::Err::Failure;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Serialize, Deserialize)]
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

pub fn scoring(i: &str) -> IResult<&str, Scoring> {
	alt((map(tag_no_case("VS"), |_| Scoring::Vs), |i| {
		let (i, _) = tag_no_case("BM25")(i)?;
		let (i, _) = openparentheses(i)?;
		let (i, k1) = recognize_float(i)?;
		let k1 = k1.parse::<f32>().map_err(|_| Failure(Parser(i)))?;
		let (i, _) = commas(i)?;
		let (i, b) = recognize_float(i)?;
		let b = b.parse::<f32>().map_err(|_| Failure(Parser(i)))?;
		let (i, _) = closeparentheses(i)?;
		Ok((
			i,
			Scoring::Bm {
				k1,
				b,
			},
		))
	}))(i)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn scoring_bm_25() {
		let sql = "BM25(1.0,0.6)";
		let res = scoring(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("BM25(1,0.6)", format!("{}", out))
	}

	#[test]
	fn scoring_vs() {
		let sql = "VS";
		let res = scoring(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("VS", format!("{}", out))
	}
}
