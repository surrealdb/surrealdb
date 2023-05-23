use crate::sql::common::{closeparentheses, commas, openparentheses};
use crate::sql::error::IResult;
use crate::sql::number::number;
use crate::sql::Number;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Scoring {
	Bm {
		k1: Number,
		b: Number,
	}, // BestMatching25
	Vs, // VectorSearch
}

impl Default for Scoring {
	fn default() -> Self {
		Self::Bm {
			k1: Number::Float(1.2),
			b: Number::Float(0.75),
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
		let (i, k1) = number(i)?;
		let (i, _) = commas(i)?;
		let (i, b) = number(i)?;
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
		assert_eq!("BM25(1.0,0.6)", format!("{}", out))
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
