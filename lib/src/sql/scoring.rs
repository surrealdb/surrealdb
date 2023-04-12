use crate::sql::common::{closeparenthese, commas, openparenthese};
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
	Bm(Number, Number), // BestMatching25
	Vs,                 // VectorSearch
}

impl Default for Scoring {
	fn default() -> Self {
		Self::Bm(Number::Float(1.2), Number::Float(0.75))
	}
}

impl fmt::Display for Scoring {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Bm(k1, b) => write!(f, "BM25({},{})", k1, b),
			Self::Vs => f.write_str("VS"),
		}
	}
}

pub fn scoring(i: &str) -> IResult<&str, Scoring> {
	alt((map(tag_no_case("VS"), |_| Scoring::Vs), |i| {
		let (i, _) = tag_no_case("BM25")(i)?;
		let (i, _) = openparenthese(i)?;
		let (i, k1) = number(i)?;
		let (i, _) = commas(i)?;
		let (i, b) = number(i)?;
		let (i, _) = closeparenthese(i)?;
		Ok((i, Scoring::Bm(k1, b)))
	}))(i)
}
