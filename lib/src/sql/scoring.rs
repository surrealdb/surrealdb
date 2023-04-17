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
	Bm {
		k1: Number,
		b: Number,
		order: Number,
	}, // BestMatching25
	Vs, // VectorSearch
}

impl Default for Scoring {
	fn default() -> Self {
		Self::Bm {
			k1: Number::Float(1.2),
			b: Number::Float(0.75),
			order: Number::Int(10000),
		}
	}
}

impl fmt::Display for Scoring {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Bm {
				k1,
				b,
				order,
			} => write!(f, "BM25({},{},{})", k1, b, order),
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
		let (i, _) = commas(i)?;
		let (i, order) = number(i)?;
		let (i, _) = closeparenthese(i)?;
		Ok((
			i,
			Scoring::Bm {
				k1,
				b,
				order,
			},
		))
	}))(i)
}
