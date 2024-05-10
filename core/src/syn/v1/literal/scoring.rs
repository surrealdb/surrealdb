use super::super::{
	common::{closeparentheses, commas, expect_delimited, openparentheses},
	IResult,
};
use crate::sql::Scoring;
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{cut, map_res, value},
	number::complete::recognize_float,
};

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
		value(Default::default(), tag_no_case("BM25")),
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
