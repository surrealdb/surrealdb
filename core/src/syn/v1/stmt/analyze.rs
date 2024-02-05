use super::super::{comment::shouldbespace, literal::ident, IResult};
use crate::sql::statements::AnalyzeStatement;
use nom::{bytes::complete::tag_no_case, combinator::cut};

pub fn analyze(i: &str) -> IResult<&str, AnalyzeStatement> {
	let (i, _) = tag_no_case("ANALYZE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("INDEX")(i)?;
	cut(|i| {
		let (i, _) = shouldbespace(i)?;
		let (i, idx) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("ON")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, tb) = ident(i)?;
		Ok((i, AnalyzeStatement::Idx(tb, idx)))
	})(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::Ident;

	#[test]
	fn analyze_index() {
		let sql = "ANALYZE INDEX my_index ON my_table";
		let res = analyze(sql);
		let out = res.unwrap().1;
		assert_eq!(out, AnalyzeStatement::Idx(Ident::from("my_table"), Ident::from("my_index")));
		assert_eq!("ANALYZE INDEX my_index ON my_table", format!("{}", out));
	}
}
