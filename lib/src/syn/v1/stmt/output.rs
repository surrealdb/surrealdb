use super::super::{comment::shouldbespace, part::fetch, value::value, IResult};
use crate::sql::statements::OutputStatement;
use nom::{bytes::complete::tag_no_case, combinator::opt, sequence::preceded};

pub fn output(i: &str) -> IResult<&str, OutputStatement> {
	let (i, _) = tag_no_case("RETURN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = value(i)?;
	let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
	Ok((
		i,
		OutputStatement {
			what,
			fetch,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn output_statement() {
		let sql = "RETURN $param";
		let res = output(sql);
		let out = res.unwrap().1;
		assert_eq!("RETURN $param", format!("{}", out));
	}
}
