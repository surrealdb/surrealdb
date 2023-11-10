use super::super::{
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, scoring, table, tables, timeout},
	operator::{assigner, dir},
	part::{data, output},
	thing::thing,
	value::{value, whats},
	IResult,
};
use crate::sql::statements::CreateStatement;
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, into, map, map_res, opt, recognize, value as map_value},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

pub fn create(i: &str) -> IResult<&str, CreateStatement> {
	let (i, _) = tag_no_case("CREATE")(i)?;
	let (i, only) = opt(preceded(shouldbespace, tag_no_case("ONLY")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, (data, output, timeout, parallel)) = cut(|i| {
		let (i, data) = opt(preceded(shouldbespace, data))(i)?;
		let (i, output) = opt(preceded(shouldbespace, output))(i)?;
		let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
		let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
		Ok((i, (data, output, timeout, parallel)))
	})(i)?;
	Ok((
		i,
		CreateStatement {
			only: only.is_some(),
			what,
			data,
			output,
			timeout,
			parallel: parallel.is_some(),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn create_statement() {
		let sql = "CREATE test";
		let res = create(sql);
		let out = res.unwrap().1;
		assert_eq!("CREATE test", format!("{}", out))
	}
}
