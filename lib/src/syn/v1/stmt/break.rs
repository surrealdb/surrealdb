use nom::bytes::complete::tag_no_case;

pub fn r#break(i: &str) -> IResult<&str, BreakStatement> {
	let (i, _) = tag_no_case("BREAK")(i)?;
	Ok((i, BreakStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn break_basic() {
		let sql = "BREAK";
		let res = r#break(sql);
		let out = res.unwrap().1;
		assert_eq!("BREAK", format!("{}", out))
	}
}
