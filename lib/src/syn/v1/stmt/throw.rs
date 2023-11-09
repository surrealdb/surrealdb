pub fn throw(i: &str) -> IResult<&str, ThrowStatement> {
	let (i, _) = tag_no_case("THROW")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, e) = value(i)?;
	Ok((
		i,
		ThrowStatement {
			error: e,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn throw_basic() {
		let sql = "THROW 'Record does not exist'";
		let res = throw(sql);
		let out = res.unwrap().1;
		assert_eq!("THROW 'Record does not exist'", format!("{}", out))
	}
}
