pub fn timeout(i: &str) -> IResult<&str, Timeout> {
	let (i, _) = tag_no_case("TIMEOUT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(duration)(i)?;
	Ok((i, Timeout(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn timeout_statement() {
		let sql = "TIMEOUT 5s";
		let res = timeout(sql);
		let out = res.unwrap().1;
		assert_eq!("TIMEOUT 5s", format!("{}", out));
		assert_eq!(out, Timeout(Duration::try_from("5s").unwrap()));
	}
}
