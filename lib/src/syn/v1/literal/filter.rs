fn ascii(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("ASCII")(i)?;
	Ok((i, Filter::Ascii))
}

fn edgengram(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("EDGENGRAM")(i)?;
	cut(|i| {
		let (i, _) = openparentheses(i)?;
		let (i, min) = u16(i)?;
		let (i, _) = commas(i)?;
		let (i, max) = u16(i)?;
		let (i, _) = closeparentheses(i)?;
		Ok((i, Filter::EdgeNgram(min, max)))
	})(i)
}

fn ngram(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("NGRAM")(i)?;
	cut(|i| {
		let (i, _) = openparentheses(i)?;
		let (i, min) = u16(i)?;
		let (i, _) = commas(i)?;
		let (i, max) = u16(i)?;
		let (i, _) = closeparentheses(i)?;
		Ok((i, Filter::Ngram(min, max)))
	})(i)
}

fn lowercase(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("LOWERCASE")(i)?;
	Ok((i, Filter::Lowercase))
}

fn snowball(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("SNOWBALL")(i)?;
	cut(|i| {
		let (i, _) = openparentheses(i)?;
		let (i, language) = language(i)?;
		let (i, _) = closeparentheses(i)?;
		Ok((i, Filter::Snowball(language)))
	})(i)
}

fn uppercase(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("UPPERCASE")(i)?;
	Ok((i, Filter::Uppercase))
}

fn filter(i: &str) -> IResult<&str, Filter> {
	alt((ascii, edgengram, lowercase, ngram, snowball, uppercase))(i)
}

pub(super) fn filters(i: &str) -> IResult<&str, Vec<Filter>> {
	separated_list1(commas, filter)(i)
}
