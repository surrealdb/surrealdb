fn tokenizer(i: &str) -> IResult<&str, Tokenizer> {
	let (i, t) = alt((
		value(Tokenizer::Blank, tag_no_case("BLANK")),
		value(Tokenizer::Camel, tag_no_case("CAMEL")),
		value(Tokenizer::Class, tag_no_case("CLASS")),
		value(Tokenizer::Punct, tag_no_case("PUNCT")),
	))(i)?;
	Ok((i, t))
}

pub(super) fn tokenizers(i: &str) -> IResult<&str, Vec<Tokenizer>> {
	separated_list1(commas, tokenizer)(i)
}
