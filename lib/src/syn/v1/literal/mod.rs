pub mod datetime;
pub mod duration;
pub mod filter;
pub mod geometry;
pub mod language;
pub mod mock;
pub mod number;
pub mod object;
pub mod range;
pub mod regex;
pub mod scoring;
pub mod strand;
pub mod timeout;
pub mod tokenizer;
pub mod uuid;

pub fn ident(i: &str) -> IResult<&str, Ident> {
	let (i, v) = expected("an identifier", ident_raw)(i)?;
	Ok((i, Ident::from(v)))
}

pub fn multi(i: &str) -> IResult<&str, Ident> {
	let (i, v) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;
	Ok((i, Ident::from(v)))
}

pub fn ident_raw(i: &str) -> IResult<&str, String> {
	let (i, v) = alt((ident_default, ident_backtick, ident_brackets))(i)?;
	Ok((i, v))
}

fn ident_default(i: &str) -> IResult<&str, String> {
	let (i, v) = take_while1(val_char)(i)?;
	Ok((i, String::from(v)))
}

fn ident_backtick(i: &str) -> IResult<&str, String> {
	let (i, _) = char(BACKTICK)(i)?;
	let (i, v) = escaped_transform(
		is_not(BACKTICK_ESC_NUL),
		'\\',
		alt((
			value('\u{5c}', char('\\')),
			value('\u{60}', char('`')),
			value('\u{2f}', char('/')),
			value('\u{08}', char('b')),
			value('\u{0c}', char('f')),
			value('\u{0a}', char('n')),
			value('\u{0d}', char('r')),
			value('\u{09}', char('t')),
		)),
	)(i)?;
	let (i, _) = char(BACKTICK)(i)?;
	Ok((i, v))
}

fn ident_brackets(i: &str) -> IResult<&str, String> {
	let (i, v) = delimited(char(BRACKET_L), is_not(BRACKET_END_NUL), char(BRACKET_R))(i)?;
	Ok((i, String::from(v)))
}

pub fn param(i: &str) -> IResult<&str, Param> {
	let (i, _) = char('$')(i)?;
	cut(|i| {
		let (i, v) = ident(i)?;
		Ok((i, Param::from(v)))
	})(i)
}

pub fn table(i: &str) -> IResult<&str, Table> {
	let (i, v) = expected("a table name", ident_raw)(i)?;
	Ok((i, Table(v)))
}

pub fn tables(i: &str) -> IResult<&str, Tables> {
	let (i, v) = separated_list1(commas, table)(i)?;
	Ok((i, Tables(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn ident_normal() {
		let sql = "test";
		let res = ident(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Ident::from("test"));
	}

	#[test]
	fn ident_quoted_backtick() {
		let sql = "`test`";
		let res = ident(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Ident::from("test"));
	}

	#[test]
	fn ident_quoted_brackets() {
		let sql = "⟨test⟩";
		let res = ident(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Ident::from("test"));
	}

	#[test]
	fn param_normal() {
		let sql = "$test";
		let res = param(sql);
		let out = res.unwrap().1;
		assert_eq!("$test", format!("{}", out));
		assert_eq!(out, Param::parse("$test"));
	}

	#[test]
	fn param_longer() {
		let sql = "$test_and_deliver";
		let res = param(sql);
		let out = res.unwrap().1;
		assert_eq!("$test_and_deliver", format!("{}", out));
		assert_eq!(out, Param::parse("$test_and_deliver"));
	}

	#[test]
	fn table_normal() {
		let sql = "test";
		let res = table(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Table(String::from("test")));
	}

	#[test]
	fn table_quoted_backtick() {
		let sql = "`test`";
		let res = table(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Table(String::from("test")));
	}

	#[test]
	fn table_quoted_brackets() {
		let sql = "⟨test⟩";
		let res = table(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Table(String::from("test")));
	}
}
