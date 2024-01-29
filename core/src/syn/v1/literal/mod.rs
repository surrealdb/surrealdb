use super::{
	common::{commas, val_char},
	error::expected,
	IResult,
};
use crate::sql::{Ident, Param, Table, Tables};
use nom::{
	branch::alt,
	bytes::complete::{escaped_transform, is_not, tag, take_while1},
	character::complete::char,
	combinator::{cut, recognize, value},
	multi::separated_list1,
	sequence::delimited,
};

pub mod algorithm;
pub mod datetime;
pub mod duration;
pub mod filter;
pub mod language;
pub mod number;
pub mod range;
pub mod regex;
pub mod scoring;
pub mod strand;
pub mod tokenizer;
pub mod uuid;

pub use self::algorithm::algorithm;
pub use self::datetime::{datetime, datetime_all_raw};
pub use self::duration::duration;
pub use self::filter::filters;
pub use self::number::number;
pub use self::range::range;
pub use self::regex::regex;
pub use self::scoring::scoring;
pub use self::strand::strand;
pub use self::uuid::uuid;

const BRACKET_L: char = '⟨';
const BRACKET_R: char = '⟩';
const BRACKET_END_NUL: &str = "⟩\0";

pub fn ident(i: &str) -> IResult<&str, Ident> {
	let (i, v) = expected("an identifier", ident_raw)(i)?;
	Ok((i, Ident::from(v)))
}

pub fn ident_path(i: &str) -> IResult<&str, Ident> {
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
	let (i, _) = char('`')(i)?;
	let (i, v) = escaped_transform(
		is_not("`\\\0"),
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
	let (i, _) = char('`')(i)?;
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
	use crate::syn::test::Parse;

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
