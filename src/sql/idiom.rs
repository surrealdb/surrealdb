use crate::sql::common::commas;
use crate::sql::common::{escape, val_char};
use crate::sql::filter::{filter, Filter};
use crate::sql::ident::ident_raw;
use nom::bytes::complete::tag;
use nom::combinator::opt;
use nom::multi::separated_nonempty_list;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Idioms(Vec<Idiom>);

impl fmt::Display for Idioms {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

pub fn idioms(i: &str) -> IResult<&str, Idioms> {
	let (i, v) = separated_nonempty_list(commas, idiom)(i)?;
	Ok((i, Idioms(v)))
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Idiom {
	pub parts: Vec<(String, Option<Filter>)>,
}

impl<'a> From<&'a str> for Idiom {
	fn from(s: &str) -> Self {
		idiom(s).unwrap().1
	}
}

impl From<Vec<(String, Option<Filter>)>> for Idiom {
	fn from(v: Vec<(String, Option<Filter>)>) -> Self {
		Idiom {
			parts: v,
		}
	}
}

impl fmt::Display for Idiom {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if self.parts.len() == 1 {
			match self.parts.first().unwrap() {
				(i, Some(ref a)) => write!(f, "{}[{}]", i, a),
				(i, None) => write!(f, "{}", escape(&i, &val_char, "`")),
			}
		} else {
			write!(
				f,
				"{}",
				self.parts
					.iter()
					.map(|(ref i, ref a)| match a {
						Some(ref a) => format!("{}[{}]", i, a),
						None => format!("{}", escape(&i, &val_char, "`")),
					})
					.collect::<Vec<_>>()
					.join(".")
			)
		}
	}
}

pub fn idiom(i: &str) -> IResult<&str, Idiom> {
	let (i, v) = separated_nonempty_list(tag("."), all)(i)?;
	Ok((i, Idiom::from(v)))
}

fn all(i: &str) -> IResult<&str, (String, Option<Filter>)> {
	let (i, v) = raw(i)?;
	let (i, a) = opt(fil)(i)?;
	Ok((i, (v, a)))
}

fn raw(i: &str) -> IResult<&str, String> {
	let (i, v) = ident_raw(i)?;
	Ok((i, String::from(v)))
}

fn fil(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag("[")(i)?;
	let (i, v) = filter(i)?;
	let (i, _) = tag("]")(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn idiom_normal() {
		let sql = "test";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![(String::from("test"), None),],
			}
		);
	}

	#[test]
	fn idiom_quoted_backtick() {
		let sql = "`test`";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![(String::from("test"), None),],
			}
		);
	}

	#[test]
	fn idiom_quoted_brackets() {
		let sql = "⟨test⟩";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![(String::from("test"), None),],
			}
		);
	}

	#[test]
	fn idiom_nested() {
		let sql = "test.temp";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![(String::from("test"), None), (String::from("temp"), None),],
			}
		);
	}

	#[test]
	fn idiom_nested_quoted() {
		let sql = "test.`some key`";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.`some key`", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![(String::from("test"), None), (String::from("some key"), None),],
			}
		);
	}

	#[test]
	fn idiom_nested_array_all() {
		let sql = "test.temp[*]";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[*]", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![
					(String::from("test"), None),
					(String::from("temp"), Some(Filter::from("*"))),
				],
			}
		);
	}

	#[test]
	fn idiom_nested_array_last() {
		let sql = "test.temp[$]";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[$]", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![
					(String::from("test"), None),
					(String::from("temp"), Some(Filter::from("$"))),
				],
			}
		);
	}

	#[test]
	fn idiom_nested_array_value() {
		let sql = "test.temp[*].text";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[*].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![
					(String::from("test"), None),
					(String::from("temp"), Some(Filter::from("*"))),
					(String::from("text"), None),
				],
			}
		);
	}

	#[test]
	fn idiom_nested_array_question() {
		let sql = "test.temp[? test = true].text";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![
					(String::from("test"), None),
					(String::from("temp"), Some(Filter::from("WHERE test = true"))),
					(String::from("text"), None),
				],
			}
		);
	}

	#[test]
	fn idiom_nested_array_condition() {
		let sql = "test.temp[WHERE test = true].text";
		let res = idiom(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom {
				parts: vec![
					(String::from("test"), None),
					(String::from("temp"), Some(Filter::from("WHERE test = true"))),
					(String::from("text"), None),
				],
			}
		);
	}
}
