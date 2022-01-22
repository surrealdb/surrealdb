use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::part::{all, field, first, graph, index, part, Part};
use crate::sql::value::Value;
use nom::branch::alt;
use nom::multi::many0;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Idioms(pub Vec<Idiom>);

impl fmt::Display for Idioms {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

pub fn locals(i: &str) -> IResult<&str, Idioms> {
	let (i, v) = separated_list1(commas, local)(i)?;
	Ok((i, Idioms(v)))
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Idiom {
	pub parts: Vec<Part>,
}

impl From<String> for Idiom {
	fn from(v: String) -> Self {
		Idiom {
			parts: vec![Part::from(v)],
		}
	}
}

impl From<Vec<Part>> for Idiom {
	fn from(v: Vec<Part>) -> Self {
		Idiom {
			parts: v,
		}
	}
}

impl Idiom {
	pub fn add(&self, n: Part) -> Idiom {
		let mut p = self.parts.to_vec();
		p.push(n);
		Idiom::from(p)
	}
	pub fn next(&self) -> Idiom {
		match self.parts.len() {
			0 => Idiom::from(vec![]),
			_ => Idiom::from(self.parts[1..].to_vec()),
		}
	}
}

impl Idiom {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &mut Executor,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		match doc {
			// There is a current document
			Some(v) => v.get(ctx, opt, exe, self).await,
			// There isn't any document
			None => Ok(Value::None),
		}
	}
}

impl fmt::Display for Idiom {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{}",
			self.parts
				.iter()
				.enumerate()
				.map(|(i, p)| match (i, p) {
					(0, Part::Field(v)) => format!("{}", v),
					_ => format!("{}", p),
				})
				.collect::<Vec<_>>()
				.join("")
		)
	}
}

// Used in a DEFINE FIELD and DEFINE INDEX clause
pub fn local(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(alt((all, index, field)))(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

// Used in a $param definition
pub fn param(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(part)(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

pub fn idiom(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = alt((first, graph))(i)?;
	let (i, mut v) = many0(part)(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::expression::Expression;
	use crate::sql::test::Parse;

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
				parts: vec![Part::from("test")],
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
				parts: vec![Part::from("test")],
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
				parts: vec![Part::from("test")],
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
				parts: vec![Part::from("test"), Part::from("temp"),],
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
				parts: vec![Part::from("test"), Part::from("some key"),],
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
				parts: vec![Part::from("test"), Part::from("temp"), Part::All,],
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
				parts: vec![Part::from("test"), Part::from("temp"), Part::Last,],
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
				parts: vec![Part::from("test"), Part::from("temp"), Part::All, Part::from("text")],
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
					Part::from("test"),
					Part::from("temp"),
					Part::from(Value::from(Expression::parse("test = true"))),
					Part::from("text")
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
					Part::from("test"),
					Part::from("temp"),
					Part::from(Value::from(Expression::parse("test = true"))),
					Part::from("text")
				],
			}
		);
	}
}
