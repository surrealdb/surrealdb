use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::part::{all, field, first, graph, index, last, part, Part};
use crate::sql::value::Value;
use nom::branch::alt;
use nom::multi::many0;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Idioms(pub Vec<Idiom>);

impl Idioms {
	pub fn len(&self) -> usize {
		self.0.len()
	}
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}
}

impl Deref for Idioms {
	type Target = Vec<Idiom>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

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

impl Deref for Idiom {
	type Target = [Part];
	fn deref(&self) -> &Self::Target {
		self.parts.as_slice()
	}
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
	pub fn add(mut self, n: Part) -> Idiom {
		self.parts.push(n);
		self
	}

	pub fn to_path(&self) -> String {
		format!("/{}", self).replace(']', "").replace(&['.', '['][..], "/")
	}
}

impl Idiom {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		match doc {
			// There is a current document
			Some(v) => v.get(ctx, opt, txn, self).await?.compute(ctx, opt, txn, doc).await,
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

// Used in a DEFINE FIELD and DEFINE INDEX clauses
pub fn local(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(alt((all, index, field)))(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

// Used in a SPLIT, ORDER, and GROUP clauses
pub fn basic(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(alt((all, last, index, field)))(i)?;
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
