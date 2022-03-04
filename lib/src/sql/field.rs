use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Fields(pub Vec<Field>);

impl Fields {
	pub fn all(&self) -> bool {
		self.0.iter().any(|v| matches!(v, Field::All))
	}
	pub fn other(&self) -> impl Iterator<Item = &Field> {
		self.0.iter().filter(|v| !matches!(v, Field::All))
	}
	pub fn single(&self) -> Option<Idiom> {
		match self.0.len() {
			1 => match self.0.first() {
				Some(Field::All) => None,
				Some(Field::Alone(e)) => Some(e.to_idiom()),
				Some(Field::Alias(_, i)) => Some(i.to_owned()),
				_ => None,
			},
			_ => None,
		}
	}
}

impl fmt::Display for Fields {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

pub fn fields(i: &str) -> IResult<&str, Fields> {
	let (i, v) = separated_list1(commas, field)(i)?;
	Ok((i, Fields(v)))
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Field {
	All,
	Alone(Value),
	Alias(Value, Idiom),
}

impl Default for Field {
	fn default() -> Field {
		Field::All
	}
}

impl fmt::Display for Field {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Field::All => write!(f, "*"),
			Field::Alone(e) => write!(f, "{}", e),
			Field::Alias(e, a) => write!(f, "{} AS {}", e, a),
		}
	}
}

pub fn field(i: &str) -> IResult<&str, Field> {
	alt((all, alias, alone))(i)
}

pub fn all(i: &str) -> IResult<&str, Field> {
	let (i, _) = tag_no_case("*")(i)?;
	Ok((i, Field::All))
}

pub fn alone(i: &str) -> IResult<&str, Field> {
	let (i, f) = value(i)?;
	Ok((i, Field::Alone(f)))
}

pub fn alias(i: &str) -> IResult<&str, Field> {
	let (i, f) = value(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("AS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, a) = idiom(i)?;
	Ok((i, Field::Alias(f, a)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn field_all() {
		let sql = "*";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("*", format!("{}", out));
	}

	#[test]
	fn field_single() {
		let sql = "field";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("field", format!("{}", out));
	}

	#[test]
	fn field_multiple() {
		let sql = "field, other.field";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("field, other.field", format!("{}", out));
	}

	#[test]
	fn field_aliases() {
		let sql = "field AS one, other.field AS two";
		let res = fields(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("field AS one, other.field AS two", format!("{}", out));
	}
}
