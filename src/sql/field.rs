use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::expression::{expression, Expression};
use crate::sql::idiom::{idiom, Idiom};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Fields(Vec<Field>);

impl fmt::Display for Fields {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

pub fn fields(i: &str) -> IResult<&str, Fields> {
	let (i, v) = separated_list1(commas, field)(i)?;
	Ok((i, Fields(v)))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Field {
	All,
	Alone(Expression),
	Alias(Expression, Idiom),
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
			Field::Alone(ref e) => write!(f, "{}", e),
			Field::Alias(ref e, ref a) => write!(f, "{} AS {}", e, a),
		}
	}
}

pub fn field(i: &str) -> IResult<&str, Field> {
	alt((all, alias, alone))(i)
}

fn all(i: &str) -> IResult<&str, Field> {
	let (i, _) = tag_no_case("*")(i)?;
	Ok((i, Field::All))
}

fn alone(i: &str) -> IResult<&str, Field> {
	let (i, f) = expression(i)?;
	Ok((i, Field::Alone(f)))
}

fn alias(i: &str) -> IResult<&str, Field> {
	let (i, f) = expression(i)?;
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
