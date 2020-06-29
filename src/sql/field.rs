use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::expression::{expression, Expression};
use crate::sql::idiom::{idiom, Idiom};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::multi::separated_nonempty_list;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Fields(Vec<Field>);

impl fmt::Display for Fields {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

pub fn fields(i: &str) -> IResult<&str, Fields> {
	let (i, v) = separated_nonempty_list(commas, field)(i)?;
	Ok((i, Fields(v)))
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Field {
	pub field: Expression,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub alias: Option<Idiom>,
}

impl fmt::Display for Field {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if let Some(ref a) = self.alias {
			write!(f, "{} AS {}", self.field, a)
		} else {
			write!(f, "{}", self.field)
		}
	}
}

pub fn field(i: &str) -> IResult<&str, Field> {
	let (i, f) = expression(i)?;
	let (i, a) = opt(alias)(i)?;
	Ok((
		i,
		Field {
			field: f,
			alias: a,
		},
	))
}

fn alias(i: &str) -> IResult<&str, Idiom> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("AS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = idiom(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

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
