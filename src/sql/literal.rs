use crate::sql::array::{array, Array};
use crate::sql::common::commas;
use crate::sql::datetime::{datetime, Datetime};
use crate::sql::duration::{duration, Duration};
use crate::sql::function::{function, Function};
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::model::{model, Model};
use crate::sql::number::{number, Number};
use crate::sql::object::{object, Object};
use crate::sql::param::{param, Param};
use crate::sql::strand::{strand, Strand};
use crate::sql::subquery::{subquery, Subquery};
use crate::sql::thing::{thing, Thing};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::multi::separated_nonempty_list;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Literals(Vec<Literal>);

impl fmt::Display for Literals {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Literal {
	Null,
	Bool(bool),
	Param(Param),
	Idiom(Idiom),
	Thing(Thing),
	Model(Model),
	Array(Array),
	Object(Object),
	Number(Number),
	Strand(Strand),
	Duration(Duration),
	Datetime(Datetime),
	Function(Function),
	Subquery(Subquery),
}

impl Default for Literal {
	fn default() -> Literal {
		Literal::Null
	}
}

impl From<f32> for Literal {
	fn from(f: f32) -> Self {
		Literal::Number(Number::from(f))
	}
}

impl From<f64> for Literal {
	fn from(f: f64) -> Self {
		Literal::Number(Number::from(f))
	}
}

impl<'a> From<&'a str> for Literal {
	fn from(s: &str) -> Self {
		Literal::Strand(Strand::from(s))
	}
}

impl fmt::Display for Literal {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Literal::Null => write!(f, "NULL"),
			Literal::Bool(true) => write!(f, "true"),
			Literal::Bool(false) => write!(f, "false"),
			Literal::Param(v) => write!(f, "{}", v),
			Literal::Idiom(v) => write!(f, "{}", v),
			Literal::Thing(v) => write!(f, "{}", v),
			Literal::Model(v) => write!(f, "{}", v),
			Literal::Array(v) => write!(f, "{}", v),
			Literal::Object(v) => write!(f, "{}", v),
			Literal::Number(v) => write!(f, "{}", v),
			Literal::Strand(v) => write!(f, "{}", v),
			Literal::Duration(v) => write!(f, "{}", v),
			Literal::Datetime(v) => write!(f, "{}", v),
			Literal::Function(v) => write!(f, "{}", v),
			Literal::Subquery(v) => write!(f, "{}", v),
		}
	}
}

pub fn literals(i: &str) -> IResult<&str, Literals> {
	let (i, v) = separated_nonempty_list(commas, literal)(i)?;
	Ok((i, Literals(v)))
}

pub fn literal(i: &str) -> IResult<&str, Literal> {
	alt((
		map(tag_no_case("NULL"), |_| Literal::Null),
		map(tag_no_case("true"), |_| Literal::Bool(true)),
		map(tag_no_case("false"), |_| Literal::Bool(false)),
		map(subquery, |v| Literal::Subquery(v)),
		map(function, |v| Literal::Function(v)),
		map(datetime, |v| Literal::Datetime(v)),
		map(duration, |v| Literal::Duration(v)),
		map(number, |v| Literal::Number(v)),
		map(strand, |v| Literal::Strand(v)),
		map(object, |v| Literal::Object(v)),
		map(array, |v| Literal::Array(v)),
		map(param, |v| Literal::Param(v)),
		map(thing, |v| Literal::Thing(v)),
		map(model, |v| Literal::Model(v)),
		map(idiom, |v| Literal::Idiom(v)),
	))(i)
}

pub fn simple(i: &str) -> IResult<&str, Literal> {
	alt((
		map(number, |v| Literal::Number(v)),
		map(strand, |v| Literal::Strand(v)),
		map(object, |v| Literal::Object(v)),
		map(array, |v| Literal::Array(v)),
	))(i)
}
