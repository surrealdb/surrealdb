use crate::sql::comment::mightbespace;
use crate::sql::definition::{definition, Definition};
use crate::sql::literal::Literal;
use crate::sql::operator::{operator, Operator};
use nom::branch::alt;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Expression {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub lhs: Option<Box<Definition>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub op: Option<Operator>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub rhs: Option<Box<Expression>>,
}

impl<'a> From<&'a str> for Expression {
	fn from(s: &str) -> Self {
		expression(s).unwrap().1
	}
}

impl From<Literal> for Expression {
	fn from(v: Literal) -> Self {
		Expression {
			lhs: Some(Box::new(Definition::from(v))),
			op: None,
			rhs: None,
		}
	}
}

impl fmt::Display for Expression {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if let Some(ref lhs) = self.lhs {
			write!(f, "{}", lhs)?
		}
		if let Some(ref op) = self.op {
			write!(f, " {} ", op)?
		}
		if let Some(ref rhs) = self.rhs {
			write!(f, "{}", rhs)?
		}
		Ok(())
	}
}

pub fn expression(i: &str) -> IResult<&str, Expression> {
	alt((dual, lone))(i)
}

fn dual(i: &str) -> IResult<&str, Expression> {
	let (i, l) = definition(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, o) = operator(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, r) = expression(i)?;
	Ok((
		i,
		Expression {
			lhs: Some(Box::new(l)),
			op: Some(o),
			rhs: Some(Box::new(r)),
		},
	))
}

fn lone(i: &str) -> IResult<&str, Expression> {
	let (i, l) = definition(i)?;
	Ok((
		i,
		Expression {
			lhs: Some(Box::new(l)),
			op: None,
			rhs: None,
		},
	))
}
