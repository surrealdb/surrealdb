use crate::sql::comment::shouldbespace;
use crate::sql::expression::{expression, Expression};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::multi::many0;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IfelseStatement {
	pub first: (Expression, Expression),
	pub other: Vec<(Expression, Expression)>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub close: Option<Expression>,
}

impl fmt::Display for IfelseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "IF {} THEN {}", self.first.0, self.first.1)?;
		for ref o in self.other.iter() {
			write!(f, " ELSE IF {} THEN {}", o.0, o.1)?
		}
		if let Some(ref v) = self.close {
			write!(f, " ELSE {}", v)?
		}
		write!(f, " END")?;
		Ok(())
	}
}

pub fn ifelse(i: &str) -> IResult<&str, IfelseStatement> {
	let (i, _) = tag_no_case("IF")(i)?;
	let (i, first) = first(i)?;
	let (i, other) = many0(other)(i)?;
	let (i, close) = opt(close)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("END")(i)?;
	Ok((
		i,
		IfelseStatement {
			first,
			other,
			close,
		},
	))
}

fn first(i: &str) -> IResult<&str, (Expression, Expression)> {
	let (i, _) = shouldbespace(i)?;
	let (i, cond) = expression(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("THEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, then) = expression(i)?;
	Ok((i, (cond, then)))
}

fn other(i: &str) -> IResult<&str, (Expression, Expression)> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ELSE IF")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, cond) = expression(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("THEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, then) = expression(i)?;
	Ok((i, (cond, then)))
}

fn close(i: &str) -> IResult<&str, Expression> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ELSE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, then) = expression(i)?;
	Ok((i, then))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn ifelse_statement_first() {
		let sql = "IF this THEN that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_statement_close() {
		let sql = "IF this THEN that ELSE that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_statement_other() {
		let sql = "IF this THEN that ELSE IF this THEN that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_statement_other_close() {
		let sql = "IF this THEN that ELSE IF this THEN that ELSE that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
