use crate::ctx::Parent;
use crate::dbs;
use crate::dbs::Executor;
use crate::doc::Document;
use crate::err::Error;
use crate::fnc;
use crate::sql::comment::mightbespace;
use crate::sql::literal::{literal, Literal};
use crate::sql::operator::{operator, Operator};
use nom::branch::alt;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Expression {
	Single(Box<Literal>),
	Binary(Box<Literal>, Operator, Box<Expression>),
}

impl Default for Expression {
	fn default() -> Expression {
		Expression::Single(Box::new(Literal::Null))
	}
}

impl<'a> From<&'a str> for Expression {
	fn from(s: &str) -> Self {
		expression(s).unwrap().1
	}
}

impl From<Literal> for Expression {
	fn from(v: Literal) -> Self {
		Expression::Single(Box::new(v))
	}
}

impl fmt::Display for Expression {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Expression::Single(ref l) => write!(f, "{}", l),
			Expression::Binary(ref l, ref o, ref r) => write!(f, "{} {} {}", l, o, r),
		}
	}
}

impl dbs::Process for Expression {
	fn process(
		&self,
		ctx: &Parent,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		match self {
			Expression::Single(ref l) => l.process(ctx, exe, doc),
			Expression::Binary(ref l, ref o, ref r) => {
				let l = l.process(ctx, exe, doc)?;
				match o {
					Operator::Or => match l.as_bool() {
						true => return Ok(l), // No need to continue
						_ => {}               // Continue
					},
					Operator::And => match l.as_bool() {
						false => return Ok(l), // No need to continue
						_ => {}                // Continue
					},
					_ => {} // Continue
				}
				let r = r.process(ctx, exe, doc)?;
				match o {
					Operator::Or => fnc::operate::or(l, r),
					Operator::And => fnc::operate::and(l, r),
					Operator::Add => fnc::operate::add(&l, &r),
					Operator::Sub => fnc::operate::sub(&l, &r),
					Operator::Mul => fnc::operate::mul(&l, &r),
					Operator::Div => fnc::operate::div(&l, &r),
					Operator::Equal => fnc::operate::equal(&l, &r),
					Operator::NotEqual => fnc::operate::not_equal(&l, &r),
					Operator::AllEqual => fnc::operate::all_equal(&l, &r),
					Operator::AnyEqual => fnc::operate::any_equal(&l, &r),
					Operator::Like => fnc::operate::like(&l, &r),
					Operator::NotLike => fnc::operate::not_like(&l, &r),
					Operator::AllLike => fnc::operate::all_like(&l, &r),
					Operator::AnyLike => fnc::operate::any_like(&l, &r),
					Operator::LessThan => fnc::operate::less_than(&l, &r),
					Operator::LessThanOrEqual => fnc::operate::less_than_or_equal(&l, &r),
					Operator::MoreThan => fnc::operate::more_than(&l, &r),
					Operator::MoreThanOrEqual => fnc::operate::more_than_or_equal(&l, &r),
					Operator::Contain => fnc::operate::contain(&l, &r),
					Operator::NotContain => fnc::operate::not_contain(&l, &r),
					Operator::ContainAll => fnc::operate::contain_all(&l, &r),
					Operator::ContainSome => fnc::operate::contain_some(&l, &r),
					Operator::ContainNone => fnc::operate::contain_none(&l, &r),
					Operator::Inside => fnc::operate::inside(&l, &r),
					Operator::NotInside => fnc::operate::not_inside(&l, &r),
					Operator::AllInside => fnc::operate::inside_all(&l, &r),
					Operator::SomeInside => fnc::operate::inside_some(&l, &r),
					Operator::NoneInside => fnc::operate::inside_none(&l, &r),
					Operator::Intersects => fnc::operate::intersects(&l, &r),
					_ => unreachable!(),
				}
			}
		}
	}
}

pub fn expression(i: &str) -> IResult<&str, Expression> {
	alt((binary, single))(i)
}

fn binary(i: &str) -> IResult<&str, Expression> {
	let (i, l) = literal(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, o) = operator(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, r) = expression(i)?;
	Ok((i, Expression::Binary(Box::new(l), o, Box::new(r))))
}

fn single(i: &str) -> IResult<&str, Expression> {
	let (i, l) = literal(i)?;
	Ok((i, Expression::Single(Box::new(l))))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn expression_single() {
		let sql = "true";
		let res = expression(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("true", format!("{}", out));
	}

	#[test]
	fn expression_double() {
		let sql = "true AND false";
		let res = expression(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("true AND false", format!("{}", out));
	}

	#[test]
	fn expression_left_opened() {
		let sql = "3 * 3 * 3 = 27";
		let res = expression(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("3 * 3 * 3 = 27", format!("{}", out));
	}

	#[test]
	fn expression_left_closed() {
		let sql = "(3 * 3 * 3) = 27";
		let res = expression(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(3 * 3 * 3) = 27", format!("{}", out));
	}

	#[test]
	fn expression_both_opened() {
		let sql = "3 * 3 * 3 = 3 * 3 * 3";
		let res = expression(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_both_closed() {
		let sql = "(3 * 3 * 3) = (3 * 3 * 3)";
		let res = expression(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(3 * 3 * 3) = (3 * 3 * 3)", format!("{}", out));
	}
}
