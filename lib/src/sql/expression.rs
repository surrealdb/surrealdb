use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::fnc;
use crate::sql::error::IResult;
use crate::sql::operator::{operator, Operator};
use crate::sql::value::{single, value, Value};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Expression {
	pub l: Value,
	pub o: Operator,
	pub r: Value,
}

impl Default for Expression {
	fn default() -> Expression {
		Expression {
			l: Value::Null,
			o: Operator::default(),
			r: Value::Null,
		}
	}
}

impl Expression {
	// Create a new expression
	fn new(l: Value, o: Operator, r: Value) -> Self {
		Self {
			l,
			o,
			r,
		}
	}
	// Augment an existing expression
	fn augment(mut self, l: Value, o: Operator) -> Self {
		if o.precedence() >= self.o.precedence() {
			match self.l {
				Value::Expression(x) => {
					self.l = x.augment(l, o).into();
					self
				}
				_ => {
					self.l = Self::new(l, o, self.l).into();
					self
				}
			}
		} else {
			let r = Value::from(self);
			Self::new(l, o, r)
		}
	}
}

impl Expression {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		let l = self.l.compute(ctx, opt, txn, doc).await?;
		match self.o {
			Operator::Or => {
				if let true = l.is_truthy() {
					return Ok(l);
				}
			}
			Operator::And => {
				if let false = l.is_truthy() {
					return Ok(l);
				}
			}
			_ => {} // Continue
		}
		let r = self.r.compute(ctx, opt, txn, doc).await?;
		match self.o {
			Operator::Or => fnc::operate::or(l, r),
			Operator::And => fnc::operate::and(l, r),
			Operator::Add => fnc::operate::add(l, r),
			Operator::Sub => fnc::operate::sub(l, r),
			Operator::Mul => fnc::operate::mul(l, r),
			Operator::Div => fnc::operate::div(l, r),
			Operator::Equal => fnc::operate::equal(&l, &r),
			Operator::Exact => fnc::operate::exact(&l, &r),
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
			Operator::ContainAny => fnc::operate::contain_any(&l, &r),
			Operator::ContainNone => fnc::operate::contain_none(&l, &r),
			Operator::Inside => fnc::operate::inside(&l, &r),
			Operator::NotInside => fnc::operate::not_inside(&l, &r),
			Operator::AllInside => fnc::operate::inside_all(&l, &r),
			Operator::AnyInside => fnc::operate::inside_any(&l, &r),
			Operator::NoneInside => fnc::operate::inside_none(&l, &r),
			Operator::Outside => fnc::operate::outside(&l, &r),
			Operator::Intersects => fnc::operate::intersects(&l, &r),
			_ => unreachable!(),
		}
	}
}

impl fmt::Display for Expression {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} {}", self.l, self.o, self.r)
	}
}

pub fn expression(i: &str) -> IResult<&str, Expression> {
	let (i, l) = single(i)?;
	let (i, o) = operator(i)?;
	let (i, r) = value(i)?;
	let v = match r {
		Value::Expression(r) => r.augment(l, o),
		_ => Expression::new(l, o, r),
	};
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn expression_statement() {
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
	fn expression_right_opened() {
		let sql = "27 = 3 * 3 * 3";
		let res = expression(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("27 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_right_closed() {
		let sql = "27 = (3 * 3 * 3)";
		let res = expression(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("27 = (3 * 3 * 3)", format!("{}", out));
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
