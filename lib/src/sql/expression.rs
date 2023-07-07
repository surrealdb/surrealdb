use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc;
use crate::sql::comment::mightbespace;
use crate::sql::error::IResult;
use crate::sql::operator::{self, Operator};
use crate::sql::value::{single, value, Value};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Expression";

/// Binary expressions.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Expression")]
pub enum Expression {
	Unary {
		o: Operator,
		v: Value,
	},
	Binary {
		l: Value,
		o: Operator,
		r: Value,
	},
}

impl Default for Expression {
	fn default() -> Expression {
		Expression::Binary {
			l: Value::Null,
			o: Operator::default(),
			r: Value::Null,
		}
	}
}

impl Expression {
	/// Create a new binary expression
	pub(crate) fn new(l: Value, o: Operator, r: Value) -> Self {
		Self::Binary {
			l,
			o,
			r,
		}
	}
	/// Augment an existing expression
	fn augment(mut self, l: Value, o: Operator) -> Self {
		match &mut self {
			Self::Binary {
				l: left,
				o: op,
				..
			} if o.precedence() >= op.precedence() => match left {
				Value::Expression(x) => {
					*x.as_mut() = std::mem::take(x).augment(l, o);
					self
				}
				_ => {
					*left = Self::new(l, o, std::mem::take(left)).into();
					self
				}
			},
			e => {
				let r = Value::from(std::mem::take(e));
				Self::new(l, o, r)
			}
		}
	}
}

impl Expression {
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Self::Unary {
				v,
				..
			} => v.writeable(),
			Self::Binary {
				l,
				r,
				..
			} => l.writeable() || r.writeable(),
		}
	}

	/// Returns the operator
	pub(crate) fn operator(&self) -> &Operator {
		match self {
			Expression::Unary {
				o,
				..
			} => o,
			Expression::Binary {
				o,
				..
			} => o,
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		let (l, o, r) = match self {
			Self::Unary {
				o,
				v,
			} => {
				let operand = v.compute(ctx, opt, txn, doc).await?;
				return match o {
					Operator::Neg => fnc::operate::neg(operand),
					Operator::Not => fnc::operate::not(operand),
					op => unreachable!("{op:?} is not a unary op"),
				};
			}
			Self::Binary {
				l,
				o,
				r,
			} => (l, o, r),
		};

		let l = l.compute(ctx, opt, txn, doc).await?;
		match o {
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
			Operator::Tco => {
				if let true = l.is_truthy() {
					return Ok(l);
				}
			}
			Operator::Nco => {
				if let true = l.is_some() {
					return Ok(l);
				}
			}
			_ => {} // Continue
		}
		let r = r.compute(ctx, opt, txn, doc).await?;
		match o {
			Operator::Or => fnc::operate::or(l, r),
			Operator::And => fnc::operate::and(l, r),
			Operator::Tco => fnc::operate::tco(l, r),
			Operator::Nco => fnc::operate::nco(l, r),
			Operator::Add => fnc::operate::add(l, r),
			Operator::Sub => fnc::operate::sub(l, r),
			Operator::Mul => fnc::operate::mul(l, r),
			Operator::Div => fnc::operate::div(l, r),
			Operator::Pow => fnc::operate::pow(l, r),
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
			Operator::Matches(_) => fnc::operate::matches(ctx, txn, doc, self).await,
			_ => unreachable!(),
		}
	}
}

impl fmt::Display for Expression {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Unary {
				o,
				v,
			} => write!(f, "{o}{v}"),
			Self::Binary {
				l,
				o,
				r,
			} => write!(f, "{l} {o} {r}"),
		}
	}
}

pub fn unary(i: &str) -> IResult<&str, Expression> {
	let (i, o) = operator::unary(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = single(i)?;
	Ok((
		i,
		Expression::Unary {
			o,
			v,
		},
	))
}

pub fn binary(i: &str) -> IResult<&str, Expression> {
	let (i, l) = single(i)?;
	let (i, o) = operator::binary(i)?;
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
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("true AND false", format!("{}", out));
	}

	#[test]
	fn expression_left_opened() {
		let sql = "3 * 3 * 3 = 27";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("3 * 3 * 3 = 27", format!("{}", out));
	}

	#[test]
	fn expression_left_closed() {
		let sql = "(3 * 3 * 3) = 27";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(3 * 3 * 3) = 27", format!("{}", out));
	}

	#[test]
	fn expression_right_opened() {
		let sql = "27 = 3 * 3 * 3";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("27 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_right_closed() {
		let sql = "27 = (3 * 3 * 3)";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("27 = (3 * 3 * 3)", format!("{}", out));
	}

	#[test]
	fn expression_both_opened() {
		let sql = "3 * 3 * 3 = 3 * 3 * 3";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_both_closed() {
		let sql = "(3 * 3 * 3) = (3 * 3 * 3)";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(3 * 3 * 3) = (3 * 3 * 3)", format!("{}", out));
	}

	#[test]
	fn expression_unary() {
		let sql = "-a";
		let res = unary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn expression_with_unary() {
		let sql = "-(5) + 5";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}
}
