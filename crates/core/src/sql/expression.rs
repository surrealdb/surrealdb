use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc;
use crate::sql::operator::Operator;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Expression";

/// Binary expressions.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Expression")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	pub fn new(l: Value, o: Operator, r: Value) -> Self {
		Self::Binary {
			l,
			o,
			r,
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

	/// Checks whether all expression parts are static values
	pub(crate) fn is_static(&self) -> bool {
		match self {
			Self::Unary {
				v,
				..
			} => v.is_static(),
			Self::Binary {
				l,
				r,
				..
			} => l.is_static() && r.is_static(),
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
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Check the type of expression
		match self {
			// This is a unary expression: !test
			Self::Unary {
				o,
				v,
			} => match o {
				Operator::Add => Ok(v.compute(stk, ctx, opt, doc).await?),
				Operator::Neg => fnc::operate::neg(v.compute(stk, ctx, opt, doc).await?),
				Operator::Not => fnc::operate::not(v.compute(stk, ctx, opt, doc).await?),
				o => Err(fail!("Invalid operator '{o:?}' encountered")),
			},
			// This is a binary expression: test != NONE
			Self::Binary {
				l,
				o,
				r,
			} => {
				let l = l.compute(stk, ctx, opt, doc).await?;
				match o {
					Operator::Or => {
						if l.is_truthy() {
							return Ok(l);
						}
					}
					Operator::And => {
						if !l.is_truthy() {
							return Ok(l);
						}
					}
					Operator::Tco => {
						if l.is_truthy() {
							return Ok(l);
						}
					}
					Operator::Nco => {
						if l.is_some() {
							return Ok(l);
						}
					}
					_ => {} // Continue
				}
				let r = r.compute(stk, ctx, opt, doc).await?;
				match o {
					Operator::Or => fnc::operate::or(l, r),
					Operator::And => fnc::operate::and(l, r),
					Operator::Tco => fnc::operate::tco(l, r),
					Operator::Nco => fnc::operate::nco(l, r),
					Operator::Add => fnc::operate::add(l, r),
					Operator::Sub => fnc::operate::sub(l, r),
					Operator::Mul => fnc::operate::mul(l, r),
					Operator::Div => fnc::operate::div(l, r),
					Operator::Rem => fnc::operate::rem(l, r),
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
					Operator::Matches(_) => {
						fnc::operate::matches(stk, ctx, opt, doc, self, l, r).await
					}
					Operator::Knn(_, _) | Operator::Ann(_, _) => {
						fnc::operate::knn(stk, ctx, opt, doc, self).await
					}
					o => Err(fail!("Invalid operator '{o:?}' encountered")),
				}
			}
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
