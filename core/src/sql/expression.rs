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
	#[doc(hidden)]
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

	/// Partially compute the expression, evaluating parameters
	pub(crate) async fn partially_compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self {
			Self::Unary {
				o,
				v,
			} => Ok(Value::Expression(Box::new(Self::Unary {
				o: o.to_owned(),
				v: v.partially_compute(stk, ctx, opt, doc).await?,
			}))),
			Self::Binary {
				l,
				o,
				r,
			} => Ok(Value::Expression(Box::new(Self::Binary {
				l: l.partially_compute(stk, ctx, opt, doc).await?,
				o: o.to_owned(),
				r: r.partially_compute(stk, ctx, opt, doc).await?,
			}))),
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		let (l, o, r) = match self {
			Self::Unary {
				o,
				v,
			} => {
				let operand = v.compute(stk, ctx, opt, doc).await?;
				return match o {
					Operator::Neg => fnc::operate::neg(operand),
					// TODO: Check if it is a number?
					Operator::Add => Ok(operand),
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
			Operator::Matches(_) => fnc::operate::matches(stk, ctx, opt, doc, self, l, r).await,
			Operator::Knn(_, _) | Operator::Ann(_, _) => {
				fnc::operate::knn(stk, ctx, opt, doc, self).await
			}
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

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod test {
	use crate::ctx::Context;
	use crate::sql::{Expression, Operator, Value};
	use reblessive::TreeStack;

	#[tokio::test]
	async fn param_evaluated_partial_compute() {
		let expression = Expression::Binary {
			l: Value::Param("foo".into()),
			o: Operator::Equal,
			r: Value::Param("bar".into()),
		};
		let mut ctx = Context::default();
		ctx.add_value("foo", Value::Number(1.0.into()));
		ctx.add_value("bar", Value::Number(2.0.into()));
		let mut stack = TreeStack::new();
		let expression = stack
			.enter(|stk| async {
				expression.partially_compute(stk, &ctx, &Default::default(), None).await.unwrap()
			})
			.finish()
			.await;
		assert_eq!(
			expression,
			Value::Expression(Box::new(Expression::Binary {
				l: Value::Number(1.0.into()),
				o: Operator::Equal,
				r: Value::Number(2.0.into()),
			}))
		);
	}
}
