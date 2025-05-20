use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::operator::Operator;
use crate::sql::value::SqlValue;
use crate::fnc;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

use super::ControlFlow;
use super::FlowResult;
use super::operator::BindingPower;

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
		v: SqlValue,
	},
	Binary {
		l: SqlValue,
		o: Operator,
		r: SqlValue,
	},
}

impl Default for Expression {
	fn default() -> Expression {
		Expression::Binary {
			l: SqlValue::Null,
			o: Operator::default(),
			r: SqlValue::Null,
		}
	}
}

impl Expression {
	/// Create a new binary expression
	pub fn new(l: SqlValue, o: Operator, r: SqlValue) -> Self {
		Self::Binary {
			l,
			o,
			r,
		}
	}
}

impl Expression {
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

	}

impl fmt::Display for Expression {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Unary {
				o,
				v,
			} => {
				if BindingPower::for_value(v) < BindingPower::Unary {
					write!(f, "{o}({v})")
				} else {
					write!(f, "{o}{v}")
				}
			}
			Self::Binary {
				l,
				o,
				r,
			} => {
				let op_bp = BindingPower::for_operator(o);
				if BindingPower::for_value(l) < op_bp {
					write!(f, "({l})")?;
				} else {
					write!(f, "{l}")?;
				}
				write!(f, " {o} ")?;
				if BindingPower::for_value(r) < op_bp {
					write!(f, "({r})")?;
				} else {
					write!(f, "{r}")?;
				}
				Ok(())
			}
		}
	}
}

impl From<Expression> for crate::expr::Expression {
	fn from(v: Expression) -> Self {
		match v {
			Expression::Unary {
				o,
				v,
			} => Self::Unary {
				o: o.into(),
				v: v.into(),
			},
			Expression::Binary {
				l,
				o,
				r,
			} => Self::Binary {
				l: l.into(),
				o: o.into(),
				r: r.into(),
			},
		}
	}
}

impl From<crate::expr::Expression> for Expression {
	fn from(v: crate::expr::Expression) -> Self {
		match v {
			crate::expr::Expression::Unary {
				o,
				v,
			} => Self::Unary {
				o: o.into(),
				v: v.into(),
			},
			crate::expr::Expression::Binary {
				l,
				o,
				r,
			} => Self::Binary {
				l: l.into(),
				o: o.into(),
				r: r.into(),
			},
		}
	}
}