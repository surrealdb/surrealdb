use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc;
use crate::sql::operator::Operator;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

use super::operator::BindingPower;
use super::ControlFlow;
use super::FlowResult;

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

crate::sql::impl_display_from_sql!(Expression);

impl crate::sql::DisplaySql for Expression {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
