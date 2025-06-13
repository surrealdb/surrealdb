use crate::sql::fmt::Fmt;
use crate::sql::{Expr, Idiom, Script, operator::BindingPower};
use anyhow::Result;
use std::cmp::Ordering;
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Function";

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Function {
	Normal(String, Vec<Expr>),
	Custom(String, Vec<Expr>),
	Script(Script, Vec<Expr>),
	/// Fields are: the function object itself, it's arguments and whether the arguments are calculated.
	#[revision(start = 2)]
	Anonymous(Expr, Vec<Expr>, bool),
}

impl From<Function> for crate::expr::Function {
	fn from(v: Function) -> Self {
		match v {
			Function::Normal(s, e) => Self::Normal(s, e.into_iter().map(Into::into).collect()),
			Function::Custom(s, e) => Self::Custom(s, e.into_iter().map(Into::into).collect()),
			Function::Script(s, e) => {
				Self::Script(s.into(), e.into_iter().map(Into::into).collect())
			}
			Function::Anonymous(p, e, b) => {
				Self::Anonymous(p.into(), e.into_iter().map(Into::into).collect(), b)
			}
		}
	}
}

impl From<crate::expr::Function> for Function {
	fn from(v: crate::expr::Function) -> Self {
		match v {
			crate::expr::Function::Normal(s, e) => {
				Self::Normal(s, e.into_iter().map(Into::into).collect())
			}
			crate::expr::Function::Custom(s, e) => {
				Self::Custom(s, e.into_iter().map(Into::into).collect())
			}
			crate::expr::Function::Script(s, e) => {
				Self::Script(s.into(), e.into_iter().map(Into::into).collect())
			}
			crate::expr::Function::Anonymous(p, e, b) => {
				Self::Anonymous(p.into(), e.into_iter().map(Into::into).collect(), b)
			}
		}
	}
}

impl fmt::Display for Function {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Normal(s, e) => write!(f, "{s}({})", Fmt::comma_separated(e)),
			Self::Custom(s, e) => write!(f, "fn::{s}({})", Fmt::comma_separated(e)),
			Self::Script(s, e) => write!(f, "function({}) {{{s}}}", Fmt::comma_separated(e)),
			Self::Anonymous(p, e, _) => {
				if BindingPower::for_value(p) < BindingPower::Postfix {
					write!(f, "({p})")?;
				} else {
					write!(f, "{p}")?;
				}
				write!(f, "({})", Fmt::comma_separated(e))
			}
		}
	}
}
