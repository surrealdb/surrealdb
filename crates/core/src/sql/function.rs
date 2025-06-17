use crate::sql::fmt::Fmt;
use crate::sql::{Expr, Model, Script, operator::BindingPower};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Function";

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Function {
	Normal(String),
	Custom(String),
	Script(Script),
	Model(Model),
}

impl From<Function> for crate::expr::Function {
	fn from(v: Function) -> Self {
		match v {
			Function::Normal(s) => crate::expr::Function::Normal(s),
			Function::Custom(s) => crate::expr::Function::Custom(s),
			Function::Script(s) => crate::expr::Function::Script(s.into()),
			Function::Model(m) => crate::expr::Function::Model(m.into()),
		}
	}
}

impl From<crate::expr::Function> for Function {
	fn from(v: crate::expr::Function) -> Self {
		match v {
			crate::expr::Function::Normal(s) => Self::Normal(s),
			crate::expr::Function::Custom(s) => Self::Custom(s),
			crate::expr::Function::Script(s) => Self::Script(s.into()),
			crate::expr::Function::Model(m) => Self::Model(m.into()),
		}
	}
}

///TODO(3.0): Remove after proper first class function support?
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FunctionCall {
	pub receiver: Function,
	pub arguments: Vec<Expr>,
}

impl From<FunctionCall> for crate::expr::FunctionCall {
	fn from(value: FunctionCall) -> Self {
		crate::expr::FunctionCall {
			receiver: self.receiver.into(),
			arguments: self.arguments.into_iter().map(Into::into).collect(),
		}
	}
}

impl From<crate::expr::FunctionCall> for FunctionCall {
	fn from(value: crate::expr::FunctionCall) -> Self {
		FunctionCall {
			receiver: self.receiver.into(),
			arguments: self.arguments.into_iter().map(Into::into).collect(),
		}
	}
}

impl fmt::Display for FunctionCall {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.receiver {
			Function::Normal(s) => write!(f, "{s}({})", Fmt::comma_separated(self.arguments)),
			Function::Custom(s) => write!(f, "fn::{s}({})", Fmt::comma_separated(self.arguments)),
			Function::Script(s) => {
				write!(f, "function({}) {{{s}}}", Fmt::comma_separated(self.arguments))
			}
			Function::Model(m) => {
				write!(f, "function({}) {{{s}}}", Fmt::comma_separated(self.arguments))
			}
		}
	}
}
