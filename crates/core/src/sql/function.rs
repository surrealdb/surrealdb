use std::fmt;

use crate::sql::fmt::Fmt;
use crate::sql::{Expr, Ident, Idiom, Model, Script};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Function {
	Normal(String),
	Custom(String),
	Script(Script),
	Model(Model),
}

impl Function {
	pub fn to_idiom(&self) -> Idiom {
		match self {
			// Safety: "function" does not contain null bytes"
			Self::Script(_) => Idiom::field(unsafe { Ident::new_unchecked("function".to_owned()) }),
			Self::Normal(f) => Idiom::field(unsafe { Ident::new_unchecked(f.to_owned()) }),
			Self::Custom(f) => Idiom::field(unsafe { Ident::new_unchecked(format!("fn::{f}")) }),
			Self::Model(m) => Idiom::field(unsafe { Ident::new_unchecked(m.to_string()) }),
		}
	}
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
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FunctionCall {
	pub receiver: Function,
	pub arguments: Vec<Expr>,
}

impl From<FunctionCall> for crate::expr::FunctionCall {
	fn from(value: FunctionCall) -> Self {
		crate::expr::FunctionCall {
			receiver: value.receiver.into(),
			arguments: value.arguments.into_iter().map(Into::into).collect(),
		}
	}
}

impl From<crate::expr::FunctionCall> for FunctionCall {
	fn from(value: crate::expr::FunctionCall) -> Self {
		FunctionCall {
			receiver: value.receiver.into(),
			arguments: value.arguments.into_iter().map(Into::into).collect(),
		}
	}
}

impl fmt::Display for FunctionCall {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.receiver {
			Function::Normal(ref s) => {
				write!(f, "{s}({})", Fmt::comma_separated(self.arguments.iter()))
			}
			Function::Custom(ref s) => {
				write!(f, "fn::{s}({})", Fmt::comma_separated(self.arguments.iter()))
			}
			Function::Script(ref s) => {
				write!(f, "function({}) {{{s}}}", Fmt::comma_separated(self.arguments.iter()))
			}
			Function::Model(ref m) => {
				write!(f, "{m}({})", Fmt::comma_separated(self.arguments.iter()))
			}
		}
	}
}
