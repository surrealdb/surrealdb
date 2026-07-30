//! `sql` -> `expr` conversions for [`crate::sql::function`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::function::*;

impl From<Function> for crate::expr::Function {
	fn from(v: Function) -> Self {
		match v {
			Function::Normal(s) => crate::expr::Function::Normal(s),
			Function::Custom(s) => crate::expr::Function::Custom(s),
			Function::Script(s) => crate::expr::Function::Script(s.into()),
			Function::Model(m) => crate::expr::Function::Model(m.into()),
			Function::Module(m, s) => crate::expr::Function::Module(m, s),
			Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => crate::expr::Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			},
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
			crate::expr::Function::Module(m, s) => Self::Module(m, s),
			crate::expr::Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => Self::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			},
		}
	}
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
