use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{EscapeKwFreeIdent, Fmt};
use crate::sql::{Expr, Idiom, Model, Script};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Function {
	Normal(String),
	Custom(String),
	Script(Script),
	Model(Model),
	Module(String, Option<String>),
	Silo {
		org: String,
		pkg: String,
		major: u32,
		minor: u32,
		patch: u32,
		sub: Option<String>,
	},
}

impl Function {
	pub(crate) fn to_idiom(&self) -> Idiom {
		match self {
			// Safety: "function" does not contain null bytes"
			Self::Script(_) => Idiom::field("function".to_owned()),
			Self::Normal(f) => Idiom::field(f.to_owned()),
			Self::Custom(name) => Idiom::field(format!("fn::{name}")),
			Self::Model(m) => Idiom::field(m.to_sql()),
			Self::Module(m, s) => match s {
				Some(s) => Idiom::field(format!("mod::{m}::{s}")),
				None => Idiom::field(format!("mod::{m}")),
			},
			Self::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => match sub {
				Some(s) => {
					Idiom::field(format!("silo::{org}::{pkg}<{major}.{minor}.{patch}>::{s}"))
				}
				None => Idiom::field(format!("silo::{org}::{pkg}<{major}.{minor}.{patch}>")),
			},
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

impl ToSql for FunctionCall {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.receiver {
			Function::Normal(ref s) => {
				for (idx, s) in s.split("::").enumerate() {
					if idx != 0 {
						f.push_str("::");
					}
					f.push_str(s);
				}

				write_sql!(f, fmt, "({})", Fmt::comma_separated(self.arguments.iter()));
			}
			Function::Custom(ref s) => {
				f.push_str("fn");
				for s in s.split("::") {
					f.push_str("::");
					write_sql!(f, fmt, "{}", EscapeKwFreeIdent(s));
				}
				write_sql!(f, fmt, "({})", Fmt::comma_separated(self.arguments.iter()));
			}
			Function::Script(ref s) => {
				write_sql!(
					f,
					fmt,
					"function({}) {{{s}}}",
					Fmt::comma_separated(self.arguments.iter())
				);
			}
			Function::Model(ref m) => {
				write_sql!(f, fmt, "{m}({})", Fmt::comma_separated(self.arguments.iter()));
			}
			Function::Module(ref m, ref s) => {
				f.push_str("mod");
				for s in m.split("::") {
					f.push_str("::");
					write_sql!(f, fmt, "{}", EscapeKwFreeIdent(s));
				}
				if let Some(s) = s {
					write_sql!(f, fmt, "::{}", EscapeKwFreeIdent(s));
				}
				write_sql!(f, fmt, "({})", Fmt::comma_separated(self.arguments.iter()));
			}
			Function::Silo {
				ref org,
				ref pkg,
				ref major,
				ref minor,
				ref patch,
				ref sub,
			} => match sub {
				Some(s) => write_sql!(
					f,
					fmt,
					"silo::{}::{}<{major}.{minor}.{patch}>::{}({})",
					EscapeKwFreeIdent(org),
					EscapeKwFreeIdent(pkg),
					EscapeKwFreeIdent(s),
					Fmt::comma_separated(self.arguments.iter())
				),
				None => write_sql!(
					f,
					fmt,
					"silo::{}::{}<{major}.{minor}.{patch}>({})",
					EscapeKwFreeIdent(org),
					EscapeKwFreeIdent(pkg),
					Fmt::comma_separated(self.arguments.iter())
				),
			},
		}
	}
}
