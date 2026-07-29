use common::fmt::{EscapeIdent, EscapeKwFreeIdent, Fmt};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{CoverStmts, Expr, Idiom, Model, Script};

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
	// we explicitely dont want a display implementation but do need to print a function to a string
	#[allow(clippy::inherent_to_string)]
	pub fn to_string(&self) -> String {
		match self {
			// Safety: "function" does not contain null bytes"
			Self::Script(_) => "function".to_owned(),
			Self::Normal(f) => f.to_owned(),
			Self::Custom(name) => format!("fn::{name}"),
			Self::Model(m) => m.to_sql(),
			Self::Module(m, s) => match s {
				Some(s) => format!("mod::{m}::{s}"),
				None => format!("mod::{m}"),
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
					format!("silo::{org}::{pkg}<{major}.{minor}.{patch}>::{s}")
				}
				None => format!("silo::{org}::{pkg}<{major}.{minor}.{patch}>"),
			},
		}
	}

	pub fn to_idiom(&self) -> Idiom {
		Idiom::field(self.to_string())
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FunctionCall {
	pub receiver: Function,
	pub arguments: Vec<Expr>,
}

impl ToSql for FunctionCall {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.receiver {
			Function::Normal(ref s) => {
				for (idx, s) in s.split("::").enumerate() {
					if idx != 0 {
						f.push_str("::");
					} else {
						write_sql!(f, fmt, "{}", EscapeIdent(s));
						continue;
					}
					s.fmt_sql(f, fmt);
				}
			}
			Function::Custom(ref s) => {
				f.push_str("fn");
				for s in s.split("::") {
					f.push_str("::");
					write_sql!(f, fmt, "{}", EscapeKwFreeIdent(s));
				}
			}
			Function::Script(ref s) => {
				write_sql!(
					f,
					fmt,
					"function({}) {{{s}}}",
					Fmt::comma_separated(self.arguments.iter().map(CoverStmts))
				);
				return;
			}
			Function::Model(ref m) => {
				write_sql!(f, fmt, "{m}");
			}
			Function::Module(ref m, ref s) => {
				f.push_str("mod::");
				write_sql!(f, fmt, " {}", EscapeKwFreeIdent(m));
				if let Some(s) = s {
					for segment in s.split("::") {
						write_sql!(f, fmt, "::{}", EscapeKwFreeIdent(segment));
					}
				}
			}
			Function::Silo {
				ref org,
				ref pkg,
				ref major,
				ref minor,
				ref patch,
				ref sub,
			} => {
				write_sql!(
					f,
					fmt,
					"silo::{}::{}<{major}.{minor}.{patch}>",
					EscapeKwFreeIdent(org),
					EscapeKwFreeIdent(pkg),
				);
				if let Some(s) = sub {
					for segment in s.split("::") {
						write_sql!(f, fmt, "::{}", EscapeKwFreeIdent(segment));
					}
				}
			}
		}
		write_sql!(f, fmt, "({})", Fmt::comma_separated(self.arguments.iter().map(CoverStmts)))
	}
}
