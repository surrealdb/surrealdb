use common::fmt::{EscapeIdent, EscapeKwFreeIdent, Fmt};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr, Idiom, Model, Script};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Function {
	Normal(String),
	Custom(String),
	Script(Script),
	Model(Model),
	Module(String, Option<String>, ExtPrefix),
	Silo {
		org: String,
		pkg: String,
		major: u32,
		minor: u32,
		patch: u32,
		sub: Option<String>,
	},
}

/// The prefix an extension call is written with. Both name the same
/// extension and run the same export; the prefix is what the call renders
/// with and what its implicit field name starts with, so a projection keeps
/// the key it was written to produce.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum ExtPrefix {
	/// `ext::`.
	#[default]
	Ext,
	/// `mod::`, the deprecated alias.
	Mod,
}

impl ExtPrefix {
	/// The prefix as written, `ext` or `mod`.
	pub fn as_str(self) -> &'static str {
		match self {
			ExtPrefix::Ext => "ext",
			ExtPrefix::Mod => "mod",
		}
	}
}

impl Function {
	/// Convert function call to a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			// Safety: "function" does not contain null bytes"
			Self::Script(_) => Idiom::field("function".to_owned()),
			Self::Normal(f) => Idiom::field(f.to_owned()),
			Self::Custom(f) => Idiom::field(format!("fn::{f}")),
			Self::Model(m) => Idiom::field(m.to_sql()),
			Self::Module(m, s, p) => match s {
				Some(s) => Idiom::field(format!("{}::{m}::{s}", p.as_str())),
				None => Idiom::field(format!("{}::{m}", p.as_str())),
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

	/// Checks if this function invocation is writable
	pub fn read_only(&self) -> bool {
		match self {
			Self::Custom(_)
			| Self::Script(_)
			| Self::Module(..)
			| Self::Silo {
				..
			} => false,
			Self::Normal(f) => !crate::expr::method::is_writer_builtin(f),
			Self::Model(_) => true,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FunctionCall {
	pub receiver: Function,
	pub arguments: Vec<Expr>,
}

impl FunctionCall {
	/// Returns if this expression type object can do any writes.
	pub fn read_only(&self) -> bool {
		self.receiver.read_only() && self.arguments.iter().all(|x| x.read_only())
	}
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
			Function::Module(ref m, ref s, p) => {
				write_sql!(f, fmt, "{}::{}", p.as_str(), EscapeKwFreeIdent(m));
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
