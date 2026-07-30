use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, Idiom, Model, Script};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
	/// Convert function call to a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			// Safety: "function" does not contain null bytes"
			Self::Script(_) => Idiom::field("function".to_owned()),
			Self::Normal(f) => Idiom::field(f.to_owned()),
			Self::Custom(f) => Idiom::field(format!("fn::{f}")),
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

	/// Checks if this function invocation is writable
	pub fn read_only(&self) -> bool {
		match self {
			Self::Custom(_)
			| Self::Script(_)
			| Self::Module(_, _)
			| Self::Silo {
				..
			} => false,
			// `eval::*` can evaluate arbitrary nested queries (including writes),
			// so they must open a write transaction like `api::invoke`.
			Self::Normal(f) => f != "api::invoke" && f != "eval::surql" && f != "eval::gql",
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
		let fnc: crate::sql::FunctionCall = self.clone().into();
		fnc.fmt_sql(f, fmt);
	}
}
