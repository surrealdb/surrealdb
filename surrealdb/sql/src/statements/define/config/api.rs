use common::fmt::{EscapeKwFreeIdent, Fmt};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{CoverStmts, Expr, Permission};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiConfig {
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
}

impl ToSql for ApiConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if !self.middleware.is_empty() {
			write_sql!(f, fmt, " MIDDLEWARE ");

			for (idx, m) in self.middleware.iter().enumerate() {
				if idx != 0 {
					f.push_str(", ");
				}
				for (idx, s) in m.name.as_str().split("::").enumerate() {
					if idx != 0 {
						f.push_str("::");
					}
					EscapeKwFreeIdent(s).fmt_sql(f, fmt);
				}
				write_sql!(
					f,
					fmt,
					"({})",
					Fmt::pretty_comma_separated(m.args.iter().map(CoverStmts))
				);
			}
		}

		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Middleware {
	pub name: Strand,
	pub args: Vec<Expr>,
}
