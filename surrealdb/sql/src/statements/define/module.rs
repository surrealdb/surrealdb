use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::{CoverStmts, Expr, Ident, Literal, ModuleExecutable, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineModuleStatement {
	pub kind: DefineKind,
	pub name: Option<Ident>,
	pub executable: ModuleExecutable,
	/// Whether the module is loaded without verifying a signature.
	///
	/// Spelled as a trailing `UNSIGNED` clause, unordered with `COMMENT` and
	/// `PERMISSIONS`. Signature verification is not yet implemented for any
	/// executable form, so every module currently has to opt out of it
	/// explicitly and this is always `true` for a freshly parsed statement.
	/// `false` is therefore not expressible in the grammar and would not
	/// survive a render/reparse round trip, which is why the fuzzer is pinned
	/// to `true` rather than left to generate either.
	#[cfg_attr(feature = "arbitrary", arbitrary(value = true))]
	pub unsigned: bool,
	pub comment: Expr,
	pub permissions: Permission,
}

impl ToSql for DefineModuleStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("DEFINE MODULE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}
		if let Some(name) = &self.name {
			write_sql!(f, sql_fmt, " mod::{} FROM", name);
		}
		write_sql!(f, sql_fmt, " {}", self.executable);
		if self.unsigned {
			f.push_str(" UNSIGNED");
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		if sql_fmt.is_pretty() {
			f.push('\n');
			sql_fmt.write_indent(f);
		} else {
			f.push(' ');
		}
		write_sql!(f, sql_fmt, "PERMISSIONS {}", self.permissions);
	}
}
