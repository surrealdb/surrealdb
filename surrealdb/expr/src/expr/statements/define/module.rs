use common::fmt::EscapeKwFreeIdent;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::permission::Permission;
use crate::expr::{
	CoverStmts, Expr, ExtensionBindings, ExtensionCapabilities, ExtensionSigner, Literal,
	ManifestDigest, ModuleExecutable,
};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineModuleStatement {
	pub kind: DefineKind,
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	/// `DIGEST`. `None` when the statement leaves it out, in which case
	/// executing the statement computes it from the archive.
	pub digest: Option<ManifestDigest>,
	/// `SIGNER`. A statement has at most one of `SIGNER` and `UNSIGNED`.
	pub signer: Option<ExtensionSigner>,
	/// `UNSIGNED`. A definition with neither clause is refused outside an
	/// import; an import stores it as a definition to reinstall.
	pub unsigned: bool,
	/// `CAPABILITIES`; an absent clause parses as the default, which grants
	/// nothing. `None` only renders a definition stored before the clause
	/// existed, which must be reinstalled.
	pub capabilities: Option<ExtensionCapabilities>,
	/// `BINDINGS`; empty when the statement has none.
	pub bindings: ExtensionBindings,
	pub comment: Expr,
	pub permissions: Permission,
}

impl ToSql for DefineModuleStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("DEFINE EXTENSION");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}
		if let Some(name) = &self.name {
			write_sql!(f, sql_fmt, " ext::{} FROM", EscapeKwFreeIdent(name));
		}
		write_sql!(f, sql_fmt, " {}", self.executable);
		if let Some(digest) = &self.digest {
			write_sql!(f, sql_fmt, " DIGEST {}", digest);
		}
		if let Some(signer) = &self.signer {
			write_sql!(f, sql_fmt, " SIGNER {}", signer);
		} else if self.unsigned {
			f.push_str(" UNSIGNED");
		}
		if let Some(capabilities) = &self.capabilities {
			write_sql!(f, sql_fmt, " CAPABILITIES {}", capabilities);
		}
		if !self.bindings.is_empty() {
			write_sql!(f, sql_fmt, " BINDINGS {}", self.bindings);
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
