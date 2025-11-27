use std::fmt::{self, Display, Write};

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::EscapeKwFreeIdent;
use crate::sql::{Block, Expr, Kind, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineFunctionStatement {
	pub kind: DefineKind,
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: Block,
	pub comment: Option<Expr>,
	pub permissions: Permission,
	pub returns: Option<Kind>,
}

impl ToSql for DefineFunctionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE FUNCTION");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, fmt, " fn");
		for s in self.name.split("::") {
			write_sql!(f, fmt, "::");
			EscapeKwFreeIdent(s).fmt_sql(f, fmt);
		}
		write_sql!(f, fmt, "(");
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			write_sql!(f, fmt, "${}: {kind}", EscapeKwFreeIdent(name));
		}
		f.push_str(") ");
		if let Some(ref v) = self.returns {
			write_sql!(f, fmt, "-> {v} ");
		}
		self.block.fmt_sql(f, fmt);
		if let Some(ref v) = self.comment {
			write_sql!(f, fmt, " COMMENT {}", v);
		}
		let fmt = fmt.increment();
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}

impl From<DefineFunctionStatement> for crate::expr::statements::DefineFunctionStatement {
	fn from(v: DefineFunctionStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			args: v.args.into_iter().map(|(i, k)| (i, k.into())).collect(),
			block: v.block.into(),
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
			returns: v.returns.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DefineFunctionStatement> for DefineFunctionStatement {
	fn from(v: crate::expr::statements::DefineFunctionStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			args: v.args.into_iter().map(|(i, k)| (i, k.into())).collect(),
			block: v.block.into(),
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
			returns: v.returns.map(Into::into),
		}
	}
}
