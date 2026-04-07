use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{EscapeKwFreeIdent, QuoteStr};
use crate::sql::{Block, Kind, Permission};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER FUNCTION`.
pub struct AlterFunctionStatement {
	pub name: String,
	pub if_exists: bool,
	pub args: AlterKind<Vec<(String, Kind)>>,
	pub block: AlterKind<Block>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub returns: AlterKind<Kind>,
}

impl ToSql for AlterFunctionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER FUNCTION");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " fn");
		for s in self.name.split("::") {
			write_sql!(f, fmt, "::");
			EscapeKwFreeIdent(s).fmt_sql(f, fmt);
		}

		if let AlterKind::Set(ref args) = self.args {
			write_sql!(f, fmt, "(");
			for (i, (name, kind)) in args.iter().enumerate() {
				if i > 0 {
					f.push_str(", ");
				}
				write_sql!(f, fmt, "${}: {kind}", EscapeKwFreeIdent(name));
			}
			f.push(')');
		}

		match self.returns {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " -> {v}"),
			AlterKind::Drop => f.push_str(" DROP RETURNS"),
			AlterKind::None => {}
		}

		if let AlterKind::Set(ref block) = self.block {
			f.push(' ');
			block.fmt_sql(f, fmt);
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			let fmt = fmt.increment();
			write_sql!(f, fmt, " PERMISSIONS {}", p);
		}
	}
}

impl From<AlterFunctionStatement> for crate::expr::statements::alter::AlterFunctionStatement {
	fn from(v: AlterFunctionStatement) -> Self {
		crate::expr::statements::alter::AlterFunctionStatement {
			name: v.name,
			if_exists: v.if_exists,
			args: match v.args {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(
					x.into_iter().map(|(n, k)| (n, k.into())).collect(),
				),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			block: v.block.into(),
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
			returns: v.returns.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterFunctionStatement> for AlterFunctionStatement {
	fn from(v: crate::expr::statements::alter::AlterFunctionStatement) -> Self {
		AlterFunctionStatement {
			name: v.name,
			if_exists: v.if_exists,
			args: match v.args {
				crate::expr::statements::alter::AlterKind::Set(x) => {
					AlterKind::Set(x.into_iter().map(|(n, k)| (n, k.into())).collect())
				}
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			block: v.block.into(),
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
			returns: v.returns.into(),
		}
	}
}
