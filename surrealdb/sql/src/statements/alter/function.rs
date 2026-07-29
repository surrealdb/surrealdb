use common::fmt::{EscapeKwFreeIdent, QuoteStr};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::{Block, Kind, Permission};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER FUNCTION`.
pub struct AlterFunctionStatement {
	pub name: Strand,
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
		for s in self.name.as_str().split("::") {
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
