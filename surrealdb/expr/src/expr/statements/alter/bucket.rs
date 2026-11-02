use common::fmt::QuoteStr;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::expr::permission::Permission;
use crate::expr::{Base, CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterBucketStatement {
	pub name: Expr,
	/// The level the bucket lives at; `DATABASE` when the statement names none.
	pub base: Base,
	pub if_exists: bool,
	pub backend: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub readonly: AlterKind<()>,
	pub comment: AlterKind<String>,
	/// Whether to invalidate every presigned URL already issued for this
	/// bucket, by replacing its `presign_nonce`.
	///
	/// Not an `AlterKind`: there is nothing to unset.
	pub revoke_signatures: bool,
}

impl Default for AlterBucketStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Db,
			if_exists: false,
			backend: AlterKind::None,
			permissions: None,
			readonly: AlterKind::None,
			comment: AlterKind::None,
			revoke_signatures: false,
		}
	}
}

impl ToSql for AlterBucketStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER BUCKET");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", CoverStmts(&self.name));
		if self.base != Base::Db {
			write_sql!(f, fmt, " ON {}", self.base);
		}

		match self.readonly {
			AlterKind::Set(_) => write_sql!(f, fmt, " READONLY"),
			AlterKind::Drop => write_sql!(f, fmt, " DROP READONLY"),
			AlterKind::None => {}
		}

		match self.backend {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " BACKEND {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP BACKEND"),
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			write_sql!(f, fmt, " PERMISSIONS {}", p);
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}

		if self.revoke_signatures {
			f.push_str(" REVOKE SIGNATURES");
		}
	}
}
