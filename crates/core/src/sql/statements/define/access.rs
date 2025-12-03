use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::CoverStmts;
use crate::sql::access::AccessDuration;
use crate::sql::{AccessType, Base, Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DefineAccessStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub access_type: AccessType,
	pub authenticate: Option<Expr>,
	pub duration: AccessDuration,
	pub comment: Expr,
}

impl ToSql for DefineAccessStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE ACCESS");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => {
				write_sql!(f, fmt, " OVERWRITE");
			}
			DefineKind::IfNotExists => {
				write_sql!(f, fmt, " IF NOT EXISTS");
			}
		}
		// The specific access method definition is displayed by AccessType
		write_sql!(
			f,
			fmt,
			" {} ON {} TYPE {}",
			CoverStmts(&self.name),
			self.base,
			self.access_type
		);
		// The additional authentication clause
		if let Some(ref v) = self.authenticate {
			write_sql!(f, fmt, " AUTHENTICATE {}", CoverStmts(v))
		}
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write_sql!(f, fmt, " DURATION");
		if self.access_type.can_issue_grants() {
			write_sql!(f, fmt, " FOR GRANT {},", CoverStmts(&self.duration.grant));
		}
		if self.access_type.can_issue_tokens() {
			write_sql!(f, fmt, " FOR TOKEN {},", CoverStmts(&self.duration.token));
		}

		write_sql!(f, fmt, " FOR SESSION {}", CoverStmts(&self.duration.session));
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}

impl From<DefineAccessStatement> for crate::expr::statements::DefineAccessStatement {
	fn from(v: DefineAccessStatement) -> Self {
		crate::expr::statements::DefineAccessStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			access_type: v.access_type.into(),
			authenticate: v.authenticate.map(Into::into),
			duration: v.duration.into(),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::DefineAccessStatement> for DefineAccessStatement {
	fn from(v: crate::expr::statements::DefineAccessStatement) -> Self {
		DefineAccessStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			access_type: v.access_type.into(),
			authenticate: v.authenticate.map(Into::into),
			duration: v.duration.into(),
			comment: v.comment.into(),
		}
	}
}
