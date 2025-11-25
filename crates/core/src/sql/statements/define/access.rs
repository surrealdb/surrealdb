use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::sql::access::AccessDuration;
use crate::sql::{AccessType, Base, Expr};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineAccessStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub access_type: AccessType,
	pub authenticate: Option<Expr>,
	pub duration: AccessDuration,
	pub comment: Option<Expr>,
}

impl ToSql for DefineAccessStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE ACCESS");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => {
				write_sql!(f, sql_fmt, " OVERWRITE");
			}
			DefineKind::IfNotExists => {
				write_sql!(f, sql_fmt, " IF NOT EXISTS");
			}
		}
		// The specific access method definition is displayed by AccessType
		write_sql!(f, sql_fmt, " {} ON {} TYPE {}", self.name, self.base, self.access_type);
		// The additional authentication clause
		if let Some(ref v) = self.authenticate {
			write_sql!(f, sql_fmt, " AUTHENTICATE {v}");
		}
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write_sql!(f, sql_fmt, " DURATION");
		if self.access_type.can_issue_grants() {
			f.push_str(" FOR GRANT ");
			match self.duration.grant {
				Some(ref dur) => write_sql!(f, sql_fmt, "{}", dur),
				None => f.push_str("NONE"),
			}
			f.push(',');
		}
		if self.access_type.can_issue_tokens() {
			f.push_str(" FOR TOKEN ");
			match self.duration.token {
				Some(ref dur) => write_sql!(f, sql_fmt, "{}", dur),
				None => f.push_str("NONE"),
			}
			f.push(',');
		}
		f.push_str(" FOR SESSION ");
		match self.duration.session {
			Some(ref dur) => write_sql!(f, sql_fmt, "{}", dur),
			None => f.push_str("NONE"),
		}
		if let Some(ref v) = self.comment {
			write_sql!(f, sql_fmt, " COMMENT {}", v);
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
			comment: v.comment.map(Into::into),
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
			comment: v.comment.map(Into::into),
		}
	}
}
