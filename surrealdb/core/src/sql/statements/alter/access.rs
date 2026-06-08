use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{CoverStmts, QuoteStr};
use crate::sql::{Base, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER ACCESS`.
pub struct AlterAccessStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
	pub authenticate: AlterKind<Expr>,
	pub grant_duration: AlterKind<crate::types::PublicDuration>,
	pub token_duration: AlterKind<crate::types::PublicDuration>,
	pub session_duration: AlterKind<crate::types::PublicDuration>,
	pub comment: AlterKind<String>,
}

impl Default for AlterAccessStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			if_exists: false,
			authenticate: AlterKind::None,
			grant_duration: AlterKind::None,
			token_duration: AlterKind::None,
			session_duration: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterAccessStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER ACCESS");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {} ON {}", CoverStmts(&self.name), &self.base);

		match self.authenticate {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " AUTHENTICATE {}", CoverStmts(v)),
			AlterKind::Drop => f.push_str(" DROP AUTHENTICATE"),
			AlterKind::None => {}
		}

		let has_duration = !matches!(
			(&self.grant_duration, &self.token_duration, &self.session_duration),
			(AlterKind::None, AlterKind::None, AlterKind::None)
		);
		if has_duration {
			f.push_str(" DURATION");
			match self.grant_duration {
				AlterKind::Set(ref d) => write_sql!(f, fmt, " FOR GRANT {d},"),
				AlterKind::Drop => f.push_str(" FOR GRANT NONE,"),
				AlterKind::None => {}
			}
			match self.token_duration {
				AlterKind::Set(ref d) => write_sql!(f, fmt, " FOR TOKEN {d},"),
				AlterKind::Drop => f.push_str(" FOR TOKEN NONE,"),
				AlterKind::None => {}
			}
			match self.session_duration {
				AlterKind::Set(ref d) => write_sql!(f, fmt, " FOR SESSION {d}"),
				AlterKind::Drop => f.push_str(" FOR SESSION NONE"),
				AlterKind::None => {}
			}
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}

impl From<AlterAccessStatement> for crate::expr::statements::alter::AlterAccessStatement {
	fn from(v: AlterAccessStatement) -> Self {
		crate::expr::statements::alter::AlterAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
			authenticate: v.authenticate.into(),
			grant_duration: match v.grant_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d.into())),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			token_duration: match v.token_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d.into())),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			session_duration: match v.session_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d.into())),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterAccessStatement> for AlterAccessStatement {
	fn from(v: crate::expr::statements::alter::AlterAccessStatement) -> Self {
		use crate::types::PublicDuration;
		AlterAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
			authenticate: v.authenticate.into(),
			grant_duration: match v.grant_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => {
					AlterKind::Set(PublicDuration::from(d))
				}
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			token_duration: match v.token_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => {
					AlterKind::Set(PublicDuration::from(d))
				}
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			session_duration: match v.session_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => {
					AlterKind::Set(PublicDuration::from(d))
				}
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}
