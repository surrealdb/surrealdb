use common::fmt::{QuoteStr, SqlDuration};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::{Base, CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER ACCESS`.
pub struct AlterAccessStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
	pub authenticate: AlterKind<Expr>,
	pub context: AlterKind<Expr>,
	pub grant_duration: AlterKind<std::time::Duration>,
	pub token_duration: AlterKind<std::time::Duration>,
	pub session_duration: AlterKind<std::time::Duration>,
	pub comment: AlterKind<String>,
}

impl Default for AlterAccessStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			if_exists: false,
			authenticate: AlterKind::None,
			context: AlterKind::None,
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

		match self.context {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " CONTEXT {}", CoverStmts(v)),
			AlterKind::Drop => f.push_str(" DROP CONTEXT"),
			AlterKind::None => {}
		}

		let has_duration = !matches!(
			(&self.grant_duration, &self.token_duration, &self.session_duration),
			(AlterKind::None, AlterKind::None, AlterKind::None)
		);
		if has_duration {
			f.push_str(" DURATION");
			// Each altered clause is separated from the previous one, never
			// terminated: a trailing comma leaves the parser expecting a
			// fourth `FOR`.
			let mut written = false;
			let mut separate = |f: &mut String| {
				if std::mem::replace(&mut written, true) {
					f.push(',');
				}
			};
			match self.grant_duration {
				AlterKind::Set(d) => {
					separate(f);
					write_sql!(f, fmt, " FOR GRANT {}", SqlDuration(d))
				}
				AlterKind::Drop => {
					separate(f);
					f.push_str(" FOR GRANT NONE")
				}
				AlterKind::None => {}
			}
			match self.token_duration {
				AlterKind::Set(d) => {
					separate(f);
					write_sql!(f, fmt, " FOR TOKEN {}", SqlDuration(d))
				}
				AlterKind::Drop => {
					separate(f);
					f.push_str(" FOR TOKEN NONE")
				}
				AlterKind::None => {}
			}
			match self.session_duration {
				AlterKind::Set(d) => {
					separate(f);
					write_sql!(f, fmt, " FOR SESSION {}", SqlDuration(d))
				}
				AlterKind::Drop => {
					separate(f);
					f.push_str(" FOR SESSION NONE")
				}
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
