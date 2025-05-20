use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::{Base, Ident, Object, SqlValue, Version};
use crate::sys::INFORMATION;
use anyhow::Result;
use anyhow::bail;

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum InfoStatement {
	// revision discriminant override accounting for previous behavior when adding variants and
	// removing not at the end of the enum definition.
	#[revision(override(revision = 2, discriminant = 1), override(revision = 3, discriminant = 1))]
	Root(#[revision(start = 2)] bool),

	#[revision(override(revision = 2, discriminant = 3), override(revision = 3, discriminant = 3))]
	Ns(#[revision(start = 2)] bool),

	#[revision(override(revision = 2, discriminant = 5), override(revision = 3, discriminant = 5))]
	Db(#[revision(start = 2)] bool, #[revision(start = 5)] Option<Version>),

	#[revision(override(revision = 2, discriminant = 7), override(revision = 3, discriminant = 7))]
	Tb(Ident, #[revision(start = 2)] bool, #[revision(start = 5)] Option<Version>),

	#[revision(override(revision = 2, discriminant = 9), override(revision = 3, discriminant = 9))]
	User(Ident, Option<Base>, #[revision(start = 2)] bool),

	#[revision(start = 3)]
	#[revision(override(revision = 3, discriminant = 10))]
	Index(Ident, Ident, bool),
}

impl InfoStatement {
	pub(crate) fn structurize(self) -> Self {
		match self {
			InfoStatement::Root(_) => InfoStatement::Root(true),
			InfoStatement::Ns(_) => InfoStatement::Ns(true),
			InfoStatement::Db(_, v) => InfoStatement::Db(true, v),
			InfoStatement::Tb(t, _, v) => InfoStatement::Tb(t, true, v),
			InfoStatement::User(u, b, _) => InfoStatement::User(u, b, true),
			InfoStatement::Index(i, t, _) => InfoStatement::Index(i, t, true),
		}
	}

	pub(crate) fn versionize(self, v: Version) -> Self {
		match self {
			InfoStatement::Db(s, _) => InfoStatement::Db(s, Some(v)),
			InfoStatement::Tb(t, s, _) => InfoStatement::Tb(t, s, Some(v)),
			_ => self,
		}
	}
}

impl fmt::Display for InfoStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Root(false) => f.write_str("INFO FOR ROOT"),
			Self::Root(true) => f.write_str("INFO FOR ROOT STRUCTURE"),
			Self::Ns(false) => f.write_str("INFO FOR NAMESPACE"),
			Self::Ns(true) => f.write_str("INFO FOR NAMESPACE STRUCTURE"),
			Self::Db(false, v) => match v {
				Some(v) => write!(f, "INFO FOR DATABASE VERSION {v}"),
				None => f.write_str("INFO FOR DATABASE"),
			},
			Self::Db(true, v) => match v {
				Some(v) => write!(f, "INFO FOR DATABASE VERSION {v} STRUCTURE"),
				None => f.write_str("INFO FOR DATABASE STRUCTURE"),
			},
			Self::Tb(t, false, v) => match v {
				Some(v) => write!(f, "INFO FOR TABLE {t} VERSION {v}"),
				None => write!(f, "INFO FOR TABLE {t}"),
			},

			Self::Tb(t, true, v) => match v {
				Some(v) => write!(f, "INFO FOR TABLE {t} VERSION {v} STRUCTURE"),
				None => write!(f, "INFO FOR TABLE {t} STRUCTURE"),
			},
			Self::User(u, b, false) => match b {
				Some(b) => write!(f, "INFO FOR USER {u} ON {b}"),
				None => write!(f, "INFO FOR USER {u}"),
			},
			Self::User(u, b, true) => match b {
				Some(b) => write!(f, "INFO FOR USER {u} ON {b} STRUCTURE"),
				None => write!(f, "INFO FOR USER {u} STRUCTURE"),
			},
			Self::Index(i, t, false) => write!(f, "INFO FOR INDEX {i} ON {t}"),
			Self::Index(i, t, true) => write!(f, "INFO FOR INDEX {i} ON {t} STRUCTURE"),
		}
	}
}

impl From<InfoStatement> for crate::expr::statements::InfoStatement {
	fn from(v: InfoStatement) -> Self {
		match v {
			InfoStatement::Root(v) => Self::Root(v),
			InfoStatement::Ns(v) => Self::Ns(v),
			InfoStatement::Db(v, ver) => Self::Db(v, ver.map(Into::into)),
			InfoStatement::Tb(t, v, ver) => Self::Tb(t.into(), v, ver.map(Into::into)),
			InfoStatement::User(u, b, v) => Self::User(u.into(), b.map(Into::into), v),
			InfoStatement::Index(i, t, v) => Self::Index(i.into(), t.into(), v),
		}
	}
}

impl From<crate::expr::statements::InfoStatement> for InfoStatement {
	fn from(v: crate::expr::statements::InfoStatement) -> Self {
		match v {
			crate::expr::statements::InfoStatement::Root(v) => Self::Root(v),
			crate::expr::statements::InfoStatement::Ns(v) => Self::Ns(v),
			crate::expr::statements::InfoStatement::Db(v, ver) => Self::Db(v, ver.map(Into::into)),
			crate::expr::statements::InfoStatement::Tb(t, v, ver) => {
				Self::Tb(t.into(), v, ver.map(Into::into))
			}
			crate::expr::statements::InfoStatement::User(u, b, v) => {
				Self::User(u.into(), b.map(Into::into), v)
			}
			crate::expr::statements::InfoStatement::Index(i, t, v) => {
				Self::Index(i.into(), t.into(), v)
			}
		}
	}
}
