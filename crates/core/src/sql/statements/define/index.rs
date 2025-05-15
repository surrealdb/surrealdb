use crate::ctx::Context;
#[cfg(target_family = "wasm")]
use crate::dbs::Force;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineTableStatement;
#[cfg(target_family = "wasm")]
use crate::sql::statements::{RemoveIndexStatement, UpdateStatement};
use crate::sql::{Base, Ident, Idioms, Index, Part, Strand, Value};
#[cfg(target_family = "wasm")]
use crate::sql::{Output, Values};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self};
#[cfg(target_family = "wasm")]
use std::sync::Arc;
use uuid::Uuid;

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub cols: Idioms,
	pub index: Index,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
	#[revision(start = 4)]
	pub concurrently: bool,
}

impl From<DefineIndexStatement> for crate::expr::statements::DefineIndexStatement {
	fn from(v: DefineIndexStatement) -> Self {
		Self {
			name: v.name.into(),
			what: v.what.into(),
			cols: v.cols.into_iter().map(Into::into).collect(),
			index: v.index.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			concurrently: v.concurrently,
		}
	}
}

impl From<crate::expr::statements::DefineIndexStatement> for DefineIndexStatement {
	fn from(v: crate::expr::statements::DefineIndexStatement) -> Self {
		Self {
			name: v.name.into(),
			what: v.what.into(),
			cols: v.cols.into_iter().map(Into::into).collect(),
			index: v.index.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			concurrently: v.concurrently,
		}
	}
}

crate::sql::impl_display_from_sql!(DefineIndexStatement);

impl crate::sql::DisplaySql for DefineIndexStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE INDEX")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} ON {} FIELDS {}", self.name, self.what, self.cols)?;
		if Index::Idx != self.index {
			write!(f, " {}", self.index)?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if self.concurrently {
			write!(f, " CONCURRENTLY")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineIndexStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"what".to_string() => self.what.structure(),
			"cols".to_string() => self.cols.structure(),
			"index".to_string() => self.index.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
