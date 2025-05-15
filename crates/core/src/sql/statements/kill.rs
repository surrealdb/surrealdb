use crate::dbs::{Action, Notification, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::kvs::Live;
use crate::sql::Value;
use crate::{ctx::Context, sql::FlowResultExt as _, sql::Uuid};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Value,
}

crate::sql::impl_display_from_sql!(KillStatement);

impl crate::sql::DisplaySql for KillStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "KILL {}", self.id)
	}
}
