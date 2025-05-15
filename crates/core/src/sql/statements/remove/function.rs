use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveFunctionStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

crate::sql::impl_display_from_sql!(RemoveFunctionStatement);

impl crate::sql::DisplaySql for RemoveFunctionStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE FUNCTION")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " fn::{}", self.name.0)?;
		Ok(())
	}
}
