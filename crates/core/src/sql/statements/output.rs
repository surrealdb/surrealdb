use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::sql::fetch::Fetchs;
use crate::sql::value::Value;
use crate::sql::ControlFlow;
use crate::{ctx::Context, sql::FlowResult};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct OutputStatement {
	pub what: Value,
	pub fetch: Option<Fetchs>,
}

impl OutputStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.what.writeable()
	}
}

impl From<OutputStatement> for crate::expr::statements::OutputStatement {
	fn from(v: OutputStatement) -> Self {
		crate::expr::statements::OutputStatement {
			what: v.what.into(),
			fetch: v.fetch.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::OutputStatement> for OutputStatement {
	fn from(v: crate::expr::statements::OutputStatement) -> Self {
		OutputStatement {
			what: v.what.into(),
			fetch: v.fetch.map(Into::into),
		}
	}
}

crate::sql::impl_display_from_sql!(OutputStatement);

impl crate::sql::DisplaySql for OutputStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RETURN {}", self.what)?;
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
	}
}
