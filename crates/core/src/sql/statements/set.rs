use crate::sql::Value;
use crate::sql::Kind;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct SetStatement {
	pub name: String,
	pub what: Value,
	#[revision(start = 2)]
	pub kind: Option<Kind>,
}

impl SetStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.what.writeable()
	}
}

impl From<SetStatement> for crate::expr::statements::SetStatement {
	fn from(v: SetStatement) -> Self {
		crate::expr::statements::SetStatement {
			name: v.name,
			what: v.what.into(),
			kind: v.kind.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::SetStatement> for SetStatement {
	fn from(v: crate::expr::statements::SetStatement) -> Self {
		SetStatement {
			name: v.name,
			what: v.what.into(),
			kind: v.kind.map(Into::into),
		}
	}
}

crate::sql::impl_display_from_sql!(SetStatement);

impl crate::sql::DisplaySql for SetStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LET ${}", self.name)?;
		if let Some(ref kind) = self.kind {
			write!(f, ": {}", kind)?;
		}
		write!(f, " = {}", self.what)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::syn::parse;

	#[test]
	fn check_type() {
		let query = parse("LET $param = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param = 5;");

		let query = parse("LET $param: number = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param: number = 5;");
	}
}
