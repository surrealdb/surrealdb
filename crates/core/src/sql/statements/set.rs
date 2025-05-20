use crate::sql::SqlValue;
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
	pub what: SqlValue,
	#[revision(start = 2)]
	pub kind: Option<Kind>,
}

impl fmt::Display for SetStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LET ${}", self.name)?;
		if let Some(ref kind) = self.kind {
			write!(f, ": {}", kind)?;
		}
		write!(f, " = {}", self.what)?;
		Ok(())
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
