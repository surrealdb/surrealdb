use std::fmt;

use crate::sql::{Expr, Ident, Kind};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SetStatement {
	pub name: Ident,
	pub what: Expr,
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
			name: v.name.into(),
			what: v.what.into(),
			kind: v.kind.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::SetStatement> for SetStatement {
	fn from(v: crate::expr::statements::SetStatement) -> Self {
		SetStatement {
			name: v.name.into(),
			what: v.what.into(),
			kind: v.kind.map(Into::into),
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::syn;

	#[test]
	fn check_type() {
		let query = syn::parse("LET $param = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param = 5;");

		let query = syn::parse("LET $param: number = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param: number = 5;");
	}
}
