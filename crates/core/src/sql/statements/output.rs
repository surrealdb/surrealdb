use std::fmt;

use crate::sql::Expr;
use crate::sql::fetch::Fetchs;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct OutputStatement {
	pub what: Expr,
	pub fetch: Option<Fetchs>,
}

impl fmt::Display for OutputStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RETURN {}", self.what)?;
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
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
