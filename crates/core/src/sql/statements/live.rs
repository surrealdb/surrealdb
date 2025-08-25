use std::fmt;

use uuid::Uuid;

use crate::sql::{Cond, Expr, Fetchs, Fields};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct LiveStatement {
	pub fields: Fields,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
}

impl fmt::Display for LiveStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIVE SELECT {} FROM {}", self.fields, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

impl From<LiveStatement> for crate::expr::statements::LiveStatement {
	fn from(v: LiveStatement) -> Self {
		crate::expr::statements::LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			fields: v.fields.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
			auth: None,
			session: None,
		}
	}
}
impl From<crate::expr::statements::LiveStatement> for LiveStatement {
	fn from(v: crate::expr::statements::LiveStatement) -> Self {
		LiveStatement {
			fields: v.fields.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
		}
	}
}
