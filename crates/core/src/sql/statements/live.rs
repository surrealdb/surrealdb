use std::fmt;

use uuid::Uuid;

use crate::fmt::CoverStmts;
use crate::sql::{Cond, Expr, Fetchs, Fields};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum LiveFields {
	Diff,
	Select(Fields),
}

impl From<LiveFields> for crate::expr::statements::LiveFields {
	fn from(v: LiveFields) -> Self {
		match v {
			LiveFields::Diff => crate::expr::statements::LiveFields::Diff,
			LiveFields::Select(fields) => {
				crate::expr::statements::LiveFields::Select(fields.into())
			}
		}
	}
}
impl From<crate::expr::statements::LiveFields> for LiveFields {
	fn from(v: crate::expr::statements::LiveFields) -> Self {
		match v {
			crate::expr::statements::LiveFields::Diff => LiveFields::Diff,
			crate::expr::statements::LiveFields::Select(fields) => {
				LiveFields::Select(fields.into())
			}
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct LiveStatement {
	pub fields: LiveFields,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
}

impl fmt::Display for LiveStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIVE SELECT")?;
		match &self.fields {
			LiveFields::Diff => write!(f, " DIFF")?,
			LiveFields::Select(x) => write!(f, " {}", x)?,
		}
		write!(f, " FROM {}", CoverStmts(&self.what))?;
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
