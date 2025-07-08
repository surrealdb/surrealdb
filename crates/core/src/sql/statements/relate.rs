use crate::sql::{Data, Expr, Output, Timeout};
use std::fmt;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RelateStatement {
	pub only: bool,
	pub kind: Expr,
	pub from: Expr,
	pub with: Expr,
	pub uniq: bool,
	pub data: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl fmt::Display for RelateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RELATE")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {} -> {} -> {}", self.from, self.kind, self.with)?;
		if self.uniq {
			f.write_str(" UNIQUE")?
		}
		if let Some(ref v) = self.data {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
		}
		Ok(())
	}
}

impl From<RelateStatement> for crate::expr::statements::RelateStatement {
	fn from(v: RelateStatement) -> Self {
		crate::expr::statements::RelateStatement {
			only: v.only,
			kind: v.kind.into(),
			from: v.from.into(),
			with: v.with.into(),
			uniq: v.uniq,
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
		}
	}
}

impl From<crate::expr::statements::RelateStatement> for RelateStatement {
	fn from(v: crate::expr::statements::RelateStatement) -> Self {
		RelateStatement {
			only: v.only,
			kind: v.kind.into(),
			from: v.from.into(),
			with: v.with.into(),
			uniq: v.uniq,
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
		}
	}
}
