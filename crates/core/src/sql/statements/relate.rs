use std::fmt;

use crate::sql::{Data, Expr, Output, Timeout};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RelateStatement {
	pub only: bool,
	/// The expression through which we create a relation
	pub through: Expr,
	/// The expression the relation is from
	pub from: Expr,
	/// The expression the relation targets.
	pub to: Expr,
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
		write!(f, " {} -> {} -> {}", self.from, self.through, self.to)?;
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
			through: v.through.into(),
			from: v.from.into(),
			to: v.to.into(),
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
			through: v.through.into(),
			from: v.from.into(),
			to: v.to.into(),
			uniq: v.uniq,
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
		}
	}
}
