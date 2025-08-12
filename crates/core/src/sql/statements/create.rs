use std::fmt;

use crate::sql::fmt::Fmt;
use crate::sql::{Data, Expr, Output, Timeout};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct CreateStatement {
	// A keyword modifier indicating if we are expecting a single result or several
	pub only: bool,
	// Where we are creating (i.e. table, or record ID)
	pub what: Vec<Expr>,
	// The data associated with the record being created
	pub data: Option<Data>,
	//  What the result of the statement should resemble (i.e. Diff or no result etc).
	pub output: Option<Output>,
	// The timeout for the statement
	pub timeout: Option<Timeout>,
	// If the statement should be run in parallel
	pub parallel: bool,
	// Version as nanosecond timestamp passed down to Datastore
	pub version: Option<Expr>,
}

impl fmt::Display for CreateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "CREATE")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", Fmt::comma_separated(self.what.iter()))?;
		if let Some(ref v) = self.data {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.version {
			write!(f, " VERSION {v}")?
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

impl From<CreateStatement> for crate::expr::statements::CreateStatement {
	fn from(v: CreateStatement) -> Self {
		crate::expr::statements::CreateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			version: v.version.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::CreateStatement> for CreateStatement {
	fn from(v: crate::expr::statements::CreateStatement) -> Self {
		CreateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			version: v.version.map(Into::into),
		}
	}
}
