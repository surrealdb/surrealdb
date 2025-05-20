use crate::ctx::Context;
use crate::dbs::{Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::sql::{
	Cond, Data, Explain, FlowResultExt as _, Output, SqlValue, SqlValues, Timeout, With,
};
use anyhow::{Result, ensure};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct UpsertStatement {
	pub only: bool,
	pub what: SqlValues,
	#[revision(start = 2)]
	pub with: Option<With>,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	#[revision(start = 2)]
	pub explain: Option<Explain>,
}

impl fmt::Display for UpsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "UPSERT")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", self.what)?;
		if let Some(ref v) = self.with {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.data {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.cond {
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
		if let Some(ref v) = self.explain {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

impl From<UpsertStatement> for crate::expr::statements::UpsertStatement {
	fn from(v: UpsertStatement) -> Self {
		Self {
			only: v.only,
			what: v.what.into(),
			with: v.with.map(Into::into),
			data: v.data.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::UpsertStatement> for UpsertStatement {
	fn from(v: crate::expr::statements::UpsertStatement) -> Self {
		Self {
			only: v.only,
			what: v.what.into(),
			with: v.with.map(Into::into),
			data: v.data.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
		}
	}
}
