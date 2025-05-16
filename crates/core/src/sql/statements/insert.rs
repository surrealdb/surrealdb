use crate::err::Error;
use crate::sql::{Data, Id, Output, Table, Thing, Timeout, Value, Version};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct InsertStatement {
	pub into: Option<Value>,
	pub data: Data,
	/// Does the statement have the ignore clause.
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	#[revision(start = 2)]
	pub relation: bool,
	#[revision(start = 3)]
	pub version: Option<Version>,
}

impl InsertStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
}

impl From<InsertStatement> for crate::expr::statements::InsertStatement {
	fn from(v: InsertStatement) -> Self {
		crate::expr::statements::InsertStatement {
			into: v.into.map(Into::into),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			relation: v.relation,
			version: v.version.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::InsertStatement> for InsertStatement {
	fn from(v: crate::expr::statements::InsertStatement) -> Self {
		InsertStatement {
			into: v.into.map(Into::into),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			relation: v.relation,
			version: v.version.map(Into::into),
		}
	}
}

crate::sql::impl_display_from_sql!(InsertStatement);

impl crate::sql::DisplaySql for InsertStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("INSERT")?;
		if self.relation {
			f.write_str(" RELATION")?
		}
		if self.ignore {
			f.write_str(" IGNORE")?
		}
		if let Some(into) = &self.into {
			write!(f, " INTO {}", into)?;
		}
		write!(f, " {}", self.data)?;
		if let Some(ref v) = self.update {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.version {
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

fn gen_id(v: &Value, into: &Option<Table>) -> Result<Thing, Error> {
	match into {
		Some(into) => v.rid().generate(into, true),
		None => match v.rid() {
			Value::Thing(v) => match v {
				Thing {
					id: Id::Generate(_),
					..
				} => Err(Error::InsertStatementId {
					value: v.to_string(),
				}),
				v => Ok(v),
			},
			v => Err(Error::InsertStatementId {
				value: v.to_string(),
			}),
		},
	}
}
