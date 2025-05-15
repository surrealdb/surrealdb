use crate::ctx::{Context, MutableContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::planner::RecordStrategy;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::{Data, FlowResultExt as _, Id, Output, Table, Thing, Timeout, Value, Version};

use reblessive::tree::Stk;
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

fn iterable(id: Thing, v: Value, relation: bool) -> Result<Iterable, Error> {
	match relation {
		false => Ok(Iterable::Mergeable(id, v)),
		true => {
			let f = match v.pick(&*IN) {
				Value::Thing(v) => v,
				v => {
					return Err(Error::InsertStatementIn {
						value: v.to_string(),
					})
				}
			};
			let w = match v.pick(&*OUT) {
				Value::Thing(v) => v,
				v => {
					return Err(Error::InsertStatementOut {
						value: v.to_string(),
					})
				}
			};
			Ok(Iterable::Relatable(f, id, w, Some(v)))
		}
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
