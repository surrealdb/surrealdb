use std::fmt;

use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};
use crate::val::Datetime;
use crate::vs::VersionStamp;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ShowSince {
	Timestamp(Datetime),
	Versionstamp(u64),
}

impl ShowSince {
	pub fn versionstamp(vs: &VersionStamp) -> ShowSince {
		ShowSince::Versionstamp(vs.into_u64_lossy())
	}

	pub fn as_versionstamp(&self) -> Option<VersionStamp> {
		match self {
			ShowSince::Timestamp(_) => None,
			ShowSince::Versionstamp(v) => Some(VersionStamp::from_u64(*v)),
		}
	}
}

/// A SHOW CHANGES statement for displaying changes made to a table or database.

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ShowStatement {
	pub table: Option<Ident>,
	pub since: ShowSince,
	pub limit: Option<u32>,
}

impl ShowStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::View, ResourceKind::Table, &Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Process the show query
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let r =
			crate::cf::read(&txn, ns, db, self.table.as_deref(), self.since.clone(), self.limit)
				.await?;
		// Return the changes
		let a: Vec<Value> = r.iter().cloned().map(|x| x.into_value()).collect();
		Ok(a.into())
	}
}

impl fmt::Display for ShowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SHOW CHANGES FOR")?;
		match self.table {
			Some(ref v) => write!(f, " TABLE {}", v)?,
			None => write!(f, " DATABASE")?,
		}
		match self.since {
			ShowSince::Timestamp(ref v) => write!(f, " SINCE {}", v)?,
			ShowSince::Versionstamp(ref v) => write!(f, " SINCE {}", v)?,
		}
		if let Some(ref v) = self.limit {
			write!(f, " LIMIT {}", v)?
		}
		Ok(())
	}
}
