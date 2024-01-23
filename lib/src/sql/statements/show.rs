use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Datetime, Table, Value};
use crate::vs::Versionstamp;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
pub enum ShowSince {
	Timestamp(Datetime),
	Versionstamp(u64),
}

impl ShowSince {
	pub fn versionstamp(vs: Versionstamp) -> ShowSince {
		// TODO Grab the 4 least significant bytes???
		ShowSince::Versionstamp((vs[3] << 24 | vs[2] << 16 | vs[1] << 8 | vs[0]) as u64)
	}

	pub fn as_versionstamp(&self) -> Option<Versionstamp> {
		match self {
			ShowSince::Timestamp(_) => None,
			ShowSince::Versionstamp(v) => Some([v[0], v[1], v[2], v[3], 0, 0, 0, 0, 0, 0].into()),
		}
	}
}

// ShowStatement is used to show changes in a table or database via
// the SHOW CHANGES statement.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
pub struct ShowStatement {
	pub table: Option<Table>,
	pub since: ShowSince,
	pub limit: Option<u32>,
}

impl ShowStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Selected DB?
		opt.is_allowed(Action::View, ResourceKind::Table, &Base::Db)?;
		// Clone transaction
		let txn = txn.clone();
		// Claim transaction
		let mut run = txn.lock().await;
		// Process the show query
		let tb = self.table.as_deref();
		let r = crate::cf::read(
			&mut run,
			opt.ns(),
			opt.db(),
			tb.map(|x| x.as_str()),
			self.since.clone(),
			self.limit,
		)
		.await?;
		// Return the changes
		let mut a = Vec::<Value>::new();
		for r in r.iter() {
			let v: Value = r.clone().into_value();
			a.push(v);
		}
		let v: Value = Value::Array(crate::sql::array::Array(a));
		Ok(v)
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
