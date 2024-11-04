use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Ident, Strand, Value, Values};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::ops::Add;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineEventStatement {
	pub name: Ident,
	pub what: Ident,
	pub when: Value,
	pub then: Values,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
}

impl DefineEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_tb_event(opt.ns()?, opt.db()?, &self.what, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::EvAlreadyExists {
					value: self.name.to_string(),
				});
			}
		}
		// Process the statement
		let key = crate::key::table::ev::new(opt.ns()?, opt.db()?, &self.what, &self.name);
		txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
		txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
		txn.get_or_add_tb(opt.ns()?, opt.db()?, &self.what, opt.strict).await?;
		txn.set(
			key,
			DefineEventStatement {
				// Don't persist the `IF NOT EXISTS` clause to schema
				if_not_exists: false,
				overwrite: false,
				..self.clone()
			},
			None,
		)
		.await?;
		// Refresh the table cache
		let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &self.what);
		let tb = txn.get_tb(opt.ns()?, opt.db()?, &self.what).await?;
		txn.set(
			key,
			DefineTableStatement {
				cache_events_ts: tb.cache_events_ts.add(1),
				..tb.as_ref().clone()
			},
			None,
		)
		.await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE EVENT",)?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} ON {} WHEN {} THEN {}", self.name, self.what, self.when, self.then)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineEventStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"what".to_string() => self.what.structure(),
			"when".to_string() => self.when.structure(),
			"then".to_string() => self.then.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
