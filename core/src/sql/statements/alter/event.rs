use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Strand, Value, Values};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AlterEventStatement {
	pub name: Ident,
	pub what: Ident,
	pub if_exists: bool,
	pub when: Option<Value>,
	pub then: Option<Values>,
	pub comment: Option<Option<Strand>>,
}

impl AlterEventStatement {
	pub(crate) async fn compute(
		&self,
		_stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Get the NS and DB
		let ns = opt.ns()?;
		let db = opt.db()?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the table definition
		let mut de = match txn.get_tb_event(opt.ns()?, opt.db()?, &self.what, &self.name).await {
			Ok(de) => de.deref().clone(),
			Err(Error::EvNotFound {
				..
			}) if self.if_exists => return Ok(Value::None),
			Err(v) => return Err(v),
		};
		// Process the statement
		let key = crate::key::table::ev::new(ns, db, &self.what, &self.name);
		if let Some(ref when) = &self.when {
			de.when = when.clone();
		}
		if let Some(ref then) = &self.then {
			de.then = then.clone();
		}
		if let Some(ref comment) = &self.comment {
			de.comment = comment.clone();
		}

		txn.set(key, &de).await?;

		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for AlterEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER EVENT")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if let Some(ref when) = &self.when {
			write!(f, " WHEN {when}")?
		}
		if let Some(ref then) = &self.then {
			write!(f, " THEN {then}")?
		}
		if let Some(ref comment) = &self.comment {
			write!(f, " COMMENT {}", comment.clone().map_or("UNSET".into(), |v| v.to_string()))?
		}
		Ok(())
	}
}
