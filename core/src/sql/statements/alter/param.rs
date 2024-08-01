use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{Base, Ident, Permission, Strand, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AlterParamStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub value: Option<Value>,
	pub comment: Option<Option<Strand>>,
	pub permissions: Option<Permission>,
}

impl AlterParamStatement {
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
		let mut dp = match txn.get_db_param(opt.ns()?, opt.db()?, &self.name).await {
			Ok(dp) => dp.deref().clone(),
			Err(Error::PaNotFound {
				..
			}) if self.if_exists => return Ok(Value::None),
			Err(v) => return Err(v),
		};
		// Process the statement
		let key = crate::key::database::pa::new(ns, db, &self.name);
		if let Some(ref value) = &self.value {
			dp.value = value.clone();
		}
		if let Some(ref permissions) = &self.permissions {
			dp.permissions = permissions.clone();
		}
		if let Some(ref comment) = &self.comment {
			dp.comment = comment.clone();
		}

		txn.set(key, &dp).await?;

		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for AlterParamStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER PARAM")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " ${}", self.name)?;
		if let Some(ref value) = &self.value {
			write!(f, " VALUE {value}")?
		}
		if let Some(ref comment) = &self.comment {
			write!(f, " COMMENT {}", comment.clone().map_or("UNSET".into(), |v| v.to_string()))?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		if let Some(permissions) = &self.permissions {
			write!(f, "PERMISSIONS {permissions}")?;
		}
		Ok(())
	}
}
