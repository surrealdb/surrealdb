use std::fmt::{self, Display, Write};
use std::ops::Deref;

use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::{Base, Ident, Timeout, Value};
use crate::iam::{Action, ResourceKind};
use crate::key::database::sq::Sq;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AlterSequenceStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub timeout: Option<Timeout>,
}

impl AlterSequenceStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the sequence definition
		let mut sq = match txn.get_db_sequence(ns, db, &self.name).await {
			Ok(tb) => tb.deref().clone(),
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::SeqNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Process the statement
		if let Some(timeout) = &self.timeout {
			if timeout.is_zero() {
				sq.timeout = None;
			} else {
				sq.timeout = Some(*timeout.as_std_duration());
			}
		}
		// Set the table definition
		let key = Sq::new(ns, db, &self.name);
		txn.set(&key, &sq, None).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for AlterSequenceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER SEQUENCE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		if let Some(ref timeout) = self.timeout {
			write!(f, " TIMEOUT {timeout}")?;
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		Ok(())
	}
}
