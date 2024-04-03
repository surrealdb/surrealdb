use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{Base, Ident, Permission, Strand, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
#[non_exhaustive]
pub struct DefineParamStatement {
	pub name: Ident,
	pub value: Value,
	pub comment: Option<Strand>,
	pub permissions: Permission,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineParamStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Parameter, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if param already exists
		if self.if_not_exists && run.get_db_param(opt.ns(), opt.db(), &self.name).await.is_ok() {
			return Err(Error::PaAlreadyExists {
				value: self.name.to_string(),
			});
		}
		// Process the statement
		let key = crate::key::database::pa::new(opt.ns(), opt.db(), &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.set(
			key,
			DefineParamStatement {
				// Compute the param
				value: self.value.compute(ctx, opt, txn, doc).await?,
				// Don't persist the "IF NOT EXISTS" clause to schema
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineParamStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE PARAM")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " ${} VALUE {}", self.name, self.value)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}
