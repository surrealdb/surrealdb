use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{
	fmt::{is_pretty, pretty_indent},
	Base, Ident, Permission, Strand, Value,
};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
#[non_exhaustive]
pub struct DefineModelStatement {
	pub hash: String,
	pub name: Ident,
	pub version: String,
	pub comment: Option<Strand>,
	pub permissions: Permission,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl fmt::Display for DefineModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "DEFINE MODEL")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " ml::{}<{}>", self.name, self.version)?;
		if let Some(comment) = self.comment.as_ref() {
			write!(f, " COMMENT {}", comment)?;
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

impl DefineModelStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Model, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if model already exists
		if self.if_not_exists
			&& run.get_db_model(opt.ns(), opt.db(), &self.name, &self.version).await.is_ok()
		{
			return Err(Error::MlAlreadyExists {
				value: self.name.to_string(),
			});
		}
		// Process the statement
		let key = crate::key::database::ml::new(opt.ns(), opt.db(), &self.name, &self.version);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.set(
			key,
			DefineModelStatement {
				// Don't persist the "IF NOT EXISTS" clause to schema
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;
		// Store the model file
		// TODO
		// Ok all good
		Ok(Value::None)
	}
}
