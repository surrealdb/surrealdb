use std::fmt::{self, Display};

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveModelStatement {
	pub name: Expr,
	pub version: String,
	pub if_exists: bool,
}

impl Default for RemoveModelStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			version: String::new(),
			if_exists: false,
		}
	}
}

impl RemoveModelStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Model, &Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Compute the name
		let name =
			expr_to_ident(stk, ctx, opt, doc, &self.name, "model name").await?.to_raw_string();
		// Get the defined model
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let ml = match txn.get_db_model(ns, db, &name, &self.version).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::MlNotFound {
					name: format!("{}<{}>", name, self.version),
				}
				.into());
			}
		};
		// Delete the definition
		let key = crate::key::database::ml::new(ns, db, &ml.name, &ml.version);
		txn.del(&key).await?;
		// Clear the cache
		txn.clear_cache();
		// TODO Remove the model file from storage
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE MODEL")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " ml::{}<{}>", &self.name, self.version)?;
		Ok(())
	}
}
