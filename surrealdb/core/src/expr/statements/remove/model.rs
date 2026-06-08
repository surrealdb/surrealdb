use anyhow::Result;
use surrealdb_strand::Strand;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::model::get_model_path;
use crate::expr::{Base, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct RemoveModelStatement {
	pub name: Strand,
	pub version: Strand,
	pub if_exists: bool,
}

impl RemoveModelStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Model, Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the defined model
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let ml = match txn.get_db_model(ns, db, &self.name, &self.version, None).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::MlNotFound {
					name: format!("{}<{}>", self.name.as_str(), self.version.as_str()),
				}
				.into());
			}
		};
		// Delete the definition
		let key = crate::key::database::ml::new(ns, db, &ml.name, &ml.version);
		txn.del(&key).await?;
		// Clear the cache
		txn.clear_cache();
		// `obs::del` is idempotent, so this is safe even for definitions
		// registered without an uploaded artifact (e.g. via import).
		let (ns_name, db_name) = opt.ns_db()?;
		let path = get_model_path(ns_name, db_name, &ml.name, &ml.version, &ml.hash);
		crate::obs::del(&path).await?;
		// Ok all good
		Ok(Value::None)
	}
}
