use anyhow::Result;

use crate::catalog::Error;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Base;
use crate::expr::model::get_model_path;
use crate::expr::statements::remove::model::RemoveModelStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::MlModelKey;
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_model_statement_compute(
	this: &RemoveModelStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Model, Base::Db)?;
	// Get the transaction
	let txn = ctx.tx();
	// Get the defined model
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let ml = match txn.get_db_model(ns, db, &this.name, &this.version, None).await? {
		Some(x) => x,
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(Error::MlNotFound {
				name: format!("{}<{}>", this.name.as_str(), this.version.as_str()),
			}
			.into());
		}
	};
	// Delete the definition
	let key = MlModelKey {
		ns,
		db,
		ml: std::borrow::Cow::Borrowed(&ml.name),
		vn: std::borrow::Cow::Borrowed(&ml.version),
	};
	txn.del_key(&key).await?;
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
