use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::base::Base;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RemoveTableStatement {
	pub name: Ident,
}

impl RemoveTableStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Get the defined table
		let ns = run.get_ns(opt.ns()).await?;
		let ns = ns.id.unwrap();
		let db = run.get_db(opt.ns(), opt.db()).await?;
		let db = db.id.unwrap();
		let tb = run.get_tb(opt.ns(), opt.db(), &self.name).await?;
		let id = tb.id.unwrap();
		// Delete the definition
		let key = crate::key::database::tb::new(ns, db, &self.name);
		run.del(key).await?;
		// Remove the resource data
		let key = crate::key::table::all::new(ns, db, id);
		run.delp(key, u32::MAX).await?;
		// Check if this is a foreign table
		if let Some(view) = &tb.view {
			// Process each foreign table
			for v in view.what.0.iter() {
				let view = run.get_tb(opt.ns(), opt.db(), v).await?;
				let view_id = view.id.unwrap();
				// Save the view config
				let key = crate::key::table::ft::new(ns, db, view_id, &self.name);
				run.del(key).await?;
			}
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveTableStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TABLE {}", self.name)
	}
}

pub fn table(i: &str) -> IResult<&str, RemoveTableStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TABLE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident(i)?;
	Ok((
		i,
		RemoveTableStatement {
			name,
		},
	))
}
