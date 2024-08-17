use crate::ctx::Context;
use crate::dbs::{Action as LiveAction, Notification, Options, Statement};
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveTableStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveTableStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;

			// Notify live query subscribers
			self.terminate_lives(ctx, opt).await?;

			// Get the transaction
			let txn = ctx.tx();
			// Remove the index stores
			ctx.get_index_stores().table_removed(&txn, opt.ns()?, opt.db()?, &self.name).await?;
			// Get the defined table
			let tb = txn.get_tb(opt.ns()?, opt.db()?, &self.name).await?;
			// Delete the definition
			let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &self.name);
			txn.del(key).await?;
			// Remove the resource data
			let key = crate::key::table::all::new(opt.ns()?, opt.db()?, &self.name);
			txn.delp(key).await?;
			// Check if this is a foreign table
			if let Some(view) = &tb.view {
				// Process each foreign table
				for v in view.what.0.iter() {
					// Save the view config
					let key = crate::key::table::ft::new(opt.ns()?, opt.db()?, v, &self.name);
					txn.del(key).await?;
				}
			}
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::TbNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}

	async fn terminate_lives(&self, ctx: &Context, opt: &Options) -> Result<(), Error> {
		// Check if we can send notifications
		if let Some(chn) = &opt.sender {
			// Get all live queries for this table
			let lives = ctx.tx().all_tb_lives(opt.ns()?, opt.db()?, &self.name).await?;

			for lv in lives.iter() {
				// Create a new statement
				let met = Value::from("TERMINATE");

				let mut lqctx = match lv.context(ctx) {
					Some(ctx) => ctx,
					None => continue,
				};

				lqctx.add_value("event", met.into());

				if opt.id()? == lv.node.0 {
					chn.send(Notification {
						id: lv.id,
						action: LiveAction::Terminate,
						result: Value::None,
					})
					.await?;
				}
			}
		}

		Ok(())
	}
}

impl Display for RemoveTableStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TABLE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
