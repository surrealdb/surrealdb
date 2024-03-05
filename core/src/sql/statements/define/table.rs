use std::fmt::{self, Display, Write};

use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::ctx::Context;
use crate::dbs::{Force, Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{
	changefeed::ChangeFeed,
	fmt::{is_pretty, pretty_indent},
	statements::UpdateStatement,
	Base, Ident, Permissions, Strand, Value, Values, View,
};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
pub struct DefineTableStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineTableStatement {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if table already exists
		if self.if_not_exists && run.get_tb(opt.ns(), opt.db(), &self.name).await.is_ok() {
			return Err(Error::TbAlreadyExists {
				value: self.name.to_string(),
			});
		}
		// Process the statement
		let key = crate::key::database::tb::new(opt.ns(), opt.db(), &self.name);
		let ns = run.add_ns(opt.ns(), opt.strict).await?;
		let db = run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		let dt = if self.id.is_none() && ns.id.is_some() && db.id.is_some() {
			DefineTableStatement {
				id: Some(run.get_next_tb_id(ns.id.unwrap(), db.id.unwrap()).await?),
				if_not_exists: false,
				..self.clone()
			}
		} else {
			DefineTableStatement {
				if_not_exists: false,
				..self.clone()
			}
		};
		run.set(key, &dt).await?;
		// Check if table is a view
		if let Some(view) = &self.view {
			// Remove the table data
			let key = crate::key::table::all::new(opt.ns(), opt.db(), &self.name);
			run.delp(key, u32::MAX).await?;
			// Process each foreign table
			for v in view.what.0.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(opt.ns(), opt.db(), v, &self.name);
				run.set(key, self).await?;
				// Clear the cache
				let key = crate::key::table::ft::prefix(opt.ns(), opt.db(), v);
				run.clr(key).await?;
			}
			// Release the transaction
			drop(run);
			// Force queries to run
			let opt = &opt.new_with_force(Some(Force::Table(Arc::new([dt]))));
			// Don't process field queries
			let opt = &opt.new_with_fields(false);
			// Don't process event queries
			let opt = &opt.new_with_events(false);
			// Don't process index queries
			let opt = &opt.new_with_indexes(false);
			// Process each foreign table
			for v in view.what.0.iter() {
				println!("{}", v);
				// Process the view data
				let stm = UpdateStatement {
					what: Values(vec![Value::Table(v.clone())]),
					..UpdateStatement::default()
				};
				stm.compute(ctx, opt, txn, doc).await?;
			}
			// let stm = UpdateStatement {
			// 	what: Values(vec![Value::Table(dt.name.into())]),
			// 	data: Some(dt.data(ctx, opt, txn, Action::Update, &tb.expr).await?),
			// 	..UpdateStatement::default()
			// };
			// stm.compute(ctx, opt, txn, doc).await?;
		} else if dt.changefeed.is_some() {
			run.record_table_change(opt.ns(), opt.db(), self.name.0.as_str(), &dt);
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TABLE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {}", self.name)?;
		if self.drop {
			f.write_str(" DROP")?;
		}
		f.write_str(if self.full {
			" SCHEMAFULL"
		} else {
			" SCHEMALESS"
		})?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if let Some(ref v) = self.view {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "{}", self.permissions)?;
		Ok(())
	}
}
