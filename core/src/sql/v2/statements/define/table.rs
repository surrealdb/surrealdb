use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{
	changefeed::ChangeFeed,
	fmt::{is_pretty, pretty_indent},
	statements::UpdateStatement,
	Base, Ident, Permissions, Strand, Value, Values, View,
};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
pub struct DefineTableStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<Strand>,
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
		// Process the statement
		let key = crate::key::database::tb::new(opt.ns(), opt.db(), &self.name);
		let ns = run.add_ns(opt.ns(), opt.strict).await?;
		let db = run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		let dt = if self.id.is_none() && ns.id.is_some() && db.id.is_some() {
			let mut tb = self.clone();
			tb.id = Some(run.get_next_tb_id(ns.id.unwrap(), db.id.unwrap()).await?);
			run.set(key, &tb).await?;
			tb
		} else {
			run.set(key, self).await?;
			self.to_owned()
		};
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
			let opt = &opt.new_with_force(true);
			// Don't process field queries
			let opt = &opt.new_with_fields(false);
			// Don't process event queries
			let opt = &opt.new_with_events(false);
			// Don't process index queries
			let opt = &opt.new_with_indexes(false);
			// Process each foreign table
			for v in view.what.0.iter() {
				// Process the view data
				let stm = UpdateStatement {
					what: Values(vec![Value::Table(v.clone())]),
					..UpdateStatement::default()
				};
				stm.compute(ctx, opt, txn, doc).await?;
			}
		} else if dt.changefeed.is_some() {
			run.record_table_change(opt.ns(), opt.db(), self.name.0.as_str(), &dt);
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TABLE {}", self.name)?;
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

#[cfg(test)]
mod test {
	use crate::sql::statements::DefineStatement;
	use crate::sql::{ChangeFeed, Duration};
	use crate::syn::v2::parser::Parser;
	use nom::AsBytes;

	#[test]
	pub fn can_define_changefeed_on_table() {
		let mut parser = Parser::new("DEFINE TABLE test CHANGEFEED 10m".as_bytes());
		let stm = parser.parse_define_stmt()?;
		assert_eq!(stm.to_string(), "DEFINE TABLE test CHANGEFEED 10m");
		let table_stm = match stm {
			DefineStatement::Table(t) => t,
			_ => panic!("Expected table statement"),
		};
		assert_eq!(table_stm.name.0, "test");
		let cf = table_stm.changefeed.unwrap();
		let expected = ChangeFeed {
			expiry: Duration::from_mins(10).into(),
			store_original: false,
		};
		assert_eq!(cf, expected);
	}
}
