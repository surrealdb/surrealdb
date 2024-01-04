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
use crate::sql::{Idiom, Kind, Part};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

use super::DefineFieldStatement;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
	pub relation: Option<(Option<Kind>, Option<Kind>)>,
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
		if let Some((in_field, out_field)) = &self.relation {
			let in_kind = in_field.clone().unwrap_or(Kind::Record(vec![]));
			let out_kind = out_field.clone().unwrap_or(Kind::Record(vec![]));

			let in_key = crate::key::table::fd::new(opt.ns(), opt.db(), &self.name, "in");
			let out_key = crate::key::table::fd::new(opt.ns(), opt.db(), &self.name, "out");

			// TODO: fix permissions so they don't defalut to full
			run.set(
				in_key,
				DefineFieldStatement {
					name: Idiom(vec![Part::from("in")]),
					what: self.name.clone(),
					kind: Some(in_kind),
					..Default::default()
				},
			)
			.await?;
			run.set(
				out_key,
				DefineFieldStatement {
					name: Idiom(vec![Part::from("out")]),
					what: self.name.clone(),
					kind: Some(out_kind),
					..Default::default()
				},
			)
			.await?;
		}

		// TODO: define id field here

		let tb_key = crate::key::table::fd::prefix(opt.ns(), opt.db(), &self.name);
		run.clr(tb_key).await?;
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

impl DefineTableStatement {
	pub fn is_relation(&self) -> bool {
		self.relation.is_some()
	}
}

fn get_tables_from_kind(kind: &Kind) -> String {
	let Kind::Record(tables) = kind else {
		panic!()
	};
	tables.into_iter().map(ToString::to_string).collect::<Vec<_>>().join(" | ")
}

impl Display for DefineTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TABLE {}", self.name)?;
		if self.drop {
			f.write_str(" DROP")?;
		}
		if let Some((in_field, out_field)) = &self.relation {
			f.write_str(" RELATION")?;
			if let Some(kind) = in_field {
				write!(f, " FROM {}", get_tables_from_kind(kind))?;
			}
			if let Some(kind) = out_field {
				write!(f, " TO {}", get_tables_from_kind(kind))?;
			}
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
