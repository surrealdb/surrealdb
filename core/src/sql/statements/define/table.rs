use crate::ctx::Context;
use crate::dbs::{Force, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{
	changefeed::ChangeFeed,
	fmt::{is_pretty, pretty_indent},
	statements::UpdateStatement,
	Base, Ident, Object, Permissions, Strand, Value, Values, View,
};
use std::sync::Arc;

use crate::sql::statements::info::InfoStructure;
use crate::sql::{Idiom, Kind, Part, Table, TableType};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

use super::DefineFieldStatement;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	#[revision(start = 3)]
	pub kind: TableType,
}

impl DefineTableStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Claim transaction
		let mut run = ctx.tx_lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if table already exists
		if run.get_tb(opt.ns(), opt.db(), &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::TbAlreadyExists {
					value: self.name.to_string(),
				});
			}
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
		if let TableType::Relation(rel) = &self.kind {
			let tb: &str = &self.name;
			let in_kind = rel.from.clone().unwrap_or(Kind::Record(vec![]));
			let out_kind = rel.to.clone().unwrap_or(Kind::Record(vec![]));
			let in_key = crate::key::table::fd::new(opt.ns(), opt.db(), tb, "in");
			let out_key = crate::key::table::fd::new(opt.ns(), opt.db(), tb, "out");
			run.set(
				in_key,
				DefineFieldStatement {
					name: Idiom(vec![Part::from("in")]),
					what: tb.into(),
					kind: Some(in_kind),
					..Default::default()
				},
			)
			.await?;
			run.set(
				out_key,
				DefineFieldStatement {
					name: Idiom(vec![Part::from("out")]),
					what: tb.into(),
					kind: Some(out_kind),
					..Default::default()
				},
			)
			.await?;
		}

		let tb_key = crate::key::table::fd::prefix(opt.ns(), opt.db(), &self.name);
		run.clr(tb_key).await?;
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
			let opt = &opt.new_with_force(Force::Table(Arc::new([dt])));
			// Process each foreign table
			for v in view.what.0.iter() {
				// Process the view data
				let stm = UpdateStatement {
					what: Values(vec![Value::Table(v.clone())]),
					..UpdateStatement::default()
				};
				stm.compute(stk, ctx, opt, doc).await?;
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
		matches!(self.kind, TableType::Relation(_))
	}

	pub fn allows_relation(&self) -> bool {
		matches!(self.kind, TableType::Relation(_) | TableType::Any)
	}

	pub fn allows_normal(&self) -> bool {
		matches!(self.kind, TableType::Normal | TableType::Any)
	}
}

fn get_tables_from_kind(tables: &[Table]) -> String {
	tables.iter().map(|t| t.0.as_str()).collect::<Vec<_>>().join(" | ")
}

impl Display for DefineTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TABLE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {}", self.name)?;
		write!(f, " TYPE")?;
		match &self.kind {
			TableType::Normal => {
				f.write_str(" NORMAL")?;
			}
			TableType::Relation(rel) => {
				f.write_str(" RELATION")?;
				if let Some(Kind::Record(kind)) = &rel.from {
					write!(f, " IN {}", get_tables_from_kind(kind))?;
				}
				if let Some(Kind::Record(kind)) = &rel.to {
					write!(f, " OUT {}", get_tables_from_kind(kind))?;
				}
			}
			TableType::Any => {
				f.write_str(" ANY")?;
			}
		}
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

impl InfoStructure for DefineTableStatement {
	fn structure(self) -> Value {
		let Self {
			name,
			drop,
			full,
			view,
			permissions,
			changefeed,
			comment,
			kind,
			..
		} = self;
		let mut acc = Object::default();

		acc.insert("name".to_string(), name.structure());

		acc.insert("drop".to_string(), drop.into());
		acc.insert("full".to_string(), full.into());

		if let Some(view) = view {
			acc.insert("view".to_string(), view.structure());
		}

		acc.insert("permissions".to_string(), permissions.structure());

		if let Some(changefeed) = changefeed {
			acc.insert("changefeed".to_string(), changefeed.structure());
		}

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		acc.insert("kind".to_string(), kind.structure());

		Value::Object(acc)
	}
}
