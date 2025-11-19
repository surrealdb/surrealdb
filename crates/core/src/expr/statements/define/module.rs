use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{ModuleDefinition, ModuleName, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Expr, ModuleExecutable};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineModuleStatement {
	pub kind: DefineKind,
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	pub comment: Option<Expr>,
	pub permissions: Permission,
}

impl DefineModuleStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Module, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		let storage_name = ModuleName::try_from(self)?.get_storage_name();
		if txn.get_db_module(ns, db, &storage_name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::MdAlreadyExists {
							name: storage_name,
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}
		// Process the statement
		{
			let (ns, db) = opt.ns_db()?;
			txn.get_or_add_db(Some(ctx), ns, db).await?
		};

		txn.put_db_module(
			ns,
			db,
			&ModuleDefinition {
				name: self.name.clone(),
				executable: self.executable.clone().into(),
				comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
				permissions: self.permissions.clone(),
			},
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
