use std::fmt::{self, Write};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{ModuleDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::{Base, Expr};
use crate::fmt::{is_pretty, pretty_indent};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

mod executable;
pub(crate) use executable::*;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineModuleStatement {
	pub kind: DefineKind,
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	pub comment: Option<Expr>,
	pub permissions: Permission,
}

impl VisitExpression for DefineModuleStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.comment.iter().for_each(|comment| comment.visit(visitor));
	}
}

impl DefineModuleStatement {
	fn get_storage_name(&self) -> Result<String> {
		if let Some(name) = &self.name {
			Ok(format!("mod::{}", name))
		} else if let ModuleExecutable::Silo(silo) = &self.executable {
			Ok(format!(
				"silo::{}::{}<{}.{}.{}>",
				silo.organisation, silo.package, silo.major, silo.minor, silo.patch
			))
		} else {
			bail!("A module without a name cannot be stored")
		}
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		let storage_name = self.get_storage_name()?;
		if txn.get_db_module(ns, db, &storage_name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::FcAlreadyExists {
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
			txn.get_or_add_db(Some(ctx), ns, db, opt.strict).await?
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

impl fmt::Display for DefineModuleStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE MODULE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		if let Some(name) = &self.name {
			write!(f, " mod::{name} AS")?;
		}
		write!(f, " {}", self.executable)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}
