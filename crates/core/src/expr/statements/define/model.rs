use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{MlModelDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Expr, FlowResultExt};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineModelStatement {
	pub kind: DefineKind,
	pub hash: String,
	pub name: String,
	pub version: String,
	pub comment: Expr,
	pub permissions: Permission,
}

impl DefineModelStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Model, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		if let Some(model) = txn.get_db_model(ns, db, &self.name, &self.version).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::MlAlreadyExists {
							name: model.name.clone(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}

		let comment = stk
			.run(|stk| self.comment.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to()?;

		// Process the statement
		let key = crate::key::database::ml::new(ns, db, &self.name, &self.version);
		txn.set(
			&key,
			&MlModelDefinition {
				hash: self.hash.clone(),
				name: self.name.clone(),
				version: self.version.clone(),
				comment,
				permissions: self.permissions.clone(),
			},
			None,
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for DefineModelStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::define::DefineModelStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
