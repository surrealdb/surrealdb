use std::borrow::Cow;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{Error as CatalogError, MlModelDefinition, Permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::expr::{Base, Expr, FlowResultExt};
use crate::iam::{Action, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineModelStatement {
	pub kind: DefineKind,
	pub hash: Strand,
	pub name: Strand,
	pub version: Strand,
	pub comment: Expr,
	pub permissions: Permission,
}

impl DefineModelStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "DefineModelStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Model, Base::Db)?;
		// A PERMISSIONS clause must not perform writes (GHSA-66r2-5gwj-gxm2).
		if self.permissions.has_direct_write() {
			bail!(ExecError::PermissionClauseNotReadonly {
				kind: "model",
				name: self.name.to_string(),
			});
		}
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		if let Some(model) = txn.get_db_model(ns, db, &self.name, &self.version, None).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(CatalogError::MlAlreadyExists {
							name: model.name.to_string(),
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
		let key = crate::key::database::ml::Ml {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			ml: Cow::Borrowed(&self.name),
			vn: Cow::Borrowed(&self.version),
		};
		txn.set_key(
			&key,
			&MlModelDefinition {
				hash: self.hash.clone(),
				name: self.name.clone(),
				version: self.version.clone(),
				comment,
				permissions: self.permissions.clone(),
			}
			.to_stored(),
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
