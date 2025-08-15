use std::fmt::{self, Write};

use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::DefineKind;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Ident, Permission};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DefineModelStatement {
	pub kind: DefineKind,
	pub hash: String,
	pub name: Ident,
	pub version: String,
	pub comment: Option<Strand>,
	pub permissions: Permission,
}

impl_kv_value_revisioned!(DefineModelStatement);

impl DefineModelStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
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
							name: model.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}

		// Process the statement
		let key = crate::key::database::ml::new(ns, db, &self.name, &self.version);
		txn.set(
			&key,
			&DefineModelStatement {
				// Don't persist the `IF NOT EXISTS` clause to schema
				kind: DefineKind::Default,
				..self.clone()
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

impl fmt::Display for DefineModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "DEFINE MODEL")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " ml::{}<{}>", self.name, self.version)?;
		if let Some(comment) = self.comment.as_ref() {
			write!(f, " COMMENT {}", comment)?;
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

impl InfoStructure for DefineModelStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"version".to_string() => self.version.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
