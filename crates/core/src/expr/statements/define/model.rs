use std::fmt::{self, Write};

use anyhow::{Result, bail};

use super::DefineKind;
use crate::catalog::{MlModelDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::{Base, Ident};
use crate::iam::{Action, ResourceKind};
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineModelStatement {
	pub kind: DefineKind,
	pub hash: String,
	pub name: Ident,
	pub version: String,
	pub comment: Option<Strand>,
	pub permissions: Permission,
}

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
			&MlModelDefinition {
				hash: self.hash.clone(),
				name: self.name.to_raw_string(),
				version: self.version.clone(),
				comment: self.comment.clone().map(|x| x.to_raw_string()),
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
