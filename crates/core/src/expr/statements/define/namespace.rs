use std::fmt::{self, Display};

use anyhow::{Result, bail};

use super::DefineKind;
use crate::catalog::NamespaceDefinition;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Ident};
use crate::iam::{Action, ResourceKind};
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineNamespaceStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
}

impl DefineNamespaceStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let namespace_id = if let Some(ns) = txn.get_ns_by_name(&self.name).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::NsAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
			ns.namespace_id
		} else {
			txn.lock().await.get_next_ns_id().await?
		};

		// Process the statement
		let ns_def = NamespaceDefinition {
			namespace_id,
			name: self.name.to_raw_string(),
			comment: self.comment.clone().map(|c| c.into_string()),
		};
		txn.put_ns(ns_def).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE NAMESPACE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}
