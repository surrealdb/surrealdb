use std::fmt::{self, Display};

use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::DefineKind;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Ident};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DefineNamespaceStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
}

impl_kv_value_revisioned!(DefineNamespaceStatement);

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
		if txn.get_ns(&self.name).await.is_ok() {
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
		}
		// Process the statement
		let key = crate::key::root::ns::new(&self.name);
		txn.set(
			&key,
			&DefineNamespaceStatement {
				id: match self.id {
					Some(id) => Some(id),
					None => Some(txn.lock().await.get_next_ns_id().await?),
				},
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

impl InfoStructure for DefineNamespaceStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
