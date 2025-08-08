use crate::catalog::NamespaceDefinition;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Ident, Strand, Value};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::ToSql;
use anyhow::{Result, bail};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineNamespaceStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
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
		let namespace_id = if let Some(ns) = txn.get_ns_by_name(&self.name).await? {
			if self.if_not_exists {
				return Ok(Value::None);
			}

			if !self.overwrite && !opt.import {
				bail!(Error::NsAlreadyExists {
					name: self.name.to_string(),
				});
			}
			ns.namespace_id
		} else {
			txn.lock().await.get_next_ns_id().await?
		};

		// Process the statement
		let catalog_key = crate::key::catalog::ns::new(&self.name);
		let ns_def = NamespaceDefinition {
			namespace_id,
			name: self.name.to_string(),
			comment: self.comment.clone().map(|c| c.to_string()),
		};
		txn.set(&catalog_key, &ns_def, None).await?;

		let key = crate::key::root::ns::new(namespace_id);
		txn.set(&key, &ns_def, None).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE NAMESPACE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v.to_sql())?
		}
		Ok(())
	}
}
