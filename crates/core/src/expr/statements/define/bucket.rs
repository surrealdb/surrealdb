use std::fmt::{self, Display};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::{CursorDoc, DefineKind};
use crate::buc::{self, BucketConnectionKey};
use crate::catalog::{BucketDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Expr, FlowResultExt, Ident};
use crate::iam::{Action, ResourceKind};
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineBucketStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub backend: Option<Expr>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Option<Strand>,
}

impl DefineBucketStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Bucket, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Check if the definition exists
		if let Some(bucket) = txn.get_db_bucket(ns, db, &self.name).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::BuAlreadyExists {
							value: bucket.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}
		// Process the backend input
		let backend = if let Some(ref url) = self.backend {
			Some(
				stk.run(|stk| url.compute(stk, ctx, opt, doc))
					.await
					.catch_return()?
					.coerce_to::<String>()?,
			)
		} else {
			None
		};

		// Validate the store
		let name = self.name.to_string();
		let store = if let Some(ref backend) = backend {
			buc::connect(backend, false, self.readonly).await?
		} else {
			buc::connect_global(ns, db, &name).await?
		};

		// Persist the store to cache
		if let Some(buckets) = ctx.get_buckets() {
			let key = BucketConnectionKey::new(ns, db, &name);
			buckets.insert(key, store);
		}

		// Process the statement
		let key = crate::key::database::bu::new(ns, db, &name);
		let ap = BucketDefinition {
			id: None,
			name: self.name.to_raw_string(),
			backend,
			permissions: self.permissions.clone(),
			readonly: self.readonly,
			comment: self.comment.as_ref().map(|c| c.to_raw_string()),
		};
		txn.set(&key, &ap, None).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineBucketStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE BUCKET")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;

		if self.readonly {
			write!(f, " READONLY")?;
		}

		if let Some(ref backend) = self.backend {
			write!(f, " BACKEND {backend}")?;
		}

		write!(f, " PERMISSIONS {}", self.permissions)?;

		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {comment}")?;
		}

		Ok(())
	}
}
