use std::fmt::{self, Display};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::{CursorDoc, DefineKind};
use crate::buc::{self, BucketConnectionKey};
use crate::catalog::providers::BucketProvider;
use crate::catalog::{BucketDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, FlowResultExt, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineBucketStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub backend: Option<Expr>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Option<Expr>,
}

impl VisitExpression for DefineBucketStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.name.visit(visitor);
		self.backend.iter().for_each(|action| action.visit(visitor));
		self.comment.iter().for_each(|expr| expr.visit(visitor));
	}
}

impl Default for DefineBucketStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			backend: None,
			permissions: Permission::default(),
			readonly: false,
			comment: None,
		}
	}
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
		// Process the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "bucket name").await?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Check if the definition exists
		if let Some(bucket) = txn.get_db_bucket(ns, db, &name).await? {
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
			name: name.clone(),
			backend,
			permissions: self.permissions.clone(),
			readonly: self.readonly,
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
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
			write!(f, " COMMENT {}", comment)?;
		}

		Ok(())
	}
}
