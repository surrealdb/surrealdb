use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::BucketProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveBucketStatement {
	pub name: Expr,
	pub if_exists: bool,
}

impl VisitExpression for RemoveBucketStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.name.visit(visitor);
	}
}
impl Default for RemoveBucketStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl RemoveBucketStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Bucket, &Base::Db)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "bucket name").await?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let Some(bu) = txn.get_db_bucket(ns, db, &name).await? else {
			if self.if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::BuNotFound {
					name,
				}
				.into());
			}
		};

		// Delete the definition
		let key = crate::key::database::bu::new(ns, db, &bu.name);
		txn.del(&key).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveBucketStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE BUCKET")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
