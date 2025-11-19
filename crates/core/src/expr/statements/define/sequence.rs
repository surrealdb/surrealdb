use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::SequenceDefinition;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Timeout, Value};
use crate::iam::{Action, ResourceKind};
use crate::key::database::sq::Sq;
use crate::key::sequence::Prefix;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineSequenceStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub batch: Expr,
	pub start: Expr,
	pub timeout: Option<Timeout>,
}

impl Default for DefineSequenceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			batch: Expr::Literal(Literal::Integer(0)),
			start: Expr::Literal(Literal::Integer(0)),
			timeout: None,
		}
	}
}

impl DefineSequenceStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;
		// Compute name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "sequence name").await?;
		// Compute timeout
		let timeout = map_opt!(x as &self.timeout => x.compute(stk, ctx, opt, doc).await?.0);
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Check if the definition exists
		if txn.get_db_sequence(ns, db, &name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::SeqAlreadyExists {
							name: name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}

		let db = {
			let (ns, db) = opt.ns_db()?;
			txn.get_or_add_db(Some(ctx), ns, db).await?
		};

		// Process the statement
		let key = Sq::new(db.namespace_id, db.database_id, &name);
		let sq = SequenceDefinition {
			name: name.clone(),
			batch: compute_to!(stk, ctx, opt, doc, self.batch => i64)
				.try_into()
				.map_err(|_| anyhow::anyhow!("batch must be a u32"))?,
			start: compute_to!(stk, ctx, opt, doc, self.start => i64),
			timeout,
		};
		// Set the definition
		txn.set(&key, &sq, None).await?;

		// Clear any pre-existing sequence records
		let ba_range = Prefix::new_ba_range(db.namespace_id, db.database_id, &sq.name)?;
		txn.delr(ba_range).await?;
		let st_range = Prefix::new_st_range(db.namespace_id, db.database_id, &sq.name)?;
		txn.delr(st_range).await?;

		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
