use std::borrow::Cow;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::Error;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::remove::sequence::RemoveSequenceStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::KVRange;
use crate::key::database::all::DatabaseRoot;
use crate::key::database::sq::Sq;
use crate::key::sequence::{BaPrefix, StPrefix};
use crate::legacy::expr_to_ident;
use crate::val::Value;

pub(crate) async fn remove_sequence_statement_compute(
	this: &RemoveSequenceStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Sequence, Base::Db)?;
	// Compute the name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "sequence name").await?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;

	// Get the transaction
	let txn = ctx.tx();

	// Get the definition
	let sq = match txn.get_db_sequence(ns, db, &name, None).await {
		Ok(x) => x,
		Err(e) => {
			if this.if_exists && matches!(e.downcast_ref(), Some(Error::SeqNotFound { .. })) {
				return Ok(Value::None);
			} else {
				return Err(e);
			}
		}
	};
	// Remove the sequence
	if let Some(seq) = ctx.get_sequences() {
		seq.sequence_removed(ns, db, &name).await;
	}
	// Delete any sequence records
	let ba_range = BaPrefix {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		sq: Cow::Borrowed(&sq.name),
	};
	txn.delr(ba_range.encode_range()?).await?;
	let st_range = StPrefix {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		sq: Cow::Borrowed(&sq.name),
	};
	txn.delr(st_range.encode_range()?).await?;
	// Delete the definition
	let key = Sq {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		sq: Cow::Borrowed(name.as_str()),
	};
	txn.del_key(&key).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
