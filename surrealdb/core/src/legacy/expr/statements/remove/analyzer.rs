use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{Error, Index};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::remove::analyzer::RemoveAnalyzerStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::AnalyzerKey;
use crate::legacy::expr_to_ident;
use crate::val::Value;

pub(crate) async fn remove_analyzer_statement_compute(
	this: &RemoveAnalyzerStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Analyzer, Base::Db)?;
	// Compute the name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "analyzer name").await?;
	// Get the transaction
	let txn = ctx.tx();
	// Get the definition
	let az = txn.get_db_analyzer(ns, db, &name, None).await;
	let az = match az {
		Ok(x) => x,
		Err(e) => {
			if this.if_exists && matches!(e.downcast_ref(), Some(Error::AzNotFound { .. })) {
				return Ok(Value::None);
			} else {
				return Err(e);
			}
		}
	};
	// Full-text indexes load their analyzer via `FullTextIndex::new`, which
	// fails with AzNotFound if the analyzer is gone — refuse removal while
	// any index still references it.
	for tb in txn.all_tb(ns, db, None).await?.iter() {
		let tb_name = tb.name.clone();
		for ix in txn.all_tb_indexes(ns, db, &tb_name, None).await?.iter() {
			if let Index::FullText(p) = &ix.index
				&& p.analyzer.as_str() == az.name.as_str()
			{
				return Err(anyhow::Error::new(Error::AzInUse {
					name: az.name.to_string(),
					table: tb.name.to_string(),
					index: ix.name.to_string(),
				}));
			}
		}
	}
	// Delete the definition
	let key = AnalyzerKey {
		ns,
		db,
		az: std::borrow::Cow::Borrowed(&az.name),
	};
	txn.del_key(&key).await?;
	// Clear the cache
	txn.clear_cache();
	// Cleanup in-memory mappers if not used anymore
	let azs = txn.all_db_analyzers(ns, db, None).await?;
	ctx.get_index_stores().mappers().cleanup(&azs);
	// Ok all good
	Ok(Value::None)
}
