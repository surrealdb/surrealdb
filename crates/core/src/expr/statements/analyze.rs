use std::fmt;
use std::fmt::{Display, Formatter};

use anyhow::{Result, bail};

use crate::catalog::Index;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::Base;
use crate::expr::ident::Ident;
use crate::iam::{Action, ResourceKind};
use crate::idx::IndexKeyBase;
use crate::idx::ft::search::SearchIndex;
use crate::idx::trees::mtree::MTreeIndex;
use crate::kvs::TransactionType;
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AnalyzeStatement {
	Idx(Ident, Ident),
}

impl AnalyzeStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		match self {
			AnalyzeStatement::Idx(tb, idx) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Index, &Base::Db)?;
				// Read the index
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				let ix = ctx.tx().get_tb_index(ns, db, tb, idx).await?;
				let ikb = IndexKeyBase::new(ns, db, &ix.what, &ix.name);
				// Index operation dispatching
				let value: Value = match &ix.index {
					Index::Search(p) => {
						let ft = SearchIndex::new(
							ctx,
							ns,
							db,
							p.az.as_str(),
							ikb,
							p,
							TransactionType::Read,
						)
						.await?;
						ft.statistics(ctx).await?.into()
					}
					Index::MTree(p) => {
						let tx = ctx.tx();
						let mt = MTreeIndex::new(&tx, ikb, p, TransactionType::Read).await?;
						mt.statistics(&tx).await?.into()
					}
					_ => {
						bail!(Error::Unimplemented(
							"Statistics on unique and non-unique indexes.".to_string()
						))
					}
				};
				// Return the result object
				Ok(value)
			}
		}
	}
}

impl Display for AnalyzeStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Idx(tb, idx) => write!(f, "ANALYZE INDEX {idx} ON {tb}"),
		}
	}
}
