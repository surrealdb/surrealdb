use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::idx::ft::FtIndex;
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::trees::store::TreeStoreType;
use crate::idx::IndexKeyBase;
use crate::sql::ident::Ident;
use crate::sql::index::Index;
use crate::sql::value::Value;
use crate::sql::Base;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub enum AnalyzeStatement {
	Idx(Ident, Ident),
}

impl AnalyzeStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self {
			AnalyzeStatement::Idx(tb, idx) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Index, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Read the index
				let ix = run
					.get_and_cache_tb_index(opt.ns(), opt.db(), tb.as_str(), idx.as_str())
					.await?;
				let ikb = IndexKeyBase::new(opt, &ix);

				// Index operation dispatching
				let value: Value = match &ix.index {
					Index::Search(p) => {
						let az = run.get_db_analyzer(opt.ns(), opt.db(), p.az.as_str()).await?;
						let ft =
							FtIndex::new(&mut run, az, ikb, p, TreeStoreType::Traversal).await?;
						ft.statistics(&mut run).await?.into()
					}
					Index::MTree(p) => {
						let mt =
							MTreeIndex::new(&mut run, ikb, p, TreeStoreType::Traversal).await?;
						mt.statistics(&mut run).await?.into()
					}
					_ => {
						return Err(Error::FeatureNotYetImplemented {
							feature: "Statistics on unique and non-unique indexes.".to_string(),
						})
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
