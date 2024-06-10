use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::idx::ft::FtIndex;
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::IndexKeyBase;
use crate::kvs::TransactionType;
use crate::sql::ident::Ident;
use crate::sql::index::Index;
use crate::sql::value::Value;
use crate::sql::Base;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AnalyzeStatement {
	Idx(Ident, Ident),
}

impl AnalyzeStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self {
			AnalyzeStatement::Idx(tb, idx) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Index, &Base::Db)?;
				// Read the index
				let ix = ctx.tx().get_tb_index(opt.ns()?, opt.db()?, &tb, &idx).await?;
				let ikb = IndexKeyBase::new(opt.ns()?, opt.db()?, &ix)?;
				// Index operation dispatching
				let value: Value = match &ix.index {
					Index::Search(p) => {
						let ft =
							FtIndex::new(ctx, opt, p.az.as_str(), ikb, p, TransactionType::Read)
								.await?;
						ft.statistics(ctx).await?.into()
					}
					Index::MTree(p) => {
						let tx = ctx.tx();
						let mt = MTreeIndex::new(
							ctx.get_index_stores(),
							&tx,
							ikb,
							p,
							TransactionType::Read,
						)
						.await?;
						mt.statistics(&tx).await?.into()
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
