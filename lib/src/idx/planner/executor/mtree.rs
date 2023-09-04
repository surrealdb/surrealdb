use crate::dbs::Options;
use crate::err::Error;
use crate::idx::docids::DocIds;
use crate::idx::planner::executor::{IteratorRef, QueryExecutor};
use crate::idx::planner::iterators::{KnnThingIterator, ThingIterator};
use crate::idx::planner::plan::{IndexOption, Lookup};
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::trees::store::TreeStoreType;
use crate::idx::IndexKeyBase;
use crate::kvs;
use crate::sql::index::MTreeParams;
use crate::sql::{Array, Expression};
use roaring::RoaringTreemap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

impl QueryExecutor {
	pub(super) async fn check_mtree_entry(
		&mut self,
		opt: &Options,
		run: &mut kvs::Transaction,
		io: &IndexOption,
		exp: Expression,
		p: &MTreeParams,
	) -> Result<(), Error> {
		if let Lookup::MtKnn {
			a,
			k,
		} = io.lo()
		{
			let ixn = &io.ix().name.0;
			let entry = if let Some(mt) = self.mt_map.get(ixn) {
				MtEntry::new(run, mt, a, *k).await?
			} else {
				let ikb = IndexKeyBase::new(opt, io.ix());
				let mt = MTreeIndex::new(run, ikb, p, TreeStoreType::Read).await?;
				let ixn = ixn.to_owned();
				let entry = MtEntry::new(run, &mt, a, *k).await?;
				self.mt_map.insert(ixn, mt);
				entry
			};
			self.mt_exp.insert(exp, entry);
		}
		Ok(())
	}

	pub(super) fn new_mtree_index_knn_iterator(&self, ir: IteratorRef) -> Option<ThingIterator> {
		if let Some(exp) = self.iterators.get(ir as usize) {
			if let Some(mte) = self.mt_exp.get(exp) {
				let it = KnnThingIterator::new(mte.doc_ids.clone(), mte.res.clone());
				return Some(ThingIterator::Knn(it));
			}
		}
		None
	}
}

#[derive(Clone)]
pub(super) struct MtEntry {
	doc_ids: Arc<RwLock<DocIds>>,
	res: VecDeque<RoaringTreemap>,
}

impl MtEntry {
	async fn new(
		tx: &mut kvs::Transaction,
		mt: &MTreeIndex,
		a: &Array,
		k: u32,
	) -> Result<Self, Error> {
		let res = mt.knn_search(tx, a, k as usize).await?;
		Ok(Self {
			res,
			doc_ids: mt.doc_ids(),
		})
	}
}
