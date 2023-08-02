use crate::dbs::Options;
use crate::err::Error;
use crate::idx::docids::DocIds;
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::plan::IndexOption;
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::trees::store::TreeStoreType;
use crate::idx::IndexKeyBase;
use crate::kvs;
use crate::sql::index::MTreeParams;
use crate::sql::Expression;
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
		let ixn = &io.ix().name.0;
		let entry = if let Some(mt) = self.mt_map.get(ixn) {
			MtEntry::new(mt, io.clone())
		} else {
			let ikb = IndexKeyBase::new(opt, io.ix());
			let mt = MTreeIndex::new(run, ikb, p, TreeStoreType::Read).await?;
			let ixn = ixn.to_owned();
			let entry = MtEntry::new(&mt, io.clone());
			self.mt_map.insert(ixn, mt);
			entry
		};
		self.mt_exp.insert(exp, entry);
		Ok(())
	}
}

#[derive(Clone)]
pub(super) struct MtEntry {
	_index_option: IndexOption,
	_doc_ids: Arc<RwLock<DocIds>>,
}

impl MtEntry {
	fn new(mt: &MTreeIndex, io: IndexOption) -> Self {
		Self {
			_index_option: io,
			_doc_ids: mt.doc_ids(),
		}
	}
}
