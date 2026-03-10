use std::default::Default;

use radix_trie::Trie;

use crate::ctx::FrozenContext;
use crate::dbs::Processable;
use crate::kvs::Key;

// TODO: This is currently processed in memory. In the future is should be on
// disk (mmap?)
type Distinct = Trie<Key, bool>;

#[derive(Default)]
pub(crate) struct SyncDistinct {
	processed: Distinct,
}

impl SyncDistinct {
	pub(super) fn new(ctx: &FrozenContext) -> Option<Self> {
		if let Some(pla) = ctx.get_query_planner()
			&& pla.requires_distinct()
		{
			return Some(Self::default());
		}
		None
	}

	pub(super) fn check_already_processed(&mut self, pro: &Processable) -> bool {
		// If the serialization failed we couldn't have processed it.
		if let Some(key) = pro.rid.as_ref().and_then(|r| storekey::encode_vec(&**r).ok()) {
			if self.processed.get(&key).is_some() {
				true
			} else {
				self.processed.insert(key, true);
				false
			}
		} else {
			false
		}
	}
}
