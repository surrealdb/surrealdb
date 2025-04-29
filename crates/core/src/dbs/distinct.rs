use std::default::Default;

use radix_trie::Trie;

use crate::ctx::Context;
use crate::dbs::Processed;
use crate::kvs::Key;

// TODO: This is currently processed in memory. In the future is should be on
// disk (mmap?)
type Distinct = Trie<Key, bool>;

#[derive(Default)]
pub(crate) struct SyncDistinct {
	processed: Distinct,
}

impl SyncDistinct {
	pub(super) fn new(ctx: &Context) -> Option<Self> {
		if let Some(pla) = ctx.get_query_planner() {
			if pla.requires_distinct() {
				return Some(Self::default());
			}
		}
		None
	}

	pub(super) fn check_already_processed(&mut self, pro: &Processed) -> bool {
		// If the serialization failed we couldn't have processed it.
		if let Some(key) = pro.rid.as_ref().and_then(|r| revision::to_vec(&**r).ok()) {
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
