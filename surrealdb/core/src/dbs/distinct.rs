use std::default::Default;

use ahash::HashSet;

use crate::ctx::FrozenContext;
use crate::dbs::Processable;

// TODO: This is currently processed in memory. In the future is should be on
// disk (mmap?)
type Distinct = HashSet<Vec<u8>>;

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
			// `insert` returns false when the key was already present, i.e. this
			// record has already been processed.
			!self.processed.insert(key)
		} else {
			false
		}
	}
}
