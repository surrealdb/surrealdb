use crate::ctx::Context;
use crate::dbs::{Iterable, Processed};
use crate::kvs::Key;
use radix_trie::Trie;
use std::default::Default;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::Mutex;

// TODO: This is currently processed in memory. In the future is should be on disk (mmap?)
type Distinct = Trie<Key, bool>;

#[derive(Default)]
pub(crate) struct SyncDistinct {
	processed: Distinct,
}

impl SyncDistinct {
	pub(super) fn new(ctx: &Context<'_>) -> Option<Self> {
		if let Some(pla) = ctx.get_query_planner() {
			if pla.requires_distinct() {
				return Some(Self::default());
			}
		}
		None
	}

	fn is_distinct(ctx: &Context<'_>, i: &Iterable) -> bool {
		if let Iterable::Index(t, ir, _) = i {
			if let Some(pla) = ctx.get_query_planner() {
				if let Some(exe) = pla.get_query_executor(&t.0) {
					return exe.is_distinct(*ir);
				}
			}
		}
		false
	}

	pub(super) fn requires_distinct<'a>(
		ctx: &Context<'_>,
		dis: Option<&'a mut SyncDistinct>,
		i: &Iterable,
	) -> Option<&'a mut SyncDistinct> {
		if dis.is_some() && Self::is_distinct(ctx, i) {
			dis
		} else {
			None
		}
	}

	pub(super) fn check_already_processed(&mut self, pro: &Processed) -> bool {
		if let Some(key) = pro.rid.as_ref().map(|rid| rid.to_vec()) {
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

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default, Clone)]
pub(crate) struct AsyncDistinct {
	processed: Arc<Mutex<SyncDistinct>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl AsyncDistinct {
	pub(super) fn new(ctx: &Context<'_>) -> Option<Self> {
		if let Some(pla) = ctx.get_query_planner() {
			if pla.requires_distinct() {
				return Some(Self::default());
			}
		}
		None
	}

	pub(super) fn requires_distinct(
		ctx: &Context<'_>,
		dis: Option<&AsyncDistinct>,
		i: &Iterable,
	) -> Option<AsyncDistinct> {
		if let Some(dis) = dis {
			if SyncDistinct::is_distinct(ctx, i) {
				return Some(dis.clone());
			}
		}
		None
	}

	pub(super) async fn check_already_processed(&self, pro: &Processed) -> bool {
		self.processed.lock().await.check_already_processed(pro)
	}
}
