mod bkeys;
mod btree;
mod ft;
mod kvsim;

use derive::Key;
use serde::{Deserialize, Serialize};

type IndexId = u64;

#[derive(Debug, Serialize, Deserialize, Key)]
struct BaseStateKey {
	domain: Domain,
	index_id: IndexId,
}

type Domain = u8;

const INDEX_DOMAIN: u8 = 0x00;
const DOC_IDS_DOMAIN: u8 = 0x10;
const DOC_KEYS_DOMAIN: u8 = 0x11;
const TERMS_DOMAIN: u8 = 0x20;
const DOC_LENGTHS_DOMAIN: u8 = 0x30;
const POSTING_DOMAIN: u8 = 0x40;

impl BaseStateKey {
	fn new(domain: u8, index_id: u64) -> Self {
		Self {
			domain,
			index_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::bkeys::KeyVisitor;
	use crate::idx::btree::Payload;
	use crate::idx::kvsim::KVSimulator;
	use crate::kvs::Key;
	use std::collections::HashMap;

	#[derive(Default)]
	pub(super) struct HashVisitor {
		map: HashMap<Key, Payload>,
	}

	impl KeyVisitor for HashVisitor {
		fn visit(&mut self, _kv: &mut KVSimulator, key: Key, payload: Payload) {
			self.map.insert(key, payload);
		}
	}

	impl HashVisitor {
		pub(super) fn check_len(&self, len: usize, info: &str) {
			assert_eq!(self.map.len(), len, "len issue: {}", info);
		}
		pub(super) fn check(&self, res: Vec<(Key, Payload)>, info: &str) {
			self.check_len(res.len(), info);
			for (k, p) in res {
				assert_eq!(self.map.get(&k), Some(&p));
			}
		}
	}
}
