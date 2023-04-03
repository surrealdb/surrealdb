mod bkeys;
mod btree;
mod ft;
mod kvsim;

use derive::Key;
use serde::{Deserialize, Serialize};

type IndexId = u64;

#[derive(Serialize, Deserialize, Key)]
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
