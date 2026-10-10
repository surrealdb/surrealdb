//! Doc-ID kernels: the key a doc-ID's record is stored under, and the record
//! key the stored bytes name.
//!
//! An index stores postings and entries under doc-IDs, and turns a doc-ID back
//! into a record with the table's reverse mapping: one key per doc-ID, under
//! the table's doc-ID keyspace, whose value is the record's key. A caller reads
//! those keys with its own keyspace read, from the batch of keys
//! [`record_key`] builds, and decodes each present value with
//! [`decode_record_key`]. A doc-ID with no value names no record.
//!
//! Both kernels are pure: they never read the mapping, so a read path sees
//! every key it reads and can batch them.

use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::docids::DocId;
use crate::key::schema::DocKeyKey;
use crate::key::{KVKey, KVValue, Key};
use crate::val::{RecordIdKey, TableName};

/// The key of the reverse mapping of `doc_id` in table `tb` of database `db`
/// in namespace `ns`.
pub fn record_key(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	doc_id: DocId,
) -> Result<Key<'static>> {
	DocKeyKey::new(ns, db, Cow::Borrowed(tb), doc_id).encode_key()
}

/// The record key a reverse mapping's stored bytes name.
pub fn decode_record_key(bytes: &[u8]) -> Result<RecordIdKey> {
	RecordIdKey::kv_decode_value(bytes, ())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_mapping_decodes_to_the_record_key_it_stores() {
		for key in [RecordIdKey::from(7), RecordIdKey::from("a".to_owned())] {
			let bytes = key.kv_encode_value().unwrap();
			assert_eq!(decode_record_key(&bytes).unwrap(), key);
		}
	}

	#[test]
	fn a_doc_id_is_keyed_within_its_table_only() {
		let (ns, db) = (NamespaceId(1), DatabaseId(2));
		let (a, b) = (TableName::from("a"), TableName::from("b"));
		let key = record_key(ns, db, &a, 5).unwrap();
		assert_ne!(key, record_key(ns, db, &b, 5).unwrap());
		assert_ne!(key, record_key(ns, db, &a, 6).unwrap());
		assert_eq!(key, record_key(ns, db, &a, 5).unwrap());
	}
}
