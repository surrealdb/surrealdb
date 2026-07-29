//! DiskANN pending-operation state key.

use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::idx::trees::diskann::DiskAnnPendingState;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	/// Stores one shard of the distributed-safe pending-operation summary for one DiskANN index.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dp<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'p',
		pub shard: u16,
	}
}

impl_kv_key_storekey!(Dp<'a> => DiskAnnPendingState);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Dp {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			shard: 7,
		};
		let enc = Dp::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dp\0\x07"
		);
	}
}
