//! DiskANN sharded pending-state guard key.
//!
//! This is the `!dw` (sharded) analogue of [`crate::key::index::dp`] (`!dp`). New writes track
//! their pending state here, keyed by the writer's shard, so it is decoupled from the legacy `!dp`
//! guard. A pre-change node's compactor only ever clears `!dp` (it has no knowledge of
//! `!dw`/`!dy`), so keeping the sharded guard in its own family means an old compactor can never
//! mark a shard empty while a `!dw` entry it cannot see still exists — which would otherwise hide
//! that record from upgraded lookups during a mixed-version rolling upgrade.

use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::idx::trees::diskann::DiskAnnPendingState;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	/// Stores one shard of the sharded (`!dw`) pending-operation summary for one DiskANN index.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dy<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'y',
		pub shard: u16,
	}
}

impl_kv_key_storekey!(Dy<'a> => DiskAnnPendingState);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;
	use crate::key::index::dp::Dp;

	#[test]
	fn sharded_guard_key_is_distinct_from_legacy_guard() {
		let tb = TableName::from("testtb");
		let dy = Dy {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			shard: 7,
		}
		.encode_key()
		.unwrap();

		let dp = Dp {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			shard: 7,
		}
		.encode_key()
		.unwrap();
		// The sharded guard must not collide with the legacy guard an old compactor clears.
		assert_ne!(dy, dp);
	}
}
