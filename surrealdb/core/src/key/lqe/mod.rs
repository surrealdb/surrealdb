//! Stores live-query change events in a dedicated keyspace.
//!
//! This is intentionally separate from the user-facing changefeed
//! (`crate::key::change`, section marker `#`). It uses the same big-endian
//! versionstamp ordering, but the section marker `%` keeps it out of every
//! changefeed range scan and `SHOW CHANGES`, and lets live queries have their
//! own value format, `store_diff` policy, and retention. See [`crate::lq`].
use std::borrow::Cow;

use anyhow::Result;
use surrealdb_strand::TableName;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::lq::event::LiveEvents;

key! {
	// Lqe stands for live-query event.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Lqe<'a> {
		pub prefix: DatabaseRoot,
		b'%',
		// ts is the commit versionstamp, encoded big-endian.
		pub ts: Cow<'a, [u8]>,
		b'*',
		pub tb: Cow<'a, TableName>,
	}
}
impl_kv_key_storekey!(Lqe<'a> => LiveEvents);

impl Categorise for Lqe<'_> {
	fn categorise(&self) -> Category {
		Category::LiveQueryEvent
	}
}

key! {
	/// A prefix for the database's live-query events at/since a specific timestamp.
	/// Used to build range scans (e.g. for garbage collection and, later, the
	/// router's cursor reads).
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub struct LqeTsRange<'a> {
			pub prefix: DatabaseRoot,
			b'%',
			// ts is the commit versionstamp, encoded big-endian.
			pub ts: Cow<'a, [u8]>,
	}
}

impl_kv_range_storekey!(LqeTsRange<'_>);

key! {
	/// Upper bound for scanning a database's live-query event section. The router's
	/// tail reader scans [`prefix_ts`]`(cursor)..`[`suffix`] to read every event
	/// since its cursor; `0xff` sorts after any encoded `ts`/`tb` entry for the
	/// database.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub struct LqePrefix {
		pub prefix: DatabaseRoot,
		b'%',
	}
}

impl_kv_range_storekey!(LqePrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::change::ChangeFeedPrefix;
	use crate::key::database::all::DatabaseRoot;
	use crate::key::{KVKey, KVRange};
	use crate::kvs::{HlcTimeStampImpl, TimeStampImpl};

	#[test]
	fn lqe_key_uses_percent_section_marker() {
		let ts_impl = HlcTimeStampImpl;
		let buf = &mut [0u8; _];
		let ts = ts_impl.create_from_versionstamp(12345).unwrap().encode(buf);
		let tb = TableName::from("test");
		let enc = Lqe {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ts: Cow::Borrowed(ts),
			tb: Cow::Borrowed(&tb),
		}
		.encode_key()
		.unwrap();
		// Byte layout: '/' '*' <ns:4> '*' <db:4> '%' ...
		assert_eq!(&enc[0..2], b"/*");
		assert_eq!(enc[11], b'%', "section marker must be '%', distinct from changefeed '#'");
	}

	#[test]
	fn lqe_range_is_disjoint_from_changefeed_range() {
		// The changefeed range for a db is bounded within the '#' (0x23) section;
		// the lqe '%' (0x25) section sorts strictly after it, so a changefeed
		// range scan can never include an lqe key and vice versa. Proven below on
		// real encoded keys rather than the literal markers.
		let ts_impl = HlcTimeStampImpl;
		let buf = &mut [0u8; _];
		let ts = ts_impl.create_from_versionstamp(1).unwrap().encode(buf);
		let cf_suffix = ChangeFeedPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		}
		.encode_bound()
		.unwrap();
		let lqe = LqeTsRange {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ts: Cow::Borrowed(ts),
		}
		.encode_bound()
		.unwrap();
		assert!(lqe > cf_suffix, "lqe keys must sort after the changefeed suffix");
	}
}
