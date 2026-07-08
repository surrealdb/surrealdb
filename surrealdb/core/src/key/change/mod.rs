//! Stores change feeds
use std::borrow::Cow;

use anyhow::Result;

use crate::cf::TableMutations;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::TableName;

// Cf stands for change feeds
key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct ChangeFeed<'a> {
		pub prefix: DatabaseRoot,
		b'#',
		// ts is the timestamp of the change feed entry that is encoded in big-endian.
		pub ts: Cow<'a, [u8]>,
		b'*',
		pub tb: Cow<'a, TableName>,
	}
}

impl_kv_key_storekey!(ChangeFeed<'a> => TableMutations);

impl Categorise for ChangeFeed<'_> {
	fn categorise(&self) -> Category {
		Category::ChangeFeed
	}
}

key! {
	/// A prefix or suffix for a database change feed
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub struct ChangeFeedPrefix{
		pub prefix: DatabaseRoot,
		b'#',
	}
}
impl_kv_range_storekey!(ChangeFeedPrefix);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub struct ChangeFeedTsPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'#',
		pub ts: Cow<'a, [u8]>,
	}
}
impl_kv_range_storekey!(ChangeFeedTsPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};
	use crate::kvs::{HlcTimeStampImpl, TimeStampImpl};

	#[test]
	fn cf_key() {
		let ts_impl = HlcTimeStampImpl;

		let buf = &mut [0u8; _];
		let ts1 = ts_impl.create_from_versionstamp(12345).unwrap().encode(buf);
		let tb = TableName::from("test");
		let val = ChangeFeed {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ts: ts1.into(),
			tb: Cow::Borrowed(&tb),
		};
		let enc = ChangeFeed::encode_key(&val).unwrap();
		// Verify the encoded key - note that Cow<[u8]> is encoded with length prefix
		assert_eq!(
			enc.as_slice(),
			&[
				47, 42, 0, 0, 0, 1, 42, 0, 0, 0, 2, 35, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 48, 57,
				0, 42, 116, 101, 115, 116, 0
			]
		);

		let buf = &mut [0; _];
		let ts2 = ts_impl.create_from_versionstamp(12346).unwrap().encode(buf);
		let val = ChangeFeed {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ts: ts2.into(),
			tb: Cow::Borrowed(&tb),
		};
		let enc = ChangeFeed::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			&[
				47, 42, 0, 0, 0, 1, 42, 0, 0, 0, 2, 35, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 48, 58,
				0, 42, 116, 101, 115, 116, 0
			]
		);
	}

	#[test]
	fn range_key() {
		let val = ChangeFeedPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		};
		let enc = ChangeFeedPrefix::encode_bound(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#");
	}

	#[test]
	fn ts_prefix_key() {
		let ts_impl = HlcTimeStampImpl;
		let buf = &mut [0u8; _];
		let ts = ts_impl.create_from_versionstamp(12345).unwrap().encode(buf);
		let val = ChangeFeedTsPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ts: ts.into(),
		};
		let enc = ChangeFeedTsPrefix::encode_bound(&val).unwrap();
		// Verify the encoded key - note that Cow<[u8]> is encoded with length prefix
		assert_eq!(
			enc.as_slice(),
			&[
				47, 42, 0, 0, 0, 1, 42, 0, 0, 0, 2, 35, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 48, 57,
				0
			]
		);
	}
}
