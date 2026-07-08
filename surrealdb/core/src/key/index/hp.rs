//! Append-keyed pending update range for HNSW indexes.
//!
//! HNSW compaction and query-time pending searches read this range when
//! applying stored append-keyed pending values.

use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	/// Prefix for append-keyed HNSW pending updates for an index.
	///
	/// Values in this range are encoded as `VectorPendingUpdate` and are consumed
	/// by scans over the range rather than by addressing individual keys.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct HnswPendingPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'p',
	}
}

impl_kv_range_storekey!(HnswPendingPrefix<'_>);
