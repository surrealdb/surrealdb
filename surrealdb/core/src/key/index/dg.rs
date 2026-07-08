//! DiskANN compaction generation key.

use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::val::TableName;

key! {
/// Stores the compaction generation for one DiskANN index.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dg<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'g',
	}
}

impl_kv_key_storekey!(Dg<'a> => u64);
