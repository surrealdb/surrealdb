//! Stores DocId to RecordId mappings for a DiskANN index.

use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::diskann::docs::DiskAnnDocsState;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::val::{RecordIdKey, TableName};

key! {
	/// Root key storing the DiskANN document-ID allocator state.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DdRoot<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'd',
	}
}

impl_kv_key_storekey!(DdRoot<'a> => DiskAnnDocsState);

key! {
	/// Maps a compact DiskANN document ID back to a SurrealDB record key.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dd<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'd',
		pub doc_id: DocId,
	}
}

impl_kv_key_storekey!(Dd<'a> => RecordIdKey);
