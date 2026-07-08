//! Stores hashed vector to document mappings for a DiskANN index.

use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::idx::trees::diskann::docs::DiskAnnElementHashedDocs;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::val::TableName;

key! {
/// Maps a vector hash to one or more full-vector DiskANN document mappings.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dh<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		 b'!',
		 b'd',
		 b'h',
		pub hash: [u8; 32],
	}
}

impl_kv_key_storekey!(Dh<'a> => DiskAnnElementHashedDocs);
