//! Stores DiskANN graph state.

use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::idx::trees::diskann::DiskAnnState;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct Ds<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b's',
	}
}

impl_kv_key_storekey!(Ds<'a> => DiskAnnState);
