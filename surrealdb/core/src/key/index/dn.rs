//! Stores DiskANN neighbor lists.

use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::idx::trees::diskann::{DiskAnnNode, ElementId};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::val::TableName;

key! {
/// Stores one DiskANN graph adjacency list.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct Dn<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'n',
		pub element_id: ElementId,
	}
}

impl_kv_key_storekey!(Dn<'a> => DiskAnnNode);
