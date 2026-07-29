//! Stores vectors and status for a DiskANN index.

use std::borrow::Cow;

use anyhow::Result;
use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::idx::trees::diskann::{DiskAnnElement, ElementId};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
/// Stores one DiskANN graph element vector and deleted marker.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct De<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'e',
		pub element_id: ElementId,
	}
}

impl_kv_key_storekey!(De<'a> => DiskAnnElement);

key! {
/// Stores one DiskANN graph element vector and deleted marker.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DePrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'e',
	}
}
impl_kv_range_storekey!(DePrefix<'_>);
