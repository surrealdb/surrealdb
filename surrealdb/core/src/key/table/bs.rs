//! Stores durable index build state outside the index-data prefix.
//!
//! `!bs{ix}` survives index-data cleanup and is visible to every node. It is
//! the coordination record for builder ownership, writer admission tickets, and
//! planner visibility.
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::index::IndexBuildState;
use crate::val::TableName;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct Bs<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b's',
		pub ix: IndexId,
	}
}

impl_kv_key_storekey!(Bs<'a> => IndexBuildState);

impl Categorise for Bs<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBuildState
	}
}
