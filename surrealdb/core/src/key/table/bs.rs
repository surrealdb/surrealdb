//! Stores durable index build state outside the index-data prefix.
//!
//! `!bs{ix}` survives index-data cleanup and is visible to every node. It is
//! the coordination record for builder ownership, writer admission tickets, and
//! planner visibility.
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::index::IndexBuildState;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Bs<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: IndexId,
}

impl_kv_key_storekey!(Bs<'_> => IndexBuildState);

impl Categorise for Bs<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBuildState
	}
}

impl<'a> Bs<'a> {
	/// Create the durable build-state key for one table index.
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ix: IndexId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'b',
			_f: b's',
			ix,
		}
	}
}
