use super::super::kvs::Key;
use crate::cnf::DEFINITION_CACHE_SIZE;
use crate::err::Error;
use crate::sql::statements::DefineEventStatement;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::statements::DefineTableStatement;
use crate::sql::statements::LiveStatement;
use derive::Key;
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::LazyLock;
use uuid::Uuid;

pub static CACHE: LazyLock<Arc<Cache<Key, Entry>>> =
	LazyLock::new(|| Arc::new(Cache::new(*DEFINITION_CACHE_SIZE)));

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub(super) struct Evs<'a> {
	_a: u8,
	_b: u8,
	pub ns: &'a str,
	pub db: &'a str,
	pub tb: &'a str,
	pub cache: Uuid,
}

impl<'a> Evs<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, cache: Uuid) -> Self {
		Self {
			_a: b'e',
			_b: b'v',
			ns,
			db,
			tb,
			cache,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub(super) struct Fds<'a> {
	_a: u8,
	_b: u8,
	pub ns: &'a str,
	pub db: &'a str,
	pub tb: &'a str,
	pub cache: Uuid,
}

impl<'a> Fds<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, cache: Uuid) -> Self {
		Self {
			_a: b'f',
			_b: b'd',
			ns,
			db,
			tb,
			cache,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub(super) struct Fts<'a> {
	_a: u8,
	_b: u8,
	pub ns: &'a str,
	pub db: &'a str,
	pub tb: &'a str,
	pub cache: Uuid,
}

impl<'a> Fts<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, cache: Uuid) -> Self {
		Self {
			_a: b'f',
			_b: b't',
			ns,
			db,
			tb,
			cache,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub(super) struct Ixs<'a> {
	_a: u8,
	_b: u8,
	pub ns: &'a str,
	pub db: &'a str,
	pub tb: &'a str,
	pub cache: Uuid,
}

impl<'a> Ixs<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, cache: Uuid) -> Self {
		Self {
			_a: b'i',
			_b: b'x',
			ns,
			db,
			tb,
			cache,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub(super) struct Lvs<'a> {
	_a: u8,
	_b: u8,
	pub ns: &'a str,
	pub db: &'a str,
	pub tb: &'a str,
	pub cache: Uuid,
}

impl<'a> Lvs<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, cache: Uuid) -> Self {
		Self {
			_a: b'l',
			_b: b'v',
			ns,
			db,
			tb,
			cache,
		}
	}
}

#[derive(Clone)]
#[non_exhaustive]
pub(super) enum Entry {
	/// A slice of DefineEventStatement specified on a table.
	Evs(Arc<[DefineEventStatement]>),
	/// A slice of DefineFieldStatement specified on a table.
	Fds(Arc<[DefineFieldStatement]>),
	/// A slice of DefineTableStatement specified on a database.
	Fts(Arc<[DefineTableStatement]>),
	/// A slice of DefineIndexStatement specified on a table.
	Ixs(Arc<[DefineIndexStatement]>),
	/// A slice of DefineIndexStatement specified on a table.
	Lvs(Arc<[LiveStatement]>),
}

impl Entry {
	/// Converts this cache entry into a slice of [`DefineEventStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Evs`].
	pub(super) fn try_into_evs(self) -> Result<Arc<[DefineEventStatement]>, Error> {
		match self {
			Entry::Evs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Evs")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineFieldStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fds`].
	pub(super) fn try_into_fds(self) -> Result<Arc<[DefineFieldStatement]>, Error> {
		match self {
			Entry::Fds(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Fds")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineTableStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fts`].
	pub(super) fn try_into_fts(self) -> Result<Arc<[DefineTableStatement]>, Error> {
		match self {
			Entry::Fts(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Fts")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineIndexStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Ixs`].
	pub(super) fn try_into_ixs(self) -> Result<Arc<[DefineIndexStatement]>, Error> {
		match self {
			Entry::Ixs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Ixs")),
		}
	}
	/// Converts this cache entry into a slice of [`LiveStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Lvs`].
	pub(super) fn try_into_lvs(self) -> Result<Arc<[LiveStatement]>, Error> {
		match self {
			Entry::Lvs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Lvs")),
		}
	}
}
