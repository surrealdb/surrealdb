use crate::err::Error;
use crate::sql::statements::DefineEventStatement;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::statements::DefineTableStatement;
use crate::sql::statements::LiveStatement;
use std::sync::Arc;

#[derive(Clone)]
#[non_exhaustive]
pub enum Entry {
	/// A slice of DefineEventStatement specified on a table.
	Evs(Arc<[DefineEventStatement]>),
	/// A slice of DefineFieldStatement specified on a table.
	Fds(Arc<[DefineFieldStatement]>),
	/// A slice of DefineTableStatement specified on a table.
	Fts(Arc<[DefineTableStatement]>),
	/// A slice of DefineIndexStatement specified on a table.
	Ixs(Arc<[DefineIndexStatement]>),
	/// A slice of LiveStatement specified on a table.
	Lvs(Arc<[LiveStatement]>),
}

impl Entry {
	/// Converts this cache entry into a slice of [`DefineEventStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Evs`].
	pub(crate) fn try_into_evs(self) -> Result<Arc<[DefineEventStatement]>, Error> {
		match self {
			Entry::Evs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Evs")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineFieldStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fds`].
	pub(crate) fn try_into_fds(self) -> Result<Arc<[DefineFieldStatement]>, Error> {
		match self {
			Entry::Fds(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Fds")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineIndexStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Ixs`].
	pub(crate) fn try_into_ixs(self) -> Result<Arc<[DefineIndexStatement]>, Error> {
		match self {
			Entry::Ixs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Ixs")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineTableStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fts`].
	pub(crate) fn try_into_fts(self) -> Result<Arc<[DefineTableStatement]>, Error> {
		match self {
			Entry::Fts(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Fts")),
		}
	}
	/// Converts this cache entry into a slice of [`LiveStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Lvs`].
	pub(crate) fn try_into_lvs(self) -> Result<Arc<[LiveStatement]>, Error> {
		match self {
			Entry::Lvs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Lvs")),
		}
	}
}
