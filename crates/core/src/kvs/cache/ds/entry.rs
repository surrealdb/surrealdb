use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::err::Error;
use crate::expr::statements::{
	DefineEventStatement, DefineFieldStatement, DefineIndexStatement, LiveStatement,
};

#[derive(Clone, Debug)]
pub(crate) enum Entry {
	/// A cached entry of any type
	Any(Arc<dyn Any + Send + Sync>),
	/// A slice of DefineEventStatement specified on a table.
	Evs(Arc<[DefineEventStatement]>),
	/// A slice of DefineFieldStatement specified on a table.
	Fds(Arc<[DefineFieldStatement]>),
	/// A slice of TableDefinition specified on a table.
	Fts(Arc<[TableDefinition]>),
	/// A slice of DefineIndexStatement specified on a table.
	Ixs(Arc<[DefineIndexStatement]>),
	/// A slice of LiveStatement specified on a table.
	Lvs(Arc<[LiveStatement]>),
	/// An Uuid.
	Lvv(Uuid),
}

impl Entry {
	/// Converts this cache entry into a single entry of arbitrary type.
	/// This panics if called on a cache entry that is not an [`Entry::Any`].
	pub(crate) fn try_into_type<T: Send + Sync + 'static>(self: Entry) -> Result<Arc<T>> {
		match self {
			Entry::Any(v) => v
				.downcast::<T>()
				.map_err(|_| Error::unreachable("Unable to convert type into Entry::Any"))
				.map_err(anyhow::Error::new),
			_ => fail!("Unable to convert type into Entry::Any"),
		}
	}
	/// Converts this cache entry into a slice of [`DefineEventStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Evs`].
	pub(crate) fn try_into_evs(self) -> Result<Arc<[DefineEventStatement]>> {
		match self {
			Entry::Evs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Evs"),
		}
	}
	/// Converts this cache entry into a slice of [`DefineFieldStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fds`].
	pub(crate) fn try_into_fds(self) -> Result<Arc<[DefineFieldStatement]>> {
		match self {
			Entry::Fds(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Fds"),
		}
	}
	/// Converts this cache entry into a slice of [`DefineIndexStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Ixs`].
	pub(crate) fn try_into_ixs(self) -> Result<Arc<[DefineIndexStatement]>> {
		match self {
			Entry::Ixs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Ixs"),
		}
	}
	/// Converts this cache entry into a slice of [`TableDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Fts`].
	pub(crate) fn try_into_fts(self) -> Result<Arc<[TableDefinition]>> {
		match self {
			Entry::Fts(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Fts"),
		}
	}
	/// Converts this cache entry into a slice of [`LiveStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Lvs`].
	pub(crate) fn try_into_lvs(self) -> Result<Arc<[LiveStatement]>> {
		match self {
			Entry::Lvs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Lvs"),
		}
	}

	/// Converts this cache entry into a slice of [`LiveStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Lvs`].
	pub(crate) fn try_info_lvv(self) -> Result<Uuid> {
		match self {
			Entry::Lvv(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Lvv"),
		}
	}
}
