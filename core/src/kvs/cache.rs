use super::Key;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::sql::statements::AccessGrant;
use crate::sql::statements::DefineAccessStatement;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::statements::DefineDatabaseStatement;
use crate::sql::statements::DefineEventStatement;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::statements::DefineFunctionStatement;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::statements::DefineModelStatement;
use crate::sql::statements::DefineNamespaceStatement;
use crate::sql::statements::DefineParamStatement;
use crate::sql::statements::DefineTableStatement;
use crate::sql::statements::DefineUserStatement;
use crate::sql::statements::LiveStatement;
use crate::sql::Value;
use quick_cache::Weighter;
use std::any::Any;
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct EntryWeighter;

impl Weighter<Key, Entry> for EntryWeighter {
	fn weight(&self, _key: &Key, val: &Entry) -> u32 {
		match val {
			// Value entries all have the same weight,
			// and can be evicted whenever necessary.
			// We could improve this, by calculating
			// the precise weight of a Value (when
			// deserialising), and using this size to
			// determine the actual cache weight.
			Entry::Val(_) => 1,
			// We don't want to evict other entries
			// so we set the weight to 0 which will
			// prevent entries being evicted, unless
			// specifically removed from the cache.
			_ => 0,
		}
	}
}

#[derive(Clone)]
#[non_exhaustive]
pub(super) enum Entry {
	/// A cached entry of any type
	Any(Arc<dyn Any + Send + Sync>),
	/// A cached record document content
	Val(Arc<Value>),
	/// A slice of Node specified at the root.
	Nds(Arc<[Node]>),
	/// A slice of DefineUserStatement specified at the root.
	Rus(Arc<[DefineUserStatement]>),
	/// A slice of DefineAccessStatement specified at the root.
	Ras(Arc<[DefineAccessStatement]>),
	/// A slice of AccessGrant specified at the root.
	Rag(Arc<[AccessGrant]>),
	/// A slice of DefineNamespaceStatement specified on a namespace.
	Nss(Arc<[DefineNamespaceStatement]>),
	/// A slice of DefineUserStatement specified on a namespace.
	Nus(Arc<[DefineUserStatement]>),
	/// A slice of DefineAccessStatement specified on a namespace.
	Nas(Arc<[DefineAccessStatement]>),
	/// A slice of AccessGrant specified at on a namespace.
	Nag(Arc<[AccessGrant]>),
	/// A slice of DefineDatabaseStatement specified on a namespace.
	Dbs(Arc<[DefineDatabaseStatement]>),
	/// A slice of DefineAnalyzerStatement specified on a namespace.
	Azs(Arc<[DefineAnalyzerStatement]>),
	/// A slice of DefineAccessStatement specified on a database.
	Das(Arc<[DefineAccessStatement]>),
	/// A slice of AccessGrant specified at on a database.
	Dag(Arc<[AccessGrant]>),
	/// A slice of DefineUserStatement specified on a database.
	Dus(Arc<[DefineUserStatement]>),
	/// A slice of DefineFunctionStatement specified on a database.
	Fcs(Arc<[DefineFunctionStatement]>),
	/// A slice of DefineTableStatement specified on a database.
	Fts(Arc<[DefineTableStatement]>),
	/// A slice of DefineModelStatement specified on a database.
	Mls(Arc<[DefineModelStatement]>),
	/// A slice of DefineParamStatement specified on a database.
	Pas(Arc<[DefineParamStatement]>),
	/// A slice of DefineTableStatement specified on a database.
	Tbs(Arc<[DefineTableStatement]>),
	/// A slice of DefineEventStatement specified on a table.
	Evs(Arc<[DefineEventStatement]>),
	/// A slice of DefineFieldStatement specified on a table.
	Fds(Arc<[DefineFieldStatement]>),
	/// A slice of DefineIndexStatement specified on a table.
	Ixs(Arc<[DefineIndexStatement]>),
	/// A slice of LiveStatement specified on a table.
	Lvs(Arc<[LiveStatement]>),
}

impl Entry {
	/// Converts this cache entry into a single entry of arbitrary type.
	/// This panics if called on a cache entry that is not an [`Entry::Any`].
	pub(super) fn try_into_type<T: Send + Sync + 'static>(self: Entry) -> Result<Arc<T>, Error> {
		match self {
			Entry::Any(v) => {
				v.downcast::<T>().map_err(|_| fail!("Unable to convert type into Entry::Any"))
			}
			_ => Err(fail!("Unable to convert type into Entry::Any")),
		}
	}
	/// Converts this cache entry into a slice of [`Node`].
	/// This panics if called on a cache entry that is not an [`Entry::Nds`].
	pub(super) fn try_into_nds(self) -> Result<Arc<[Node]>, Error> {
		match self {
			Entry::Nds(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Nds")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineUserStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Rus`].
	pub(super) fn try_into_rus(self) -> Result<Arc<[DefineUserStatement]>, Error> {
		match self {
			Entry::Rus(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Rus")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineAccessStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Ras`].
	pub(super) fn try_into_ras(self) -> Result<Arc<[DefineAccessStatement]>, Error> {
		match self {
			Entry::Ras(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Ras")),
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrant`].
	/// This panics if called on a cache entry that is not an [`Entry::Rag`].
	pub(super) fn try_into_rag(self) -> Result<Arc<[AccessGrant]>, Error> {
		match self {
			Entry::Rag(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Rag")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineNamespaceStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Nss`].
	pub(super) fn try_into_nss(self) -> Result<Arc<[DefineNamespaceStatement]>, Error> {
		match self {
			Entry::Nss(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Nss")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineAccessStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Nas`].
	pub(super) fn try_into_nas(self) -> Result<Arc<[DefineAccessStatement]>, Error> {
		match self {
			Entry::Nas(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Nas")),
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrant`].
	/// This panics if called on a cache entry that is not an [`Entry::Nag`].
	pub(super) fn try_into_nag(self) -> Result<Arc<[AccessGrant]>, Error> {
		match self {
			Entry::Nag(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Nag")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineUserStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Nus`].
	pub(super) fn try_into_nus(self) -> Result<Arc<[DefineUserStatement]>, Error> {
		match self {
			Entry::Nus(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Nus")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineDatabaseStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Dbs`].
	pub(super) fn try_into_dbs(self) -> Result<Arc<[DefineDatabaseStatement]>, Error> {
		match self {
			Entry::Dbs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Dbs")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineAccessStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Das`].
	pub(super) fn try_into_das(self) -> Result<Arc<[DefineAccessStatement]>, Error> {
		match self {
			Entry::Das(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Das")),
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrant`].
	/// This panics if called on a cache entry that is not an [`Entry::Dag`].
	pub(super) fn try_into_dag(self) -> Result<Arc<[AccessGrant]>, Error> {
		match self {
			Entry::Dag(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Dag")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineUserStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Dus`].
	pub(super) fn try_into_dus(self) -> Result<Arc<[DefineUserStatement]>, Error> {
		match self {
			Entry::Dus(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Dus")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineAnalyzerStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Azs`].
	pub(super) fn try_into_azs(self) -> Result<Arc<[DefineAnalyzerStatement]>, Error> {
		match self {
			Entry::Azs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Azs")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineFunctionStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fcs`].
	pub(super) fn try_into_fcs(self) -> Result<Arc<[DefineFunctionStatement]>, Error> {
		match self {
			Entry::Fcs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Fcs")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineParamStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Pas`].
	pub(super) fn try_into_pas(self) -> Result<Arc<[DefineParamStatement]>, Error> {
		match self {
			Entry::Pas(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Pas")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineModelStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Mls`].
	pub(super) fn try_into_mls(self) -> Result<Arc<[DefineModelStatement]>, Error> {
		match self {
			Entry::Mls(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Mls")),
		}
	}
	/// Converts this cache entry into a slice of [`DefineTableStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Tbs`].
	pub(super) fn try_into_tbs(self) -> Result<Arc<[DefineTableStatement]>, Error> {
		match self {
			Entry::Tbs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Tbs")),
		}
	}
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
	/// Converts this cache entry into a slice of [`DefineIndexStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Ixs`].
	pub(super) fn try_into_ixs(self) -> Result<Arc<[DefineIndexStatement]>, Error> {
		match self {
			Entry::Ixs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Ixs")),
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
	/// Converts this cache entry into a slice of [`LiveStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Lvs`].
	pub(super) fn try_into_lvs(self) -> Result<Arc<[LiveStatement]>, Error> {
		match self {
			Entry::Lvs(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Lvs")),
		}
	}
	/// Converts this cache entry into a single [`Value`].
	/// This panics if called on a cache entry that is not an [`Entry::Val`].
	pub(super) fn try_into_val(self) -> Result<Arc<Value>, Error> {
		match self {
			Entry::Val(v) => Ok(v),
			_ => Err(fail!("Unable to convert type into Entry::Val")),
		}
	}
}
