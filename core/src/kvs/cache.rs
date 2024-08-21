use super::Key;
use crate::dbs::node::Node;
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
	pub(super) fn into_type<T: Send + Sync + 'static>(self: Entry) -> Arc<T> {
		match self {
			Entry::Any(v) => v.downcast::<T>().unwrap(),
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`Node`].
	/// This panics if called on a cache entry that is not an [`Entry::Nds`].
	pub(super) fn into_nds(self) -> Arc<[Node]> {
		match self {
			Entry::Nds(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineUserStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Rus`].
	pub(super) fn into_rus(self) -> Arc<[DefineUserStatement]> {
		match self {
			Entry::Rus(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineAccessStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Ras`].
	pub(super) fn into_ras(self) -> Arc<[DefineAccessStatement]> {
		match self {
			Entry::Ras(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrant`].
	/// This panics if called on a cache entry that is not an [`Entry::Rag`].
	pub(super) fn into_rag(self) -> Arc<[AccessGrant]> {
		match self {
			Entry::Rag(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineNamespaceStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Nss`].
	pub(super) fn into_nss(self) -> Arc<[DefineNamespaceStatement]> {
		match self {
			Entry::Nss(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineAccessStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Nas`].
	pub(super) fn into_nas(self) -> Arc<[DefineAccessStatement]> {
		match self {
			Entry::Nas(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrant`].
	/// This panics if called on a cache entry that is not an [`Entry::Nag`].
	pub(super) fn into_nag(self) -> Arc<[AccessGrant]> {
		match self {
			Entry::Nag(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineUserStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Nus`].
	pub(super) fn into_nus(self) -> Arc<[DefineUserStatement]> {
		match self {
			Entry::Nus(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineDatabaseStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Dbs`].
	pub(super) fn into_dbs(self) -> Arc<[DefineDatabaseStatement]> {
		match self {
			Entry::Dbs(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineAccessStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Das`].
	pub(super) fn into_das(self) -> Arc<[DefineAccessStatement]> {
		match self {
			Entry::Das(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrant`].
	/// This panics if called on a cache entry that is not an [`Entry::Dag`].
	pub(super) fn into_dag(self) -> Arc<[AccessGrant]> {
		match self {
			Entry::Dag(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineUserStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Dus`].
	pub(super) fn into_dus(self) -> Arc<[DefineUserStatement]> {
		match self {
			Entry::Dus(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineAnalyzerStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Azs`].
	pub(super) fn into_azs(self) -> Arc<[DefineAnalyzerStatement]> {
		match self {
			Entry::Azs(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineFunctionStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fcs`].
	pub(super) fn into_fcs(self) -> Arc<[DefineFunctionStatement]> {
		match self {
			Entry::Fcs(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineParamStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Pas`].
	pub(super) fn into_pas(self) -> Arc<[DefineParamStatement]> {
		match self {
			Entry::Pas(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineModelStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Mls`].
	pub(super) fn into_mls(self) -> Arc<[DefineModelStatement]> {
		match self {
			Entry::Mls(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineTableStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Tbs`].
	pub(super) fn into_tbs(self) -> Arc<[DefineTableStatement]> {
		match self {
			Entry::Tbs(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineEventStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Evs`].
	pub(super) fn into_evs(self) -> Arc<[DefineEventStatement]> {
		match self {
			Entry::Evs(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineFieldStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fds`].
	pub(super) fn into_fds(self) -> Arc<[DefineFieldStatement]> {
		match self {
			Entry::Fds(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineIndexStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Ixs`].
	pub(super) fn into_ixs(self) -> Arc<[DefineIndexStatement]> {
		match self {
			Entry::Ixs(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`DefineTableStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Fts`].
	pub(super) fn into_fts(self) -> Arc<[DefineTableStatement]> {
		match self {
			Entry::Fts(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a slice of [`LiveStatement`].
	/// This panics if called on a cache entry that is not an [`Entry::Lvs`].
	pub(super) fn into_lvs(self) -> Arc<[LiveStatement]> {
		match self {
			Entry::Lvs(v) => v,
			_ => unreachable!(),
		}
	}
	/// Converts this cache entry into a single [`Value`].
	/// This panics if called on a cache entry that is not an [`Entry::Val`].
	pub(super) fn into_val(self) -> Arc<Value> {
		match self {
			Entry::Val(v) => v,
			_ => unreachable!(),
		}
	}
}
