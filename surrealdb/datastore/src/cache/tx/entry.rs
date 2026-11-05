use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use surrealdb_catalog::node::Node;
use surrealdb_catalog::{self, Generation, Record};

use crate::catalog;

#[derive(Clone)]
pub enum Entry {
	/// A cached entry of any type
	Any(Arc<dyn Any + Send + Sync>),
	/// A cached record and the generation its stored bytes carry
	/// ([`crate::Transaction::get_stamped_record`]).
	Val(Arc<Record>, Option<Generation>),
	/// A slice of Node specified at the root.
	Nds(Arc<[Node]>),
	/// A slice of DefineUserStatement specified at the root.
	Rus(Arc<[catalog::UserDefinition]>),
	/// A slice of AccessDefinition specified at the root.
	Ras(Arc<[catalog::AccessDefinition]>),
	/// A slice of AccessGrant specified at the root.
	Rag(Arc<[catalog::AccessGrant]>),
	/// A slice of NamespaceDefinition specified on a namespace.
	Nss(Arc<[catalog::NamespaceDefinition]>),
	/// A slice of DefineUserStatement specified on a namespace.
	Nus(Arc<[catalog::UserDefinition]>),
	/// A slice of AccessDefinition specified on a namespace.
	Nas(Arc<[catalog::AccessDefinition]>),
	/// A slice of AccessGrant specified at on a namespace.
	Nag(Arc<[catalog::AccessGrant]>),
	/// A slice of DatabaseDefinition specified on a namespace.
	Dbs(Arc<[catalog::DatabaseDefinition]>),
	/// A slice of ApiDefinition specified on a namespace.
	Aps(Arc<[catalog::ApiDefinition]>),
	/// A slice of catalog::AnalyzerDefinition specified on a namespace.
	Azs(Arc<[catalog::AnalyzerDefinition]>),
	/// A slice of BucketDefinition specified on a database.
	Bus(Arc<[catalog::BucketDefinition]>),
	/// A slice of TypeDefinition specified on a database.
	Tys(Arc<[catalog::TypeDefinition]>),
	/// A slice of AccessDefinition specified on a database.
	Das(Arc<[catalog::AccessDefinition]>),
	/// A slice of AccessGrant specified at on a database.
	Dag(Arc<[catalog::AccessGrant]>),
	/// A slice of DefineUserStatement specified on a database.
	Dus(Arc<[catalog::UserDefinition]>),
	/// A slice of FunctionDefinition specified on a database.
	Fcs(Arc<[catalog::FunctionDefinition]>),
	/// A slice of ModuleDefinition specified on a database.
	Mds(Arc<[catalog::ModuleDefinition]>),
	/// A slice of TableDefinition specified on a database.
	Tbs(Arc<[catalog::TableDefinition]>),
	/// A slice of MlModelDefinition specified on a database.
	Mls(Arc<[catalog::MlModelDefinition]>),
	/// A slice of ConfigDefinition specified on a database.
	Cgs(Arc<[catalog::ConfigDefinition]>),
	/// A slice of ParamDefinition specified on a database.
	Pas(Arc<[catalog::ParamDefinition]>),
	/// A slice of DefineSequenceStatement specified on a namespace.
	Sqs(Arc<[catalog::SequenceDefinition]>),
	/// A slice of EventDefinition specified on a table.
	Evs(Arc<[catalog::EventDefinition]>),
	/// A slice of FieldDefinition specified on a table.
	Fds(Arc<[catalog::FieldDefinition]>),
	/// A slice of TableDefinition specified on a table.
	Fts(Arc<[catalog::TableDefinition]>),
	/// A slice of IndexDefinition specified on a table.
	Ixs(Arc<[catalog::IndexDefinition]>),
	/// A slice of SubscriptionDefinition specified on a table.
	Lvs(Arc<[catalog::SubscriptionDefinition]>),
}

impl Entry {
	/// Converts this cache entry into a single entry of arbitrary type.
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Any`].
	pub fn try_into_type<T: Send + Sync + 'static>(self: Entry) -> Result<Arc<T>> {
		if let Entry::Any(v) = self {
			let Ok(x) = v.downcast::<T>() else {
				fail!("Unable to convert type into Entry::Any")
			};
			Ok(x)
		} else {
			fail!("Unable to convert type into Entry::Any")
		}
	}
	/// Converts this cache entry into a slice of [`Node`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Nds`].
	pub fn try_into_nds(self) -> Result<Arc<[Node]>> {
		if let Entry::Nds(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Nds")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::UserDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Rus`].
	pub fn try_into_rus(self) -> Result<Arc<[catalog::UserDefinition]>> {
		if let Entry::Rus(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Rus")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::AccessDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Ras`].
	pub fn try_into_ras(self) -> Result<Arc<[catalog::AccessDefinition]>> {
		if let Entry::Ras(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Ras")
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrantStore`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Rag`].
	pub fn try_into_rag(self) -> Result<Arc<[catalog::AccessGrant]>> {
		if let Entry::Rag(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Rag")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::NamespaceDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Nss`].
	pub fn try_into_nss(self) -> Result<Arc<[catalog::NamespaceDefinition]>> {
		if let Entry::Nss(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Nss")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::AccessDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Nas`].
	pub fn try_into_nas(self) -> Result<Arc<[catalog::AccessDefinition]>> {
		if let Entry::Nas(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Nas")
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrantStore`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Nag`].
	pub fn try_into_nag(self) -> Result<Arc<[catalog::AccessGrant]>> {
		if let Entry::Nag(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Nag")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::UserDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Nus`].
	pub fn try_into_nus(self) -> Result<Arc<[catalog::UserDefinition]>> {
		if let Entry::Nus(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Nus")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::DatabaseDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Dbs`].
	pub fn try_into_dbs(self) -> Result<Arc<[catalog::DatabaseDefinition]>> {
		if let Entry::Dbs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Dbs")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::AccessDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Das`].
	pub fn try_into_das(self) -> Result<Arc<[catalog::AccessDefinition]>> {
		if let Entry::Das(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Das")
		}
	}
	/// Converts this cache entry into a slice of [`AccessGrantStore`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Dag`].
	pub fn try_into_dag(self) -> Result<Arc<[catalog::AccessGrant]>> {
		if let Entry::Dag(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Dag")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::UserDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Dus`].
	pub fn try_into_dus(self) -> Result<Arc<[catalog::UserDefinition]>> {
		if let Entry::Dus(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Dus")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::ApiDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Aps`].
	pub fn try_into_aps(self) -> Result<Arc<[catalog::ApiDefinition]>> {
		if let Entry::Aps(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Aps")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::AnalyzerDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Azs`].
	pub fn try_into_azs(self) -> Result<Arc<[catalog::AnalyzerDefinition]>> {
		if let Entry::Azs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Azs")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::BucketDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Bus`].
	pub fn try_into_bus(self) -> Result<Arc<[catalog::BucketDefinition]>> {
		if let Entry::Bus(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Bus")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::TypeDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Tys`].
	pub fn try_into_tys(self) -> Result<Arc<[catalog::TypeDefinition]>> {
		if let Entry::Tys(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Tys")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::SequenceDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Sqs`].
	pub fn try_into_sqs(self) -> Result<Arc<[catalog::SequenceDefinition]>> {
		if let Entry::Sqs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Sqs")
		}
	}

	/// Converts this cache entry into a slice of [`catalog::FunctionDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Fcs`].
	pub fn try_into_fcs(self) -> Result<Arc<[catalog::FunctionDefinition]>> {
		if let Entry::Fcs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Fcs")
		}
	}

	/// Converts this cache entry into a slice of [`catalog::ModuleDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Mds`].
	pub fn try_into_mds(self) -> Result<Arc<[catalog::ModuleDefinition]>> {
		if let Entry::Mds(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Mds")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::ParamDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Pas`].
	pub fn try_into_pas(self) -> Result<Arc<[catalog::ParamDefinition]>> {
		if let Entry::Pas(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Pas")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::MlModelDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Mls`].
	pub fn try_into_mls(self) -> Result<Arc<[catalog::MlModelDefinition]>> {
		if let Entry::Mls(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Mls")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::ConfigDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Cgs`].
	pub fn try_into_cgs(self) -> Result<Arc<[catalog::ConfigDefinition]>> {
		if let Entry::Cgs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Cgs")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::TableDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Tbs`].
	pub fn try_into_tbs(self) -> Result<Arc<[catalog::TableDefinition]>> {
		if let Entry::Tbs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Tbs")
		}
	}

	/// Converts this cache entry into a slice of [`catalog::EventDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Evs`].
	pub fn try_into_evs(self) -> Result<Arc<[catalog::EventDefinition]>> {
		if let Entry::Evs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Evs")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::FieldDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Fds`].
	pub fn try_into_fds(self) -> Result<Arc<[catalog::FieldDefinition]>> {
		if let Entry::Fds(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Fds")
		}
	}

	/// Converts this cache entry into a slice of [`catalog::IndexDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Ixs`].
	pub fn try_into_ixs(self) -> Result<Arc<[catalog::IndexDefinition]>> {
		if let Entry::Ixs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Ixs")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::TableDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Fts`].
	pub fn try_into_fts(self) -> Result<Arc<[catalog::TableDefinition]>> {
		if let Entry::Fts(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Fts")
		}
	}
	/// Converts this cache entry into a slice of [`catalog::SubscriptionDefinition`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Lvs`].
	pub fn try_into_lvs(self) -> Result<Arc<[catalog::SubscriptionDefinition]>> {
		if let Entry::Lvs(v) = self {
			Ok(v)
		} else {
			fail!("Unable to convert type into Entry::Lvs")
		}
	}
	/// Converts this cache entry into a single [`Record`].
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Val`].
	pub fn try_into_record(self) -> Result<Arc<Record>> {
		self.try_into_stamped_record().map(|(record, _)| record)
	}
	/// Converts this cache entry into a single [`Record`] and the generation its stored bytes
	/// carry.
	/// Fails with a typed internal error if called on a cache entry that is not an [`Entry::Val`].
	pub fn try_into_stamped_record(self) -> Result<(Arc<Record>, Option<Generation>)> {
		if let Entry::Val(record, generation) = self {
			Ok((record, generation))
		} else {
			fail!("Unable to convert type into Entry::Val")
		}
	}
}
