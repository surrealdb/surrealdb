//! Catalog providers.
//!
//! Providers are used as the data access layer for the catalog.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use tracing::Instrument;
use uuid::Uuid;

use crate::catalog;
use crate::catalog::{
	DatabaseDefinition, DatabaseId, DefaultConfig, IndexId, NamespaceDefinition, NamespaceId,
	Record, TableDefinition, TableId, UserDefinition,
};
use crate::ctx::Context;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::kvs::CachePolicy;
use crate::val::{RecordId, RecordIdKey, TableName};

/// A boxed future returned by catalog-provider trait methods.
///
/// Boxes at the trait boundary so deep async chains
/// (executor → catalog provider → transaction) don't inflate the parent
/// state machine past the 2 MB tokio thread stack.
#[cfg(target_family = "wasm")]
pub(crate) type BoxProviderFut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
#[cfg(not(target_family = "wasm"))]
pub(crate) type BoxProviderFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// On native targets, catalog provider futures are `Send`, so default bodies
/// that capture `&Self` need `Self: Sync`. On WASM those futures are not `Send`,
/// so this is a blanket no-op bound.
#[cfg(not(target_family = "wasm"))]
pub(crate) trait ProviderFutureSendRequirement: Sync {}
#[cfg(not(target_family = "wasm"))]
impl<T: Sync + ?Sized> ProviderFutureSendRequirement for T {}

#[cfg(target_family = "wasm")]
pub(crate) trait ProviderFutureSendRequirement {}
#[cfg(target_family = "wasm")]
impl<T: ?Sized> ProviderFutureSendRequirement for T {}

pub(crate) trait NodeProvider: ProviderFutureSendRequirement {
	/// Retrieve all node definitions in a datastore.
	fn all_nodes(&self) -> BoxProviderFut<'_, Result<Arc<[Node]>>>;

	/// Retrieve a specific node definition.
	fn get_node(&self, id: Uuid) -> BoxProviderFut<'_, Result<Arc<Node>>>;
}

pub(crate) trait RootProvider: ProviderFutureSendRequirement {
	/// Retrieve a specific root definition.
	fn get_default_config(&self) -> BoxProviderFut<'_, Result<Option<Arc<DefaultConfig>>>>;

	/// Retrieve a specific config definition from the root.
	fn get_root_config<'a>(
		&'a self,
		cg: &'a str,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ConfigDefinition>>>>;

	/// Retrieve a specific config definition from the root returning an error if it does not exist.
	fn expect_root_config<'a>(
		&'a self,
		cg: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<catalog::ConfigDefinition>>> {
		Box::pin(async move {
			if let Some(val) = self.get_root_config(cg).await? {
				Ok(val)
			} else {
				Err(anyhow::Error::new(Error::CgNotFound {
					name: cg.to_owned(),
				}))
			}
		})
	}
}

pub(crate) trait NamespaceProvider: ProviderFutureSendRequirement {
	/// Retrieve all namespace definitions in a datastore.
	fn all_ns(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[NamespaceDefinition]>>>;

	/// Retrieve a specific namespace definition.
	fn get_ns_by_name<'a>(
		&'a self,
		ns: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<NamespaceDefinition>>>>;

	/// Get or add a namespace with a default configuration, only if we are in
	/// dynamic mode.
	fn get_or_add_ns<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<NamespaceDefinition>>> {
		Box::pin(
			async move {
				match self.get_ns_by_name(ns, None).await? {
					Some(val) => Ok(val),
					// The entry is not in the database
					None => {
						let ns = NamespaceDefinition {
							namespace_id: self.get_next_ns_id(ctx).await?,
							name: ns.into(),
							comment: None,
						};
						self.put_ns(ns).await
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_or_add_ns")),
		)
	}

	/// Get the next namespace id.
	fn get_next_ns_id<'a>(
		&'a self,
		ctx: Option<&'a Context>,
	) -> BoxProviderFut<'a, Result<NamespaceId>>;

	/// Put a namespace definition into the datastore.
	fn put_ns(
		&self,
		ns: NamespaceDefinition,
	) -> BoxProviderFut<'_, Result<Arc<NamespaceDefinition>>>;

	/// Delete a namespace definition.
	///
	/// Mirrors [`DatabaseProvider::del_db`]: clears the metadata entry
	/// and the namespace-prefixed data range, using soft (`del`/`delp`)
	/// or hard (`clr`/`clrp`) deletes depending on `expunge`. Returns
	/// `Some(())` when a definition was found and removed, `None`
	/// otherwise.
	fn del_ns<'a>(&'a self, ns: &'a str, expunge: bool) -> BoxProviderFut<'a, Result<Option<()>>>;

	/// Retrieve a specific namespace definition returning an error if it does not exist.
	fn expect_ns_by_name<'a>(
		&'a self,
		ns: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<NamespaceDefinition>>> {
		Box::pin(async move {
			match self.get_ns_by_name(ns, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::NsNotFound {
					name: ns.to_owned(),
				}),
			}
		})
	}
}

pub(crate) trait DatabaseProvider: NamespaceProvider {
	/// Retrieve all database definitions in a namespace.
	fn all_db(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[DatabaseDefinition]>>>;

	/// Retrieve a specific database definition.
	fn get_db_by_name<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<DatabaseDefinition>>>>;

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	fn get_or_add_db_upwards<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: &'a str,
		db: &'a str,
		upwards: bool,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>>;

	/// Get the next database id.
	fn get_next_db_id<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: NamespaceId,
	) -> BoxProviderFut<'a, Result<DatabaseId>>;

	/// Put a database definition into a namespace.
	fn put_db<'a>(
		&'a self,
		ns: &'a str,
		db: DatabaseDefinition,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>>;

	/// Delete a database definition.
	fn del_db<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		expunge: bool,
	) -> BoxProviderFut<'a, Result<Option<()>>>;

	/// Retrieve a specific database definition returning an error if it does not exist.
	fn expect_db_by_name<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>> {
		Box::pin(async move {
			match self.get_db_by_name(ns, db, None).await? {
				Some(val) => Ok(val),
				None => {
					// Check if the namespace exists.
					// If it doesn't, return a namespace not found error.
					self.expect_ns_by_name(ns).await?;

					// Return a database not found error.
					Err(anyhow::anyhow!(Error::DbNotFound {
						name: db.to_owned()
					}))
				}
			}
		})
	}

	/// Retrieve all analyzer definitions for a specific database.
	fn all_db_analyzers(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AnalyzerDefinition]>>>;

	/// Retrieve all sequences definitions for a specific database.
	fn all_db_sequences(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::SequenceDefinition]>>>;

	/// Retrieve all function definitions for a specific database.
	fn all_db_functions(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::FunctionDefinition]>>>;

	/// Retrieve all module definitions for a specific database.
	fn all_db_modules(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ModuleDefinition]>>>;

	/// Retrieve all param definitions for a specific database.
	fn all_db_params(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ParamDefinition]>>>;

	/// Retrieve all model definitions for a specific database.
	fn all_db_models(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::MlModelDefinition]>>>;

	/// Retrieve all config definitions for a specific database.
	fn all_db_configs(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ConfigDefinition]>>>;

	/// Retrieve a specific model definition from a database.
	fn get_db_model<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ml: &'a str,
		vn: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::MlModelDefinition>>>>;

	/// Retrieve a specific analyzer definition.
	fn get_db_analyzer<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		az: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::AnalyzerDefinition>>>;

	fn get_db_sequence<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::SequenceDefinition>>>;

	/// Retrieve a specific function definition from a database.
	fn get_db_function<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::FunctionDefinition>>>;

	/// Put a function definition into a database.
	fn put_db_function<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &'a catalog::FunctionDefinition,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Retrieve a specific module definition from a database.
	fn get_db_module<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::ModuleDefinition>>>;

	/// Put a module definition into a database.
	fn put_db_module<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &'a catalog::ModuleDefinition,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Retrieve a specific param definition from a database.
	fn get_db_param<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::ParamDefinition>>>;

	/// Put a param definition into a database.
	fn put_db_param<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &'a catalog::ParamDefinition,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Retrieve a specific config definition from a database.
	fn get_db_config<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ConfigDefinition>>>>;

	/// Retrieve a specific config definition from a database returning an error if it does not
	/// exist.
	fn expect_db_config<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<catalog::ConfigDefinition>>> {
		Box::pin(async move {
			if let Some(val) = self.get_db_config(ns, db, cg, None).await? {
				Ok(val)
			} else {
				Err(anyhow::Error::new(Error::CgNotFound {
					name: cg.to_owned(),
				}))
			}
		})
	}
}

pub(crate) trait TableProvider: ProviderFutureSendRequirement {
	/// Retrieve all table definitions for a specific database.
	fn all_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[TableDefinition]>>>;

	/// Retrieve all view definitions for a specific table.
	fn all_tb_views<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[TableDefinition]>>>;

	/// Retrieve a specific table definition.
	fn get_tb_by_name<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<TableDefinition>>>>;

	/// Retrieve a specific table definition returning an error if it does not exist.
	fn expect_tb_by_name<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>> {
		Box::pin(async move {
			match self.get_tb_by_name(ns, db, tb, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::TbNotFound {
					name: tb.to_owned(),
				}),
			}
		})
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode.
	fn get_or_add_tb<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>>;

	/// Get the next namespace id.
	fn get_next_tb_id<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: NamespaceId,
		db: DatabaseId,
	) -> BoxProviderFut<'a, Result<TableId>>;

	/// Put a table definition into a database.
	fn put_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableDefinition,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>>;

	/// Delete a table definition.
	fn del_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Clear a table definition.
	fn clr_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Retrieve all event definitions for a specific table.
	fn all_tb_events<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::EventDefinition]>>>;

	/// Retrieve all field definitions for a specific table.
	fn all_tb_fields<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::FieldDefinition]>>>;

	/// Retrieve all index definitions for a specific table.
	fn all_tb_indexes<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::IndexDefinition]>>>;

	/// Retrieve all live definitions for a specific table.
	fn all_tb_lives<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::SubscriptionDefinition]>>>;

	/// Retrieve a specific table definition.
	fn get_tb<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<TableDefinition>>>>;

	/// Retrieve a specific table definition returning an error if it does not exist.
	fn expect_tb<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>> {
		Box::pin(async move {
			match self.get_tb(ns, db, tb, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::TbNotFound {
					name: tb.to_owned(),
				}),
			}
		})
	}

	/// Retrieve an event for a table.
	fn get_tb_event<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ev: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::EventDefinition>>>;

	/// Retrieve a field for a table.
	fn get_tb_field<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		fd: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::FieldDefinition>>>>;

	/// Put a field definition into a table.
	fn put_tb_field<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		fd: &'a catalog::FieldDefinition,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Retrieve an index for a table.
	fn get_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::IndexDefinition>>>>;

	/// Retrieve an index for a table.
	fn get_tb_index_by_id<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::IndexDefinition>>>>;

	/// Retrieve an index for a table returning an error if it does not exist.
	fn expect_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<catalog::IndexDefinition>>> {
		Box::pin(async move {
			self.get_tb_index(ns, db, tb, ix, None).await?.ok_or_else(|| {
				Error::IxNotFound {
					name: ix.to_owned(),
				}
				.into()
			})
		})
	}

	/// Put an index for a table.
	fn put_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a catalog::IndexDefinition,
	) -> BoxProviderFut<'a, Result<()>>;

	fn del_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a str,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Fetch a specific record value.
	fn get_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<Record>>>;

	/// Fetch multiple specific record values.
	fn get_records<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: &'a [RecordId],
		version: Option<u64>,
		cache_policy: CachePolicy,
	) -> BoxProviderFut<'a, Result<Vec<Arc<Record>>>>;

	/// Check if a record exists.
	fn record_exists<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<bool>>;

	/// Put record into the datastore.
	///
	/// This will error if the record already exists.
	fn put_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		record: Arc<Record>,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Set record into the datastore.
	///
	/// This will replace the record if it already exists.
	fn set_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		record: Arc<Record>,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Delete record from the datastore.
	fn del_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
	) -> BoxProviderFut<'a, Result<()>>;
}

pub(crate) trait UserProvider: ProviderFutureSendRequirement {
	/// Retrieve all user definitions in a namespace.
	fn all_root_users(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[UserDefinition]>>>;

	/// Retrieve all namespace user definitions for a specific namespace.
	fn all_ns_users(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::UserDefinition]>>>;

	/// Retrieve all database user definitions for a specific database.
	fn all_db_users(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[UserDefinition]>>>;

	/// Retrieve a specific root user definition.
	fn get_root_user<'a>(
		&'a self,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<UserDefinition>>>>;

	/// Put a user definition into a root.
	fn put_root_user<'a>(&'a self, us: &'a UserDefinition) -> BoxProviderFut<'a, Result<()>>;

	/// Retrieve a specific namespace user definition.
	fn get_ns_user<'a>(
		&'a self,
		ns: NamespaceId,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<UserDefinition>>>>;

	/// Put a user definition into a namespace.
	fn put_ns_user<'a>(
		&'a self,
		ns: NamespaceId,
		us: &'a UserDefinition,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Retrieve a specific user definition from a database.
	fn get_db_user<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<UserDefinition>>>>;

	/// Put a user definition into a database.
	fn put_db_user<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &'a UserDefinition,
	) -> BoxProviderFut<'a, Result<()>>;

	/// Retrieve a specific user definition from a root returning an error if it does not exist.
	fn expect_root_user<'a>(
		&'a self,
		us: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<UserDefinition>>> {
		Box::pin(async move {
			match self.get_root_user(us, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::UserRootNotFound {
					name: us.to_owned(),
				}),
			}
		})
	}

	/// Retrieve a specific user definition from a namespace returning an error if it does not
	/// exist.
	#[allow(unused)]
	fn expect_ns_user<'a>(
		&'a self,
		ns: NamespaceId,
		us: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<UserDefinition>>> {
		Box::pin(async move {
			match self.get_ns_user(ns, us, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::UserNsNotFound {
					name: us.to_owned(),
					ns: ns.to_string(),
				}),
			}
		})
	}

	/// Retrieve a specific user definition from a database returning an error if it does not exist.
	#[allow(unused)]
	fn expect_db_user<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<UserDefinition>>> {
		Box::pin(async move {
			match self.get_db_user(ns, db, us, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::UserDbNotFound {
					name: us.to_owned(),
					ns: ns.to_string(),
					db: db.to_string(),
				}),
			}
		})
	}
}

pub(crate) trait AuthorisationProvider: ProviderFutureSendRequirement {
	/// Retrieve all ROOT level accesses in a datastore.
	fn all_root_accesses(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>>;

	/// Retrieve all root access grants in a datastore.
	fn all_root_access_grants<'a>(
		&'a self,
		ra: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>>;

	/// Retrieve all namespace access definitions for a specific namespace.
	fn all_ns_accesses(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>>;

	/// Retrieve all namespace access grants for a specific namespace.
	fn all_ns_access_grants<'a>(
		&'a self,
		ns: NamespaceId,
		na: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>>;

	/// Retrieve all database access definitions for a specific database.
	fn all_db_accesses(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>>;

	/// Retrieve all database access grants for a specific database.
	fn all_db_access_grants<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>>;

	/// Retrieve a specific root access definition.
	fn get_root_access<'a>(
		&'a self,
		ra: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>>;

	/// Retrieve a specific root access definition returning an error if it does not exist.
	fn expect_root_access<'a>(
		&'a self,
		ra: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<catalog::AccessDefinition>>> {
		Box::pin(async move {
			match self.get_root_access(ra, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::AccessRootNotFound {
					ac: ra.to_owned(),
				}),
			}
		})
	}

	/// Retrieve a specific root access grant.
	fn get_root_access_grant<'a>(
		&'a self,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>>;

	/// Retrieve a specific namespace access definition.
	fn get_ns_access<'a>(
		&'a self,
		ns: NamespaceId,
		na: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>>;

	/// Retrieve a specific namespace access grant.
	fn get_ns_access_grant<'a>(
		&'a self,
		ns: NamespaceId,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>>;

	/// Retrieve a specific database access definition.
	fn get_db_access<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>>;

	/// Retrieve a specific database access grant.
	fn get_db_access_grant<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>>;

	/// Delete a root access definition.
	fn del_root_access<'a>(&'a self, ra: &'a str) -> BoxProviderFut<'a, Result<()>>;

	/// Delete a namespace access definition.
	fn del_ns_access<'a>(&'a self, ns: NamespaceId, na: &'a str) -> BoxProviderFut<'a, Result<()>>;

	/// Delete a database access definition.
	fn del_db_access<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
	) -> BoxProviderFut<'a, Result<()>>;
}

pub(crate) trait ApiProvider: ProviderFutureSendRequirement {
	/// Retrieve all api definitions for a specific database.
	fn all_db_apis(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ApiDefinition]>>>;

	/// Retrieve a specific api definition.
	fn get_db_api<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ApiDefinition>>>>;

	/// Put an api definition into a database.
	fn put_db_api<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &'a catalog::ApiDefinition,
	) -> BoxProviderFut<'a, Result<()>>;
}

pub(crate) trait BucketProvider: ProviderFutureSendRequirement {
	/// Retrieve all bucket definitions for a specific database.
	fn all_db_buckets(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::BucketDefinition]>>>;

	/// Retrieve a specific bucket definition.
	fn get_db_bucket<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::BucketDefinition>>>>;

	/// Retrieve a specific bucket definition returning an error if it does not exist.
	fn expect_db_bucket<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<catalog::BucketDefinition>>> {
		Box::pin(async move {
			match self.get_db_bucket(ns, db, bu, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(Error::BuNotFound {
					name: bu.to_owned(),
				}),
			}
		})
	}
}

pub(crate) trait CatalogProvider:
	NodeProvider
	+ NamespaceProvider
	+ DatabaseProvider
	+ TableProvider
	+ UserProvider
	+ AuthorisationProvider
	+ ApiProvider
	+ BucketProvider
{
	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	fn get_or_add_db<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: &'a str,
		db: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>> {
		Box::pin(async move { self.get_or_add_db_upwards(ctx, ns, db, false).await })
	}

	/// Ensures that the given namespace and database exist. If they do not, they will be created.
	fn ensure_ns_db<'a>(
		&'a self,
		ctx: Option<&'a Context>,
		ns: &'a str,
		db: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>> {
		Box::pin(async move { self.get_or_add_db_upwards(ctx, ns, db, true).await })
	}
}
