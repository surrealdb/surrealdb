//! Catalog providers.
//!
//! Providers are used as the data access layer for the catalog.

use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog;
use crate::catalog::{
	DatabaseDefinition, DatabaseId, IndexId, NamespaceDefinition, NamespaceId, TableDefinition,
	TableId, UserDefinition,
};
use crate::ctx::MutableContext;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::val::RecordIdKey;
use crate::val::record::Record;

/// SurrealDB Node provider.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub(crate) trait NodeProvider {
	/// Retrieve all node definitions in a datastore.
	async fn all_nodes(&self) -> Result<Arc<[Node]>>;

	/// Retrieve a specific node definition.
	async fn get_node(&self, id: Uuid) -> Result<Arc<Node>>;
}

/// Namespace data access provider.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub(crate) trait NamespaceProvider {
	/// Retrieve all namespace definitions in a datastore.
	async fn all_ns(&self) -> Result<Arc<[NamespaceDefinition]>>;

	/// Retrieve a specific namespace definition.
	async fn get_ns_by_name(&self, ns: &str) -> Result<Option<Arc<NamespaceDefinition>>>;

	/// Get or add a namespace with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self, ctx))]
	async fn get_or_add_ns(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		strict: bool,
	) -> Result<Arc<NamespaceDefinition>> {
		match self.get_ns_by_name(ns).await? {
			Some(val) => Ok(val),
			// The entry is not in the database
			None => {
				if strict {
					return Err(Error::NsNotFound {
						name: ns.to_owned(),
					}
					.into());
				}
				let ns = NamespaceDefinition {
					namespace_id: self.get_next_ns_id(ctx).await?,
					name: ns.to_owned(),
					comment: None,
				};
				self.put_ns(ns).await
			}
		}
	}

	/// Get the next namespace id.
	async fn get_next_ns_id(&self, ctx: Option<&MutableContext>) -> Result<NamespaceId>;

	/// Put a namespace definition into the datastore.
	async fn put_ns(&self, ns: NamespaceDefinition) -> Result<Arc<NamespaceDefinition>>;

	/// Retrieve a specific namespace definition returning an error if it does not exist.
	async fn expect_ns_by_name(&self, ns: &str) -> Result<Arc<NamespaceDefinition>> {
		match self.get_ns_by_name(ns).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::NsNotFound {
				name: ns.to_owned(),
			}),
		}
	}
}

/// Database data access provider.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub(crate) trait DatabaseProvider: NamespaceProvider {
	/// Retrieve all database definitions in a namespace.
	async fn all_db(&self, ns: NamespaceId) -> Result<Arc<[DatabaseDefinition]>>;

	/// Retrieve a specific database definition.
	async fn get_db_by_name(&self, ns: &str, db: &str) -> Result<Option<Arc<DatabaseDefinition>>>;

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	async fn get_or_add_db_upwards(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		db: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<DatabaseDefinition>>;

	/// Get the next database id.
	async fn get_next_db_id(
		&self,
		ctx: Option<&MutableContext>,
		ns: NamespaceId,
	) -> Result<DatabaseId>;

	/// Put a database definition into a namespace.
	async fn put_db(&self, ns: &str, db: DatabaseDefinition) -> Result<Arc<DatabaseDefinition>>;

	/// Delete a database definition.
	async fn del_db(&self, ns: &str, db: &str, expunge: bool) -> Result<Option<()>>;

	/// Retrieve a specific database definition returning an error if it does not exist.
	async fn expect_db_by_name(&self, ns: &str, db: &str) -> Result<Arc<DatabaseDefinition>> {
		match self.get_db_by_name(ns, db).await? {
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
	}

	/// Retrieve all analyzer definitions for a specific database.
	async fn all_db_analyzers(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::AnalyzerDefinition]>>;

	/// Retrieve all sequences definitions for a specific database.
	async fn all_db_sequences(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::SequenceDefinition]>>;

	/// Retrieve all function definitions for a specific database.
	async fn all_db_functions(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::FunctionDefinition]>>;

	/// Retrieve all param definitions for a specific database.
	async fn all_db_params(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::ParamDefinition]>>;

	/// Retrieve all model definitions for a specific database.
	async fn all_db_models(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::MlModelDefinition]>>;

	/// Retrieve all model definitions for a specific database.
	async fn all_db_configs(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::ConfigDefinition]>>;

	/// Retrieve a specific model definition from a database.
	async fn get_db_model(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ml: &str,
		vn: &str,
	) -> Result<Option<Arc<catalog::MlModelDefinition>>>;

	/// Retrieve a specific analyzer definition.
	async fn get_db_analyzer(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		az: &str,
	) -> Result<Arc<catalog::AnalyzerDefinition>>;

	async fn get_db_sequence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Result<Arc<catalog::SequenceDefinition>>;

	/// Retrieve a specific function definition from a database.
	async fn get_db_function(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &str,
	) -> Result<Arc<catalog::FunctionDefinition>>;

	/// Put a function definition into a database.
	async fn put_db_function(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &catalog::FunctionDefinition,
	) -> Result<()>;

	/// Retrieve a specific function definition from a database.
	async fn get_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &str,
	) -> Result<Arc<catalog::ParamDefinition>>;

	/// Put a param definition into a database.
	async fn put_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &catalog::ParamDefinition,
	) -> Result<()>;

	/// Retrieve a specific config definition from a database.
	async fn get_db_config(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &str,
	) -> Result<Option<Arc<catalog::ConfigDefinition>>>;

	/// Retrieve a specific config definition from a database.
	async fn expect_db_config(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &str,
	) -> Result<Arc<catalog::ConfigDefinition>> {
		if let Some(val) = self.get_db_config(ns, db, cg).await? {
			Ok(val)
		} else {
			Err(anyhow::Error::new(Error::CgNotFound {
				name: cg.to_owned(),
			}))
		}
	}
}

/// Table data access provider.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub(crate) trait TableProvider {
	/// Retrieve all table definitions for a specific database.
	async fn all_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> Result<Arc<[TableDefinition]>>;

	/// Retrieve all view definitions for a specific table.
	async fn all_tb_views(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[TableDefinition]>>;

	/// Retrieve a specific table definition.
	async fn get_tb_by_name(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Option<Arc<TableDefinition>>>;

	/// Retrieve a specific table definition returning an error if it does not exist.
	async fn expect_tb_by_name(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<TableDefinition>> {
		match self.get_tb_by_name(ns, db, tb).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::TbNotFound {
				name: tb.to_owned(),
			}),
		}
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode.
	async fn get_or_add_tb_upwards(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<TableDefinition>>;

	/// Get the next namespace id.
	async fn get_next_tb_id(
		&self,
		ctx: Option<&MutableContext>,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<TableId>;

	/// Put a table definition into a database.
	async fn put_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &TableDefinition,
	) -> Result<Arc<TableDefinition>>;

	/// Delete a table definition.
	async fn del_tb(&self, ns: &str, db: &str, tb: &str) -> Result<()>;

	/// Clear a table definition.
	async fn clr_tb(&self, ns: &str, db: &str, tb: &str) -> Result<()>;

	/// Retrieve all event definitions for a specific table.
	async fn all_tb_events(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[catalog::EventDefinition]>>;

	/// Retrieve all field definitions for a specific table.
	async fn all_tb_fields(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		version: Option<u64>,
	) -> Result<Arc<[catalog::FieldDefinition]>>;

	/// Retrieve all index definitions for a specific table.
	async fn all_tb_indexes(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[catalog::IndexDefinition]>>;

	/// Retrieve all live definitions for a specific table.
	async fn all_tb_lives(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[catalog::SubscriptionDefinition]>>;

	/// Retrieve a specific table definition.
	async fn get_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Option<Arc<TableDefinition>>>;

	/// Check if a table exists.
	async fn check_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		strict: bool,
	) -> Result<()> {
		if !strict {
			return Ok(());
		}
		self.expect_tb(ns, db, tb).await?;
		Ok(())
	}

	/// Retrieve a specific table definition returning an error if it does not exist.
	async fn expect_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<TableDefinition>> {
		match self.get_tb(ns, db, tb).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::TbNotFound {
				name: tb.to_owned(),
			}),
		}
	}

	/// Retrieve an event for a table.
	async fn get_tb_event(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ev: &str,
	) -> Result<Arc<catalog::EventDefinition>>;

	/// Retrieve a field for a table.
	async fn get_tb_field(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		fd: &str,
	) -> Result<Option<Arc<catalog::FieldDefinition>>>;

	/// Put a field definition into a table.
	async fn put_tb_field(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		fd: &catalog::FieldDefinition,
	) -> Result<()>;

	/// Retrieve an index for a table.
	async fn get_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
	) -> Result<Option<Arc<catalog::IndexDefinition>>>;

	/// Retrieve an index for a table.
	async fn get_tb_index_by_id(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: IndexId,
	) -> Result<Option<Arc<catalog::IndexDefinition>>>;

	/// Retrieve an index for a table returning an error if it does not exist.
	async fn expect_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
	) -> Result<Arc<catalog::IndexDefinition>> {
		self.get_tb_index(ns, db, tb, ix).await?.ok_or_else(|| {
			Error::IxNotFound {
				name: ix.to_owned(),
			}
			.into()
		})
	}

	/// Put an index for a table.
	async fn put_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &catalog::IndexDefinition,
	) -> Result<()>;

	async fn del_tb_index(&self, ns: NamespaceId, db: DatabaseId, tb: &str, ix: &str)
	-> Result<()>;

	/// Fetch a specific record value.
	async fn get_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Arc<Record>>;

	/// Check if a record exists.
	async fn record_exists(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
	) -> Result<bool>;

	/// Put record into the datastore.
	///
	/// This will error if the record already exists.
	async fn put_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Arc<Record>,
		version: Option<u64>,
	) -> Result<()>;

	/// Set record into the datastore.
	///
	/// This will replace the record if it already exists.
	async fn set_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Arc<Record>,
		version: Option<u64>,
	) -> Result<()>;

	/// Delete record from the datastore.
	async fn del_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
	) -> Result<()>;
}

/// User data access provider.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub(crate) trait UserProvider {
	/// Retrieve all user definitions in a namespace.
	async fn all_root_users(&self) -> Result<Arc<[UserDefinition]>>;

	/// Retrieve all namespace user definitions for a specific namespace.
	async fn all_ns_users(&self, ns: NamespaceId) -> Result<Arc<[catalog::UserDefinition]>>;

	/// Retrieve all database user definitions for a specific database.
	async fn all_db_users(&self, ns: NamespaceId, db: DatabaseId) -> Result<Arc<[UserDefinition]>>;

	/// Retrieve a specific root user definition.
	async fn get_root_user(&self, us: &str) -> Result<Option<Arc<UserDefinition>>>;

	/// Put a user definition into a root.
	async fn put_root_user(&self, us: &UserDefinition) -> Result<()>;

	/// Retrieve a specific namespace user definition.
	async fn get_ns_user(&self, ns: NamespaceId, us: &str) -> Result<Option<Arc<UserDefinition>>>;

	/// Put a user definition into a namespace.
	async fn put_ns_user(&self, ns: NamespaceId, us: &UserDefinition) -> Result<()>;

	/// Retrieve a specific user definition from a database.
	async fn get_db_user(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &str,
	) -> Result<Option<Arc<UserDefinition>>>;

	/// Put a user definition into a database.
	async fn put_db_user(&self, ns: NamespaceId, db: DatabaseId, us: &UserDefinition)
	-> Result<()>;

	/// Retrieve a specific user definition from a root returning an error if it does not exist.
	async fn expect_root_user(&self, us: &str) -> Result<Arc<UserDefinition>> {
		match self.get_root_user(us).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::UserRootNotFound {
				name: us.to_owned(),
			}),
		}
	}

	/// Retrieve a specific user definition from a namespace returning an error if it does not
	/// exist.
	#[allow(unused)]
	async fn expect_ns_user(&self, ns: NamespaceId, us: &str) -> Result<Arc<UserDefinition>> {
		match self.get_ns_user(ns, us).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::UserNsNotFound {
				name: us.to_owned(),
				ns: ns.to_string(),
			}),
		}
	}

	/// Retrieve a specific user definition from a database returning an error if it does not exist.
	#[allow(unused)]
	async fn expect_db_user(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &str,
	) -> Result<Arc<UserDefinition>> {
		match self.get_db_user(ns, db, us).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::UserDbNotFound {
				name: us.to_owned(),
				ns: ns.to_string(),
				db: db.to_string(),
			}),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub(crate) trait AuthorisationProvider {
	/// Retrieve all ROOT level accesses in a datastore.
	async fn all_root_accesses(&self) -> Result<Arc<[catalog::AccessDefinition]>>;

	/// Retrieve all root access grants in a datastore.
	async fn all_root_access_grants(&self, ra: &str) -> Result<Arc<[catalog::AccessGrant]>>;

	/// Retrieve all namespace access definitions for a specific namespace.
	async fn all_ns_accesses(&self, ns: NamespaceId) -> Result<Arc<[catalog::AccessDefinition]>>;

	/// Retrieve all namespace access grants for a specific namespace.
	async fn all_ns_access_grants(
		&self,
		ns: NamespaceId,
		na: &str,
	) -> Result<Arc<[catalog::AccessGrant]>>;

	/// Retrieve all database access definitions for a specific database.
	async fn all_db_accesses(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::AccessDefinition]>>;

	/// Retrieve all database access grants for a specific database.
	async fn all_db_access_grants(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &str,
	) -> Result<Arc<[catalog::AccessGrant]>>;

	/// Retrieve a specific root access definition.
	async fn get_root_access(&self, ra: &str) -> Result<Option<Arc<catalog::AccessDefinition>>>;

	/// Retrieve a specific root access definition returning an error if it does not exist.
	async fn expect_root_access(&self, ra: &str) -> Result<Arc<catalog::AccessDefinition>> {
		match self.get_root_access(ra).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::AccessRootNotFound {
				ac: ra.to_owned(),
			}),
		}
	}

	/// Retrieve a specific root access grant.
	async fn get_root_access_grant(
		&self,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<catalog::AccessGrant>>>;

	/// Retrieve a specific namespace access definition.
	async fn get_ns_access(
		&self,
		ns: NamespaceId,
		na: &str,
	) -> Result<Option<Arc<catalog::AccessDefinition>>>;

	/// Retrieve a specific namespace access grant.
	async fn get_ns_access_grant(
		&self,
		ns: NamespaceId,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<catalog::AccessGrant>>>;

	/// Retrieve a specific database access definition.
	async fn get_db_access(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &str,
	) -> Result<Option<Arc<catalog::AccessDefinition>>>;

	/// Retrieve a specific database access grant.
	async fn get_db_access_grant(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<catalog::AccessGrant>>>;

	/// Delete a root access definition.
	async fn del_root_access(&self, ra: &str) -> Result<()>;

	/// Delete a namespace access definition.
	async fn del_ns_access(&self, ns: NamespaceId, na: &str) -> Result<()>;

	/// Delete a database access definition.
	async fn del_db_access(&self, ns: NamespaceId, db: DatabaseId, da: &str) -> Result<()>;
}

/// API data access provider.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub(crate) trait ApiProvider {
	/// Retrieve all api definitions for a specific database.
	async fn all_db_apis(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::ApiDefinition]>>;

	/// Retrieve a specific api definition.
	async fn get_db_api(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &str,
	) -> Result<Option<Arc<catalog::ApiDefinition>>>;

	/// Put an api definition into a database.
	async fn put_db_api(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &catalog::ApiDefinition,
	) -> Result<()>;
}

/// Bucket data access provider.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub(crate) trait BucketProvider {
	/// Retrieve all bucket definitions for a specific database.
	async fn all_db_buckets(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::BucketDefinition]>>;

	/// Retrieve a specific bucket definition.
	async fn get_db_bucket(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Option<Arc<catalog::BucketDefinition>>>;

	/// Retrieve a specific bucket definition returning an error if it does not exist.
	async fn expect_db_bucket(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Arc<catalog::BucketDefinition>> {
		match self.get_db_bucket(ns, db, bu).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::BuNotFound {
				name: bu.to_owned(),
			}),
		}
	}
}

/// The catalog provider is a trait that provides access to the catalog of the datastore.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
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
	async fn get_or_add_db(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		self.get_or_add_db_upwards(ctx, ns, db, strict, false).await
	}

	/// Ensures that the given namespace and database exist. If they do not, they will be created.
	async fn ensure_ns_db(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		self.get_or_add_db_upwards(ctx, ns, db, strict, true).await
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode.
	async fn get_or_add_tb(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<TableDefinition>> {
		self.get_or_add_tb_upwards(ctx, ns, db, tb, strict, false).await
	}

	/// Ensures that a table, database, and namespace are all fully defined.
	async fn ensure_ns_db_tb(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<TableDefinition>> {
		self.get_or_add_tb_upwards(ctx, ns, db, tb, strict, true).await
	}
}
