use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use crate::catalog;
use crate::catalog::{
	DatabaseDefinition, DatabaseId, NamespaceDefinition, NamespaceId, TableDefinition,
	UserDefinition,
};
use crate::val::RecordIdKey;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::val::record::Record;


#[async_trait]
pub trait NodeProvider {
	async fn all_nodes(&self) -> Result<Arc<[Node]>>;
	async fn get_node(&self, id: Uuid) -> Result<Arc<Node>>;
}

#[async_trait]
pub trait NamespaceProvider {
	/// Retrieve all namespace definitions in a datastore.
	async fn all_ns(&self) -> Result<Arc<[NamespaceDefinition]>>;

	async fn get_ns_by_name(&self, ns: &str) -> Result<Option<Arc<NamespaceDefinition>>>;

	/// Get or add a namespace with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_or_add_ns(&self, ns: &str, strict: bool) -> Result<Arc<NamespaceDefinition>> {
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
					namespace_id: self.get_next_ns_id().await?,
					name: ns.to_owned(),
					comment: None,
				};

				return self.put_ns(ns).await;
			}
		}
	}

	async fn get_next_ns_id(&self) -> Result<NamespaceId>;

	async fn put_ns(&self, ns: NamespaceDefinition) -> Result<Arc<NamespaceDefinition>>;

	async fn expect_ns_by_name(&self, ns: &str) -> Result<Arc<NamespaceDefinition>> {
		match self.get_ns_by_name(ns).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::NsNotFound {
				name: ns.to_owned(),
			}),
		}
	}
}

#[async_trait]
pub trait DatabaseProvider: NamespaceProvider {
	/// Retrieve all database definitions in a namespace.
	async fn all_db(&self, ns: NamespaceId) -> Result<Arc<[DatabaseDefinition]>>;

	async fn get_db_by_name(&self, ns: &str, db: &str) -> Result<Option<Arc<DatabaseDefinition>>>;

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	async fn get_or_add_db_upwards(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<DatabaseDefinition>>;

	async fn put_db(&self, ns: &str, db: DatabaseDefinition) -> Result<Arc<DatabaseDefinition>>;

	async fn del_db(&self, ns: &str, db: &str, expunge: bool) -> Result<Option<()>>;

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
	async fn put_db_function(&self, ns: NamespaceId, db: DatabaseId, fc: &catalog::FunctionDefinition) -> Result<()>;

	/// Retrieve a specific function definition from a database.
	async fn get_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &str,
	) -> Result<Arc<catalog::ParamDefinition>>;

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

#[async_trait]
pub trait TableProvider {
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

	async fn get_tb_by_name(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Option<Arc<TableDefinition>>>;

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
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<TableDefinition>>;

	async fn put_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &TableDefinition,
	) -> Result<Arc<TableDefinition>>;

	async fn del_tb(&self, ns: &str, db: &str, tb: &str) -> Result<()>;

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

	async fn put_tb_field(&self, ns: NamespaceId, db: DatabaseId, tb: &str, fd: &catalog::FieldDefinition) -> Result<()>;

	/// Retrieve an index for a table.
	async fn get_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
	) -> Result<Arc<catalog::IndexDefinition>>;

	/// Fetch a specific record value.
	async fn get_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Arc<Record>>;

	async fn set_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Record,
	) -> Result<()>;

	fn set_record_cache(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Arc<Record>,
	) -> Result<()>;

	async fn del_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
	) -> Result<()>;
}

#[async_trait]
pub trait UserProvider {
	/// Retrieve all user definitions in a namespace.
	async fn all_root_users(&self) -> Result<Arc<[UserDefinition]>>;

	/// Retrieve all namespace user definitions for a specific namespace.
	async fn all_ns_users(&self, ns: NamespaceId) -> Result<Arc<[catalog::UserDefinition]>>;

	/// Retrieve all database user definitions for a specific database.
	async fn all_db_users(&self, ns: NamespaceId, db: DatabaseId) -> Result<Arc<[UserDefinition]>>;

	/// Retrieve a specific root user definition.
	async fn get_root_user(&self, us: &str) -> Result<Option<Arc<UserDefinition>>>;

	async fn put_root_user(&self, us: &UserDefinition) -> Result<()>;

	/// Retrieve a specific namespace user definition.
	async fn get_ns_user(&self, ns: NamespaceId, us: &str) -> Result<Option<Arc<UserDefinition>>>;

	async fn put_ns_user(&self, ns: NamespaceId, us: &UserDefinition) -> Result<()>;

	/// Retrieve a specific user definition from a database.
	async fn get_db_user(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &str,
	) -> Result<Option<Arc<UserDefinition>>>;

	async fn put_db_user(&self, ns: NamespaceId, db: DatabaseId, us: &UserDefinition) -> Result<()>;

	async fn expect_root_user(&self, us: &str) -> Result<Arc<UserDefinition>> {
		match self.get_root_user(us).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::UserRootNotFound {
				name: us.to_owned(),
			}),
		}
	}

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

#[async_trait]
pub trait AuthorisationProvider {
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

	async fn del_root_access(&self, ra: &str) -> Result<()>;

	async fn del_ns_access(&self, ns: NamespaceId, na: &str) -> Result<()>;

	async fn del_db_access(&self, ns: NamespaceId, db: DatabaseId, da: &str) -> Result<()>;
}

#[async_trait]
pub trait ApiProvider {
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
	) -> Result<Arc<catalog::ApiDefinition>>;

	async fn put_db_api(&self, ns: NamespaceId, db: DatabaseId, ap: &catalog::ApiDefinition) -> Result<()>;
}

#[async_trait]
pub trait BucketProvider {
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

#[async_trait]
pub trait CatalogProvider:
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
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		self.get_or_add_db_upwards(ns, db, strict, false).await
	}

	async fn ensure_ns_db(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		self.get_or_add_db_upwards(ns, db, strict, true).await
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode.
	async fn get_or_add_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<TableDefinition>> {
		self.get_or_add_tb_upwards(ns, db, tb, strict, false).await
	}

	/// Ensures that a table, database, and namespace are all fully defined.
	async fn ensure_ns_db_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<TableDefinition>> {
		self.get_or_add_tb_upwards(ns, db, tb, strict, true).await
	}

	/// Ensure a specific table (and database, and namespace) exist.
	async fn check_ns_db_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<()> {
		if !strict {
			return Ok(());
		}

		let db = match self.get_db_by_name(ns, db).await? {
			Some(db) => db,
			None => {
				return Err(Error::DbNotFound {
					name: db.to_owned(),
				}
				.into());
			}
		};

		match self.get_tb(db.namespace_id, db.database_id, tb).await? {
			Some(tb) => tb,
			None => {
				return Err(Error::TbNotFound {
					name: tb.to_owned(),
				}
				.into());
			}
		};
		Ok(())
	}
}
