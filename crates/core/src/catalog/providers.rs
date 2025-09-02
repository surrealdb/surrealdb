use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::{
	DatabaseDefinition, DatabaseId, NamespaceDefinition, NamespaceId, UserDefinition, TableDefinition,
};
use crate::err::Error;

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

    async fn get_db_by_name(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Option<Arc<DatabaseDefinition>>>;

    /// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
    async fn get_or_add_db_upwards(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<DatabaseDefinition>>;

    async fn put_db(
		&self,
		ns: &str,
		db: DatabaseDefinition,
	) -> Result<Arc<DatabaseDefinition>>;


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

	async fn expect_tb_by_name(&self, ns: &str, db: &str, tb: &str) -> Result<Arc<TableDefinition>> {
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

}

#[async_trait]
pub trait UserProvider {
	/// Retrieve all user definitions in a namespace.
	async fn all_root_users(&self) -> Result<Arc<[UserDefinition]>>;

	/// Retrieve all database user definitions for a specific database.
	async fn all_db_users(&self, ns: NamespaceId, db: DatabaseId) -> Result<Arc<[UserDefinition]>>;

	/// Retrieve a specific root user definition.
	async fn get_root_user(&self, us: &str) -> Result<Option<Arc<UserDefinition>>>;

	/// Retrieve a specific namespace user definition.
	async fn get_ns_user(&self, ns: NamespaceId, us: &str) -> Result<Option<Arc<UserDefinition>>>;

	/// Retrieve a specific user definition from a database.
	async fn get_db_user(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &str,
	) -> Result<Option<Arc<UserDefinition>>>;

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

pub trait CatalogProvider: NamespaceProvider + DatabaseProvider + TableProvider + UserProvider {}
