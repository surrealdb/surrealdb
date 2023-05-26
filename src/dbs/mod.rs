use crate::cli::CF;
use crate::err::Error;
use crate::iam::{ROOT_USER, ROOT_PASS};
use once_cell::sync::OnceCell;
use surrealdb::dbs::{Session, Auth};
use surrealdb::kvs::Datastore;

pub static DB: OnceCell<Datastore> = OnceCell::new();

const LOG: &str = "surrealdb::dbs";

pub async fn init() -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log authentication options
	match opt.strict {
		true => info!(target: LOG, "Database strict mode is enabled"),
		false => info!(target: LOG, "Database strict mode is disabled"),
	};
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path).await?;
	
	// Bootstrap the datastore
	setup(&dbs).await?;

	// Store database instance
	let _ = DB.set(dbs);

	// All ok
	Ok(())
}

// Base setup for the datastore
async fn setup(ds: &Datastore) -> Result<(), Error>  {
	// Setup the superuser if necessary
	setup_superuser(ds).await?;

	Ok(())
}

// Setup the superuser if necessary
async fn setup_superuser(ds: &Datastore) -> Result<(), Error> {
	if !Auth::is_enabled() {
		trace!(target: LOG, "Authentication is disabled. Skipping superuser setup");
		return Ok(());
	}

	let mut txn = ds.transaction(false, false).await?;

	// If there are no KV users in the datastore, create the default superuser
	match txn.all_kv_users().await {
		Ok(val) if val.is_empty() => {
			warn!(target: LOG, "No root users found. Creating superuser '{ROOT_USER}' with password '{ROOT_PASS}'. Change it right away!");

			let sql = format!("DEFINE USER {ROOT_USER} ON KV PASSWORD '{ROOT_PASS}'");
			let sess = Session::for_kv();
			ds.execute(&sql, &sess, None, false).await?;
			Ok(())
		}
		Ok(_) => Ok(()),
		Err(e) => Err(e.into()),
	}
}

#[cfg(test)]
mod tests {
    use surrealdb::kvs::Datastore;

    use crate::iam::{verify::verify_creds, ROOT_USER, ROOT_PASS};

	use super::*;

	#[tokio::test]
	async fn test_setup_superuser() {
		let ds = Datastore::new("memory").await.unwrap();

		// Setup the root user if there are no KV users
		assert_eq!(ds.transaction(false, false).await.unwrap().all_kv_users().await.unwrap().len(), 0);
		super::setup_superuser(&ds).await.unwrap();
		assert_eq!(ds.transaction(false, false).await.unwrap().all_kv_users().await.unwrap().len(), 1);
		verify_creds(&ds, None, None, ROOT_USER, ROOT_PASS).await.unwrap();

		// Do not setup the root user if there are KV users.
		// Test by making sure the custom password doesn't change.
		let sql = format!("DEFINE USER {ROOT_USER} ON KV PASSWORD 'test'");
		let sess = Session::for_kv();
		ds.execute(&sql, &sess, None, false).await.unwrap();
		let pass_hash = ds.transaction(false, false).await.unwrap().get_kv_user(ROOT_USER).await.unwrap().hash;
		super::setup_superuser(&ds).await.unwrap();
		assert_eq!(pass_hash, ds.transaction(false, false).await.unwrap().get_kv_user(ROOT_USER).await.unwrap().hash)
	}
}
