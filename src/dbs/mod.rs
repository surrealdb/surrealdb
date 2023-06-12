use std::time::Duration;

use crate::cli::CF;
use crate::err::Error;
use clap::Args;
use once_cell::sync::OnceCell;
use surrealdb::kvs::Datastore;
use surrealdb::opt::auth::Root;

pub static DB: OnceCell<Datastore> = OnceCell::new();

const LOG: &str = "surrealdb::dbs";

#[derive(Args, Debug)]
pub struct StartCommandDbsOptions {
	#[arg(help = "The maximum duration of any query")]
	#[arg(env = "SURREAL_QUERY_TIMEOUT", long)]
	#[arg(value_parser = super::cli::validator::duration)]
	query_timeout: Option<Duration>,
}

pub async fn init(
	StartCommandDbsOptions {
		query_timeout,
	}: StartCommandDbsOptions,
) -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log authentication options
	match opt.strict {
		true => info!(target: LOG, "Database strict mode is enabled"),
		false => info!(target: LOG, "Database strict mode is disabled"),
	};

	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path).await?.query_timeout(query_timeout).auth(!opt.no_auth);
	if let Some(user) = opt.user.as_ref() {
		dbs.setup_initial_creds(Root {
			username: &user,
			password: &opt.pass.as_ref().unwrap(),
		})
		.await?;
	}

	// Store database instance
	let _ = DB.set(dbs);

	// All ok
	Ok(())
}

#[cfg(test)]
mod tests {
	use surrealdb::dbs::Session;
	use surrealdb::iam::verify::verify_creds;
	use surrealdb::kvs::Datastore;

	use super::*;

	#[tokio::test]
	async fn test_setup_superuser() {
		let ds = Datastore::new("memory").await.unwrap();
		let creds = Root {
			username: "root",
			password: "root",
		};

		// Setup the root user if there are no KV users
		assert_eq!(
			ds.transaction(false, false).await.unwrap().all_kv_users().await.unwrap().len(),
			0
		);
		ds.setup_initial_creds(creds).await.unwrap();
		assert_eq!(
			ds.transaction(false, false).await.unwrap().all_kv_users().await.unwrap().len(),
			1
		);
		verify_creds(&ds, None, None, creds.username, creds.password).await.unwrap();

		// Do not setup the root user if there are KV users.
		// Test the scenario by making sure the custom password doesn't change.
		let sql = format!("DEFINE USER root ON KV PASSWORD 'test'");
		let sess = Session::for_kv();
		ds.execute(&sql, &sess, None, false).await.unwrap();
		let pass_hash = ds
			.transaction(false, false)
			.await
			.unwrap()
			.get_kv_user(creds.username)
			.await
			.unwrap()
			.hash;
		ds.setup_initial_creds(creds).await.unwrap();
		assert_eq!(
			pass_hash,
			ds.transaction(false, false)
				.await
				.unwrap()
				.get_kv_user(creds.username)
				.await
				.unwrap()
				.hash
		)
	}
}
