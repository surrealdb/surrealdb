use crate::cli::CF;
use crate::err::Error;
use clap::Args;
use once_cell::sync::OnceCell;
use std::time::Duration;
use surrealdb::kvs::Datastore;
use surrealdb::opt::auth::Root;

pub static DB: OnceCell<Datastore> = OnceCell::new();

#[derive(Args, Debug)]
pub struct StartCommandDbsOptions {
	#[arg(help = "Whether strict mode is enabled on this database instance")]
	#[arg(env = "SURREAL_STRICT", short = 's', long = "strict")]
	#[arg(default_value_t = false)]
	strict_mode: bool,
	#[arg(help = "The maximum duration that a set of statements can run for")]
	#[arg(env = "SURREAL_QUERY_TIMEOUT", long)]
	#[arg(value_parser = super::cli::validator::duration)]
	query_timeout: Option<Duration>,
	#[arg(help = "The maximum duration that any single transaction can run for")]
	#[arg(env = "SURREAL_TRANSACTION_TIMEOUT", long)]
	#[arg(value_parser = super::cli::validator::duration)]
	transaction_timeout: Option<Duration>,
	#[arg(help = "Whether to enable authentication")]
	#[arg(env = "SURREAL_AUTH", long = "auth")]
	#[arg(default_value_t = false)]
	auth_enabled: bool,
}

pub async fn init(
	StartCommandDbsOptions {
		strict_mode,
		query_timeout,
		transaction_timeout,
		auth_enabled,
	}: StartCommandDbsOptions,
) -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log specified strict mode
	debug!("Database strict mode is {strict_mode}");
	// Log specified query timeout
	if let Some(v) = query_timeout {
		debug!("Maximum query processing timeout is {v:?}");
	}
	// Log specified parse timeout
	if let Some(v) = transaction_timeout {
		debug!("Maximum transaction processing timeout is {v:?}");
	}

	if auth_enabled {
		info!("✅🔒 Authentication is enabled 🔒✅");
	} else {
		warn!("❌🔒 IMPORTANT: Authentication is disabled. This is not recommended for production use. 🔒❌");
	}
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path)
		.await?
		.with_notifications()
		.with_strict_mode(strict_mode)
		.with_query_timeout(query_timeout)
		.with_transaction_timeout(transaction_timeout)
		.with_auth_enabled(auth_enabled);
	dbs.bootstrap().await?;

	if let Some(user) = opt.user.as_ref() {
		dbs.setup_initial_creds(Root {
			username: user,
			password: opt.pass.as_ref().unwrap(),
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

		// Setup the initial user if there are no root users
		assert_eq!(
			ds.transaction(false, false).await.unwrap().all_root_users().await.unwrap().len(),
			0
		);
		ds.setup_initial_creds(creds).await.unwrap();
		assert_eq!(
			ds.transaction(false, false).await.unwrap().all_root_users().await.unwrap().len(),
			1
		);
		verify_creds(&ds, None, None, creds.username, creds.password).await.unwrap();

		// Do not setup the initial root user if there are root users:
		// Test the scenario by making sure the custom password doesn't change.
		let sql = "DEFINE USER root ON ROOT PASSWORD 'test' ROLES OWNER";
		let sess = Session::owner();
		ds.execute(sql, &sess, None).await.unwrap();
		let pass_hash = ds
			.transaction(false, false)
			.await
			.unwrap()
			.get_root_user(creds.username)
			.await
			.unwrap()
			.hash;

		ds.setup_initial_creds(creds).await.unwrap();
		assert_eq!(
			pass_hash,
			ds.transaction(false, false)
				.await
				.unwrap()
				.get_root_user(creds.username)
				.await
				.unwrap()
				.hash
		)
	}
}
