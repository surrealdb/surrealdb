//! Datastore module which is the core of the database node.
//! In this module we essentially manage the entire lifecycle of a database request acting as the
//! glue between the API and the response. In this module we use channels as a transport layer
//! and executors to process the operations. This module also gives a `context` to the transaction.

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::kvs::Datastore;
use anyhow::Result;
use std::sync::Arc;

mod distinct;
mod executor;
mod group;
mod interval;
mod iterator;
mod notification;
mod options;
mod plan;
mod processor;
mod response;
mod result;
mod session;
mod statement;
mod store;
mod tasks;
mod variables;

pub mod capabilities;
pub mod node;

pub use self::capabilities::Capabilities;
pub(crate) use self::executor::*;
pub use self::interval::*;
pub(crate) use self::iterator::*;
pub use self::notification::*;
pub use self::options::*;
pub use self::response::*;
pub use self::result::*;
pub use self::session::*;
pub(crate) use self::statement::*;
pub use self::variables::*;

#[cfg(storage)]
mod file;

#[cfg(test)]
pub(crate) mod test;

const TARGET: &str = "surrealdb_core::dbs";

#[derive(Debug, Clone)]
pub struct SurrealDBArgs {
	pub uri: String,
	pub strict_mode: bool,
	pub query_timeout: Option<Duration>,
	pub transaction_timeout: Option<Duration>,
	pub unauthenticated: bool,
	pub capabilities: Capabilities,
	pub temporary_directory: Option<PathBuf>,
	pub import_file: Option<PathBuf>,
	pub root_user: Option<String>,
	pub root_pass: Option<String>,
	pub node_membership_refresh_interval: Duration,
	pub node_membership_check_interval: Duration,
	pub node_membership_cleanup_interval: Duration,
	pub changefeed_gc_interval: Duration,
}

/// This is a wrapper around the Datastore. It is in charge of spawning the datastore and managing the lifecycle of the database.
pub struct SurrealDB {
	datastore: Arc<Datastore>,
	cancellation_token: CancellationToken,
	background_tasks: JoinSet<()>,
}

impl SurrealDB {
	pub async fn start(
		SurrealDBArgs {
			uri,
			strict_mode,
			query_timeout,
			transaction_timeout,
			unauthenticated,
			capabilities,
			temporary_directory,
			import_file,
			root_user,
			root_pass,
			node_membership_refresh_interval,
			node_membership_check_interval,
			node_membership_cleanup_interval,
			changefeed_gc_interval,
		}: SurrealDBArgs,
	) -> Result<Self> {
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
		// Log whether authentication is disabled
		if unauthenticated {
			warn!(
				"❌🔒 IMPORTANT: Authentication is disabled. This is not recommended for production use. 🔒❌"
			);
		}
		// Warn about the impact of denying all capabilities
		// if args.capabilities.get_deny_all() {
		//     warn!(
		//         "You are denying all capabilities by default. Although this is recommended, beware that any new capabilities will also be denied."
		//     );
		// }
		// Log the specified server capabilities
		debug!("Server capabilities: {capabilities}");
		// Parse and setup the desired kv datastore
		let dbs = Datastore::new(&uri)
			.await?
			.with_strict_mode(strict_mode)
			.with_query_timeout(query_timeout)
			.with_transaction_timeout(transaction_timeout)
			.with_auth_enabled(!unauthenticated)
			.with_temporary_directory(temporary_directory)
			.with_capabilities(capabilities);

		let ds = Arc::new(dbs);

		// Ensure the storage version is up-to-date to prevent corruption
		ds.check_version().await?;

		// Import file at start, if provided
		Self::import_file(Arc::clone(&ds), import_file.as_deref()).await?;

		// Setup initial server auth credentials
		Self::setup_auth(Arc::clone(&ds), root_user.as_deref(), root_pass.as_deref()).await?;

		// Bootstrap the datastore
		ds.bootstrap().await?;

		let cancellation_token = CancellationToken::new();

		let mut background_tasks = JoinSet::new();
		background_tasks.spawn(tasks::node_membership_refresh_task(
			Arc::clone(&ds),
			cancellation_token.clone(),
			node_membership_refresh_interval,
		));
		background_tasks.spawn(tasks::node_membership_check_task(
			Arc::clone(&ds),
			cancellation_token.clone(),
			node_membership_check_interval,
		));
		background_tasks.spawn(tasks::node_membership_cleanup_task(
			Arc::clone(&ds),
			cancellation_token.clone(),
			node_membership_cleanup_interval,
		));
		background_tasks.spawn(tasks::changefeed_cleanup_task(
			Arc::clone(&ds),
			cancellation_token.clone(),
			changefeed_gc_interval,
		));

		Ok(Self {
			datastore: ds,
			cancellation_token,
			background_tasks,
		})
	}

	async fn import_file(datastore: Arc<Datastore>, file: Option<&Path>) -> Result<()> {
		if let Some(file) = file {
			#[cfg(not(target_family = "wasm"))]
			{
				let sql = tokio::fs::read_to_string(file).await?;
				datastore.startup(&sql, &Session::owner()).await?;
			}

			#[cfg(target_family = "wasm")]
			{
				return Err(anyhow::anyhow!("Importing files is not supported in WASM"));
			}
		}
		Ok(())
	}

	async fn setup_auth(
		datastore: Arc<Datastore>,
		user: Option<&str>,
		pass: Option<&str>,
	) -> Result<()> {
		if let (Some(user), Some(pass)) = (user, pass) {
			datastore.initialise_credentials(user, pass).await?;
		}
		Ok(())
	}
}

impl IntoFuture for SurrealDB {
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

	fn into_future(mut self) -> Self::IntoFuture {
		Box::pin(async move {
			while let Some(result) = self.background_tasks.join_next().await {
				if let Err(err) = result {
					error!("Background task failed: {:?}", err);
				}
			}
			Ok(())
		})
	}
}
