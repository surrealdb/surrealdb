use std::collections::HashSet;
use std::sync::Arc;

use async_channel::Receiver;
use surrealdb_engine_local::Datastore;
use surrealdb_types::Notification;
use tokio::sync::watch;

use crate::conn::{self, Router};
use crate::engine::local::{Db, local_config};
use crate::method::BoxFuture;
use crate::opt::{Endpoint, WaitFor};
use crate::{ExtraFeatures, Result, SessionClone, Surreal};

impl crate::Connection for Db {}
impl conn::Sealed for Db {
	#[allow(private_interfaces)]
	fn connect(
		address: Endpoint,
		_capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let config = address.config.clone();
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);

			let engine = surrealdb_engine_local::connect(
				local_config(address),
				session_clone.receiver.clone(),
			)
			.await?;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router::from_engine(engine, features, config);

			Ok((router, waiter, session_clone).into())
		})
	}
}

impl Surreal<Db> {
	// This function was introduced by a community PR,
	//
	// It exposes internal types in the public API so it is marked as doc(hidden).
	// This function is not stable nor subject to semver stability guarentees.
	#[doc(hidden)]
	/// The datastore carries its own maintenance tasks and cancellation, both
	/// established when it was built, so neither is passed here any more. Set
	/// them on the `Builder` used to construct the datastore instead.
	pub async fn unstable_from_datastore(
		datastore: Arc<Datastore>,
		notifications: Option<Receiver<Notification>>,
	) -> Result<Self> {
		let session_clone = SessionClone::new();
		let engine = surrealdb_engine_local::from_datastore(
			datastore,
			notifications,
			session_clone.receiver.clone(),
		);

		let mut features = HashSet::new();
		features.insert(ExtraFeatures::Backup);
		features.insert(ExtraFeatures::LiveQueries);

		let waiter = watch::channel(Some(WaitFor::Connection));
		let router = Router::from_engine(engine, features, crate::opt::Config::default());

		Ok((router, waiter, session_clone).into())
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use std::sync::Arc;

	use surrealdb_engine_local::Datastore;

	use crate::Surreal;
	use crate::engine::local::Db;

	/// Bringing your own datastore reaches the same engine `connect` builds,
	/// but not by the same route, so it is the one construction path the
	/// integration suite has no way to drive.
	#[test_log::test(tokio::test)]
	async fn a_supplied_datastore_serves_queries() {
		let datastore = Datastore::new("memory").await.unwrap();
		let db =
			Surreal::<Db>::unstable_from_datastore(Arc::clone(&datastore), None).await.unwrap();

		db.use_ns("test").use_db("test").await.unwrap();
		db.query("CREATE person:one SET name = 'a'").await.unwrap().check().unwrap();

		let names: Vec<String> =
			db.query("SELECT VALUE name FROM person").await.unwrap().take(0).unwrap();
		assert_eq!(names, vec!["a".to_string()]);
	}
}
