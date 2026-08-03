use std::collections::HashSet;
use std::sync::Arc;

use async_channel::Receiver;
use surrealdb_engine_local::{Datastore, EngineOptions};
use surrealdb_types::Notification;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

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
		capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded(1);
			let config = address.config.clone();
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);

			tokio::spawn(surrealdb_engine_local::native::run_router(
				local_config(address),
				conn_tx,
				route_rx,
				session_clone.receiver.clone(),
			));

			conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router::from_route_sender(route_tx, features, config);

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
	pub async fn unstable_from_datastore(
		canceller: CancellationToken,
		datastore: Arc<Datastore>,
		notifications: Option<Receiver<Notification>>,
		engine: EngineOptions,
	) -> Result<Self> {
		let (route_tx, route_rx) = async_channel::unbounded();
		let (conn_tx, conn_rx) = async_channel::bounded::<Result<()>>(1);
		let session_clone = SessionClone::new();
		let recv = session_clone.receiver.clone();

		tokio::spawn(surrealdb_engine_local::native::run_datastore_router(
			canceller,
			datastore,
			notifications,
			engine,
			conn_tx,
			route_rx,
			recv,
		));

		conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;

		let mut features = HashSet::new();
		features.insert(ExtraFeatures::Backup);
		features.insert(ExtraFeatures::LiveQueries);

		let waiter = watch::channel(Some(WaitFor::Connection));
		let router = Router::from_route_sender(route_tx, features, crate::opt::Config::default());

		Ok((router, waiter, session_clone).into())
	}
}
