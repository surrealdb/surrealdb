use std::collections::HashSet;

use tokio::sync::watch;
use wasm_bindgen_futures::spawn_local;

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

			spawn_local(surrealdb_engine_local::wasm::run_router(
				local_config(address),
				conn_tx,
				route_rx,
				session_clone.receiver.clone(),
			));

			conn_rx.recv().await.map_err(crate::std_error_to_types_error)??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router::from_route_sender(route_tx, features, config);

			Ok((router, waiter, session_clone).into())
		})
	}
}
