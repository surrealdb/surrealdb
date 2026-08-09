use std::collections::HashSet;

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
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router::from_engine(engine, features, config);

			Ok((router, waiter, session_clone).into())
		})
	}
}
