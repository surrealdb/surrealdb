use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::OnceLock;
use std::sync::atomic::AtomicI64;

use tokio::sync::watch;
use url::Url;

use super::server;
use crate::api::conn::Router;
use crate::api::method::BoxFuture;
use crate::api::opt::endpoint::into_endpoint;
use crate::api::opt::{Endpoint, IntoEndpoint};
use crate::api::{Connect, ExtraFeatures, OnceLockExt, Result, Surreal, conn};

#[derive(Debug)]
pub struct Test;

impl IntoEndpoint<Test> for () {}
impl into_endpoint::Sealed<Test> for () {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		Ok(Endpoint::new(Url::parse("test://")?))
	}
}

#[derive(Debug, Clone)]
pub struct Client(());

impl Surreal<Client> {
	pub fn connect<P>(
		&self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			surreal: self.inner.clone().into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}

impl crate::api::Connection for Client {}

impl conn::Sealed for Client {
	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = async_channel::bounded(capacity);
			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);
			let router = Router {
				features,
				sender: route_tx,
				config: address.config,
				last_id: AtomicI64::new(0),
			};
			server::mock(route_rx);
			Ok((OnceLock::with_value(router), watch::channel(None)).into())
		})
	}
}
