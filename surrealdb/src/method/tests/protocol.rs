use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::OnceLock;

use tokio::sync::watch;
use url::Url;

use super::server;
use crate::conn::Router;
use crate::method::BoxFuture;
use crate::opt::endpoint::into_endpoint;
use crate::opt::{Endpoint, IntoEndpoint};
use crate::{Connect, ExtraFeatures, OnceLockExt, Result, SessionClone, Surreal, conn};

#[derive(Debug)]
pub struct Test;

impl IntoEndpoint<Test> for () {}
impl into_endpoint::Sealed<Test> for () {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		Ok(Endpoint::new(Url::parse("test://").map_err(crate::std_error_to_types_error)?))
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

impl crate::Connection for Client {}

impl conn::Sealed for Client {
	#[allow(private_interfaces)]
	fn connect(
		address: Endpoint,
		capacity: usize,
		session_clone: Option<SessionClone>,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = async_channel::bounded(capacity);
			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);
			let router = Router {
				features,
				sender: route_tx,
				config: address.config,
			};
			server::mock(route_rx);
			let session_clone = session_clone.unwrap_or_else(SessionClone::new);
			Ok((OnceLock::with_value(router), watch::channel(None), session_clone).into())
		})
	}
}
