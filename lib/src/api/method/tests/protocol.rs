use super::server;
use crate::api::conn::Connection;
use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Route;
use crate::api::conn::Router;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::Connect;
use crate::api::ExtraFeatures;
use crate::api::OnceLockExt;
use crate::api::Result;
use crate::api::Surreal;
use flume::Receiver;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::watch;
use url::Url;

#[derive(Debug)]
pub struct Test;

impl IntoEndpoint<Test> for () {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		Ok(Endpoint::new(Url::parse("test://")?))
	}
}

#[derive(Debug, Clone)]
pub struct Client {
	method: Method,
}

impl Surreal<Client> {
	pub fn connect<P>(
		&self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			router: self.router.clone(),
			engine: PhantomData,
			address: address.into_endpoint(),
			capacity: 0,
			waiter: self.waiter.clone(),
			response_type: PhantomData,
		}
	}
}

impl crate::api::Connection for Client {}

impl Connection for Client {
	fn new(method: Method) -> Self {
		Self {
			method,
		}
	}

	fn connect(_address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = flume::bounded(capacity);
			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);
			let router = Router {
				features,
				sender: route_tx,
				last_id: AtomicI64::new(0),
			};
			server::mock(route_rx);
			Ok(Surreal::new_from_router_waiter(
				Arc::new(OnceLock::with_value(router)),
				Arc::new(watch::channel(None)),
			))
		})
	}

	fn send<'r>(
		&'r mut self,
		router: &'r Router,
		param: Param,
	) -> BoxFuture<'r, Result<Receiver<Result<DbResponse>>>> {
		Box::pin(async move {
			let (sender, receiver) = flume::bounded(1);
			let route = Route {
				request: (0, self.method, param),
				response: sender,
			};
			router.sender.send_async(route).await.as_ref().map_err(ToString::to_string).unwrap();
			Ok(receiver)
		})
	}
}
