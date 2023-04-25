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
use crate::api::Result;
use crate::api::Surreal;
use flume::Receiver;
use once_cell::sync::OnceCell;
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct Test;

impl IntoEndpoint<Test> for () {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		Ok(Endpoint {
			endpoint: Url::parse("test://")?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

#[derive(Debug, Clone)]
pub struct Client {
	method: Method,
}

impl Surreal<Client> {
	pub fn connect<P>(
		&'static self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			router: Some(&self.router),
			address: address.into_endpoint(),
			capacity: 0,
			client: PhantomData,
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

	fn connect(
		_address: Endpoint,
		capacity: usize,
	) -> Pin<Box<dyn Future<Output = Result<Surreal<Self>>> + Send + Sync + 'static>> {
		Box::pin(async move {
			let (route_tx, route_rx) = flume::bounded(capacity);
			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Auth);
			features.insert(ExtraFeatures::Backup);
			let router = Router {
				features,
				conn: PhantomData,
				sender: route_tx,
				last_id: AtomicI64::new(0),
			};
			server::mock(route_rx);
			Ok(Surreal {
				router: OnceCell::with_value(Arc::new(router)),
			})
		})
	}

	fn send<'r>(
		&'r mut self,
		router: &'r Router<Self>,
		param: Param,
	) -> Pin<Box<dyn Future<Output = Result<Receiver<Result<DbResponse>>>> + Send + Sync + 'r>> {
		Box::pin(async move {
			let (sender, receiver) = flume::bounded(1);
			let route = Route {
				request: (0, self.method, param),
				response: sender,
			};
			router
				.sender
				.send_async(Some(route))
				.await
				.as_ref()
				.map_err(ToString::to_string)
				.unwrap();
			Ok(receiver)
		})
	}
}
