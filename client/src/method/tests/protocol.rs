use super::server;
use crate::param::from_value;
use crate::param::DbResponse;
use crate::param::Param;
use crate::param::ServerAddrs;
use crate::param::ToServerAddrs;
use crate::Connection;
use crate::Method;
use crate::Result;
use crate::Route;
use crate::Router;
use crate::Surreal;
use async_trait::async_trait;
use flume::Receiver;
use once_cell::sync::OnceCell;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
#[cfg(feature = "ws")]
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use surrealdb::sql::Value;
use url::Url;

#[derive(Debug)]
pub struct Test;

impl ToServerAddrs<Test> for () {
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		Ok(ServerAddrs {
			endpoint: Url::parse("test://localhost:8000")?,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

#[derive(Debug, Clone)]
pub struct Client {
	method: Method,
}

#[async_trait]
impl Connection for Client {
	type Request = (Method, Param);
	type Response = Result<DbResponse>;

	fn new(method: Method) -> Self {
		Self {
			method,
		}
	}

	async fn connect(_address: ServerAddrs, capacity: usize) -> Result<Surreal<Self>> {
		let (route_tx, route_rx) = flume::bounded(capacity);
		let router = Router {
			conn: PhantomData,
			sender: route_tx,
			#[cfg(feature = "ws")]
			last_id: AtomicI64::new(0),
		};
		server::mock(route_rx);
		Ok(Surreal {
			router: OnceCell::with_value(Arc::new(router)),
		})
	}

	async fn send(
		&mut self,
		router: &Router<Self>,
		param: Param,
	) -> Result<Receiver<Self::Response>> {
		let (sender, receiver) = flume::bounded(1);
		let route = Route {
			request: (self.method, param),
			response: sender,
		};
		router.sender.send_async(Some(route)).await.as_ref().map_err(ToString::to_string).unwrap();
		Ok(receiver)
	}

	async fn recv<R>(&mut self, rx: Receiver<Self::Response>) -> Result<R>
	where
		R: DeserializeOwned,
	{
		let result = rx.into_recv_async().await.unwrap();
		match result.unwrap() {
			DbResponse::Other(value) => from_value(&value),
			DbResponse::Query(..) => unreachable!(),
		}
	}

	async fn recv_query(
		&mut self,
		rx: Receiver<Self::Response>,
	) -> Result<Vec<Result<Vec<Value>>>> {
		let result = rx.into_recv_async().await.unwrap();
		match result.unwrap() {
			DbResponse::Query(results) => Ok(results),
			DbResponse::Other(..) => unreachable!(),
		}
	}
}
