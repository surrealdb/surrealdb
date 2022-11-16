use crate::method::Method;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb::sql::Value;

/// A signin future
#[derive(Debug)]
pub struct Signin<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) credentials: Result<Value>,
	pub(super) response_type: PhantomData<R>,
}

impl<'r, Client, R> IntoFuture for Signin<'r, Client, R>
where
	Client: Connection,
	R: DeserializeOwned + Send,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Result<R>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Signin);
			conn.execute(self.router?, Param::new(vec![self.credentials?])).await
		})
	}
}
