use crate::api::method::Method;
use crate::api::opt::Param;
use crate::api::Connection;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::Router;
use crate::sql::Value;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

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
	R: DeserializeOwned + Send + Sync,
{
	type Output = Result<R>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.router?;
			if !router.features.contains(&ExtraFeatures::Auth) {
				return Err(Error::AuthNotSupported.into());
			}
			let mut conn = Client::new(Method::Signin);
			conn.execute(router, Param::new(vec![self.credentials?])).await
		})
	}
}
