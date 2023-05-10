use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::sql::to_value;
use crate::sql::Id;
use crate::sql::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

/// A merge future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Merge<'r, C: Connection, D, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) content: D,
	pub(super) response_type: PhantomData<R>,
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Merge {
				router,
				resource,
				range,
				content,
				..
			} = self;
			let content = to_value(content);
			Box::pin(async move {
				let param = match range {
					Some(range) => resource?.with_range(range)?,
					None => resource?.into(),
				};
				let mut conn = Client::new(Method::Merge);
				conn.$method(router?, Param::new(vec![param, content?])).await
			})
		}
	};
}

impl<'r, Client, D> IntoFuture for Merge<'r, Client, D, Value>
where
	Client: Connection,
	D: Serialize,
{
	type Output = Result<Value>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute_value}
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, Option<R>>
where
	Client: Connection,
	D: Serialize,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute_opt}
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, Vec<R>>
where
	Client: Connection,
	D: Serialize,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute_vec}
}
