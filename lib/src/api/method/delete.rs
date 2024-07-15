use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::Id;
use crate::sql::Value;
use crate::Surreal;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

/// A record delete future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Delete<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Delete<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Delete<'static, C, R> {
		Delete {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Delete {
				client,
				resource,
				range,
				..
			} = self;
			Box::pin(async move {
				let param = match range {
					Some(range) => resource?.with_range(range)?.into(),
					None => resource?.into(),
				};
				let mut conn = Client::new(Method::Delete);
				let cmd = Command::Delete{
					what: resource?
				}
				conn.$method(client.router.extract()?, Param::new(vec![param])).await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Delete<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Delete<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Delete<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute_vec}
}

impl<C> Delete<'_, C, Value>
where
	C: Connection,
{
	/// Restricts a range of records to delete
	pub fn range(mut self, bounds: impl Into<Range<Id>>) -> Self {
		self.range = Some(bounds.into());
		self
	}
}

impl<C, R> Delete<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts a range of records to delete
	pub fn range(mut self, bounds: impl Into<Range<Id>>) -> Self {
		self.range = Some(bounds.into());
		self
	}
}
