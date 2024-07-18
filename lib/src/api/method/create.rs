use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::method::Content;
use crate::api::opt::Resource;
use crate::api::method::BoxFuture;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::Value;
use crate::Surreal;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

/// A record create future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Create<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Create<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Create<'static, C, R> {
		Create {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Create {
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let router = client.router.extract()?;
				router.$method(Method::Create, Param::new(vec![resource?.into()])).await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Create<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<'r, C, R> Create<'r, C, R>
where
	C: Connection,
{
	/// Sets content of a record
	pub fn content<D>(self, data: D) -> Content<'r, C, D, R>
	where
		D: Serialize,
	{
		Content {
			client: self.client,
			method: Method::Create,
			resource: self.resource,
			range: None,
			content: data,
			response_type: PhantomData,
		}
	}
}
