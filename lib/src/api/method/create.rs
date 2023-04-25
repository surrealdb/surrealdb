use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::method::Content;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

/// A record create future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Create<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
}

macro_rules! into_future {
	() => {
		fn into_future(self) -> Self::IntoFuture {
			let Create {
				router,
				resource,
				..
			} = self;
			Box::pin(async {
				let mut conn = Client::new(Method::Create);
				conn.execute(router?, Param::new(vec![resource?.into()])).await
			})
		}
	};
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<R>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {}
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<R>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {}
}

macro_rules! content {
	($this:ident, $data:ident) => {
		Content {
			router: $this.router,
			method: Method::Create,
			resource: $this.resource,
			range: None,
			content: $data,
			response_type: PhantomData,
		}
	};
}

impl<'r, C, R> Create<'r, C, Option<R>>
where
	C: Connection,
{
	/// Sets content of a record
	pub fn content<D>(self, data: D) -> Content<'r, C, D, R>
	where
		D: Serialize,
	{
		content!(self, data)
	}
}

impl<'r, C, R> Create<'r, C, Vec<R>>
where
	C: Connection,
{
	/// Sets content of a record
	pub fn content<D>(self, data: D) -> Content<'r, C, D, R>
	where
		D: Serialize,
	{
		content!(self, data)
	}
}
