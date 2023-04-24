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

impl<'r, Client, R> Create<'r, Client, R>
where
	Client: Connection,
{
	async fn execute<T>(self) -> Result<T>
	where
		T: DeserializeOwned,
	{
		let mut conn = Client::new(Method::Create);
		conn.execute(self.router?, Param::new(vec![self.resource?.into()])).await
	}
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned + Send + Sync + 'r,
{
	type Output = Result<R>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(self.execute())
	}
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned + Send + Sync + 'r,
{
	type Output = Result<R>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(self.execute())
	}
}

macro_rules! create_methods {
	($this:ty) => {
		impl<'r, C, R> Create<'r, C, $this>
		where
			C: Connection,
		{
			/// Sets content of a record
			pub fn content<D>(self, data: D) -> Content<'r, C, D, R>
			where
				D: Serialize,
			{
				Content {
					router: self.router,
					method: Method::Create,
					resource: self.resource,
					range: None,
					content: data,
					response_type: PhantomData,
				}
			}
		}
	};
}

create_methods!(Option<R>);
create_methods!(Vec<R>);
