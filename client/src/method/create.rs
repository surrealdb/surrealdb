use crate::method::Content;
use crate::method::Method;
use crate::param::DbResource;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::IntoFuture;
use std::marker::PhantomData;

/// A record create future
#[derive(Debug)]
pub struct Create<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<DbResource>,
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
	R: DeserializeOwned + Send + 'r,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Result<R>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(self.execute())
	}
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned + Send + 'r,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Result<R>>;

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
