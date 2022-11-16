use crate::method::Method;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use std::future::IntoFuture;
use surrealdb::sql::Uuid;

/// A live query kill future
#[derive(Debug)]
pub struct Kill<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) query_id: Uuid,
}

impl<'r, Client> IntoFuture for Kill<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Result<()>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Kill);
			conn.execute(self.router?, Param::new(vec![self.query_id.into()])).await
		})
	}
}
