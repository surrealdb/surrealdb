use crate::method::Method;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use std::future::IntoFuture;
use surrealdb::sql::Uuid;

/// A live query future
#[derive(Debug)]
pub struct Live<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) table_name: String,
}

impl<'r, Client> IntoFuture for Live<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Uuid>;
	type IntoFuture = BoxFuture<'r, Result<Uuid>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Live);
			conn.execute(self.router?, Param::new(vec![self.table_name.into()])).await
		})
	}
}
