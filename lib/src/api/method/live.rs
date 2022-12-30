use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::Connection;
use crate::api::Result;
use crate::sql::Table;
use crate::sql::Uuid;
use crate::sql::Value;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

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
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Live);
			conn.execute(self.router?, Param::new(vec![Value::Table(Table(self.table_name))])).await
		})
	}
}
