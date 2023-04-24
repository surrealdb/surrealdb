use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::Connection;
use crate::api::Result;
use crate::sql::Uuid;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

/// A live query kill future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Kill<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) query_id: Uuid,
}

impl<'r, Client> IntoFuture for Kill<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Kill);
			conn.execute(self.router?, Param::new(vec![self.query_id.into()])).await
		})
	}
}
