use crate::method::Method;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use std::future::IntoFuture;
use std::path::PathBuf;

/// A database export future
#[derive(Debug)]
pub struct Export<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) file: PathBuf,
}

impl<'r, Client> IntoFuture for Export<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Result<()>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async {
			let mut conn = Client::new(Method::Export);
			conn.execute(self.router?, Param::file(self.file)).await
		})
	}
}
