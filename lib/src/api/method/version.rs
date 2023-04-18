use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::err::Error;
use crate::api::Connection;
use crate::api::Result;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

/// A version future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Version<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
}

impl<'r, Client> IntoFuture for Version<'r, Client>
where
	Client: Connection,
{
	type Output = Result<semver::Version>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async {
			let mut conn = Client::new(Method::Version);
			let version: String = conn.execute(self.router?, Param::new(Vec::new())).await?;
			let semantic = version.trim_start_matches("surrealdb-");
			semantic.parse().map_err(|_| Error::InvalidSemanticVersion(semantic.to_string()).into())
		})
	}
}
