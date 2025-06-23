use crate::api::Connection;
use crate::api::Result;
use crate::api::Surreal;
use crate::api::method::BoxFuture;
use std::borrow::Cow;
use std::future::IntoFuture;
use surrealdb_core::sql::statements::CancelStatement;

/// A transaction cancellation future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Cancel<'req, C: Connection> {
	pub(crate) client: Cow<'req, Surreal<C>>,
}

impl<'req, C> IntoFuture for Cancel<'req, C>
where
	C: Connection,
{
	type Output = Result<Surreal<C>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(CancelStatement::default().to_string()).await?;
			Ok(self.client.into_owned())
		})
	}
}
