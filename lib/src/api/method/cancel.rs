use crate::api::method::BoxFuture;
use crate::api::Connection;
use crate::api::Result;
use crate::api::Surreal;
use crate::sql::statements::CancelStatement;
use std::future::IntoFuture;

/// A transaction cancellation future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Cancel<C: Connection> {
	pub(crate) client: Surreal<C>,
}

impl<C> IntoFuture for Cancel<C>
where
	C: Connection,
{
	type Output = Result<Surreal<C>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(CancelStatement::default()).await?;
			Ok(self.client)
		})
	}
}
