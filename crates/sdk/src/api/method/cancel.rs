use crate::api::Result;
use crate::api::Surreal;
use crate::api::method::BoxFuture;
use std::borrow::Cow;
use std::future::IntoFuture;
use surrealdb_core::sql::statements::CancelStatement;

/// A transaction cancellation future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Cancel {
	pub(crate) client: Surreal,
}

impl IntoFuture for Cancel {
	type Output = Result<Surreal>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.query(CancelStatement::default().to_string()).await?;
			Ok(self.client)
		})
	}
}
