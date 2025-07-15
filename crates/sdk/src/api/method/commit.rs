use crate::api::Result;
use crate::api::Surreal;
use crate::api::method::BoxFuture;
use std::future::IntoFuture;
use surrealdb_core::sql::statements::CommitStatement;

/// A transaction commit future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Commit {
	pub(crate) client: Surreal,
}

impl IntoFuture for Commit {
	type Output = Result<Surreal>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let stmt = CommitStatement::default().to_string();
			self.client.query(stmt).await?;
			Ok(self.client)
		})
	}
}
