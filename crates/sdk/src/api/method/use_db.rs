use surrealdb_protocol::proto::rpc::v1::UseRequest;

use crate::Surreal;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;

use crate::opt::WaitFor;
use std::borrow::Cow;
use std::future::IntoFuture;

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseDb {
	pub(super) client: Surreal,
	pub(super) ns: Option<String>,
	pub(super) db: String,
}

impl UseDb
{
}

impl IntoFuture for UseDb
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(mut self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			let response = client.r#use(UseRequest {
				namespace: self.ns.unwrap_or_default(),
				database: self.db,
			}).await?;

			todo!("STUB: UseDb future");
			Ok(())
		})
	}
}
