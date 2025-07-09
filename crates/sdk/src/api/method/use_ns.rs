use surrealdb_protocol::proto::rpc::v1::UseRequest;
use surrealdb_protocol::proto::rpc::v1::UseResponse;

use crate::Surreal;

use crate::api::Result;
use crate::api::method::BoxFuture;
use crate::api::method::UseDb;

use std::future::IntoFuture;

/// Stores the namespace to use
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct UseNs {
	pub(super) client: Surreal,
	pub(super) ns: String,
}

impl UseNs {}

impl UseNs {
	/// Switch to a specific database
	pub fn use_db(self, db: impl Into<String>) -> UseDb {
		UseDb {
			ns: self.ns.into(),
			db: db.into(),
			client: self.client,
		}
	}
}

impl IntoFuture for UseNs {
	type Output = Result<UseResponse>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			let response = client
				.r#use(UseRequest {
					namespace: self.ns,
					database: String::new(),
				})
				.await?;
			let response = response.into_inner();

			Ok(response)
		})
	}
}
