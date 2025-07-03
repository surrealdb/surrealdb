use surrealdb_protocol::proto::rpc::v1::UseRequest;

use crate::Surreal;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::method::UseDb;

use std::borrow::Cow;
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
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(mut self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			let response = client
				.r#use(UseRequest {
					namespace: self.ns,
					database: String::new(),
				})
				.await?;

			Ok(())
		})
	}
}
