use anyhow::Context;
use surrealdb_protocol::proto::rpc::v1::VersionRequest;

use crate::Surreal;

use crate::api::Result;
use crate::api::method::BoxFuture;
use surrealdb_core::expr::{TryFromValue, Value};

use std::future::IntoFuture;

/// A version future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Version {
	pub(super) client: Surreal,
}

impl Version {}

impl IntoFuture for Version {
	type Output = Result<semver::Version>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			let response = client.version(VersionRequest {}).await?;
			let response = response.into_inner();

			let version = semver::Version::parse(&response.version)?;

			Ok(version)
		})
	}
}
