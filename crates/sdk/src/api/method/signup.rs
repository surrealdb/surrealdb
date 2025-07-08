use crate::Surreal;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;

use crate::opt::auth::Jwt;
use anyhow::Context;
use std::borrow::Cow;
use std::future::IntoFuture;
use surrealdb_protocol::proto::rpc::v1::SignupRequest;

/// A signup future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Signup {
	pub(super) client: Surreal,
	pub(super) request: SignupRequest,
}

impl Signup {}

impl IntoFuture for Signup {
	type Output = Result<Jwt>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Signup {
			client,
			request,
		} = self;

		Box::pin(async move {
			let mut client = client.client.clone();
			let client = &mut client;

			let response = client.signup(request).await?;

			let response = response.into_inner();

			let value = response.value.ok_or(anyhow::anyhow!("No value found in response"))?;

			let jwt = Jwt::try_from(value)?;

			Ok(jwt)
		})
	}
}
