use surrealdb_core::iam::{AccessMethod, SigninParams};
use surrealdb_protocol::proto::rpc::v1::SigninRequest;

use crate::Surreal;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;

use crate::opt::auth::Jwt;
use std::borrow::Cow;
use std::future::IntoFuture;

/// A signin future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Signin {
	pub(super) client: Surreal,
	pub(super) access_method: AccessMethod,
}

impl IntoFuture for Signin {
	type Output = Result<Jwt>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Signin {
			client,
			access_method,
		} = self;
		Box::pin(async move {
			let mut client = client.client.clone();
			let client = &mut client;

			let response = client
				.signin(SigninRequest {
					access_method: Some(access_method.try_into()?),
					..Default::default()
				})
				.await?;

			let response = response.into_inner();

			let value = response.value.ok_or(anyhow::anyhow!("No value found in response"))?;

			let jwt = Jwt::try_from(value)?;

			Ok(jwt)
		})
	}
}
