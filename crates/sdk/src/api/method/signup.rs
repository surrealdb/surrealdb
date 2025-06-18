use crate::Surreal;
use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::error::Api;
use crate::expr::to_value;
use crate::method::OnceLockExt;
use crate::opt::auth::Jwt;
use anyhow::Context;
use serde::de::DeserializeOwned;
use serde_content::Value as Content;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::protocol::surrealdb::rpc::QueryResult;
use surrealdb_core::protocol::surrealdb::rpc::Response as ResponseProto;
use surrealdb_core::protocol::surrealdb::rpc::SignupParams;
use surrealdb_core::protocol::surrealdb::value::Value as ValueProto;

/// A signup future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Signup<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) params: SignupParams,
}

impl<C> Signup<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Signup<'static, C> {
		Signup {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client> IntoFuture for Signup<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Jwt>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Signup {
			client,
			params,
		} = self;

		Box::pin(async move {
			let router = client.inner.router.extract()?;
			let response = router.execute(Command::Signup(params)).await?;

			let results = response.into_results();
			let value = results.next().context("No results returned from signup")??;

			Jwt::try_from(value)
		})
	}
}
