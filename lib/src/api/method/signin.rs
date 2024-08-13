use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::to_value;
use crate::Surreal;
use serde::de::DeserializeOwned;
use serde_content::Value as Content;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

/// A signin future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Signin<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) credentials: serde_content::Result<Content<'static>>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Signin<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Signin<'static, C, R> {
		Signin {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client, R> IntoFuture for Signin<'r, Client, R>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Signin {
			client,
			credentials,
			..
		} = self;
		Box::pin(async move {
			let router = client.router.extract()?;
			let content = credentials.map_err(crate::error::Db::from)?;
			router
				.execute(Command::Signin {
					credentials: to_value(content)?.try_into()?,
				})
				.await
		})
	}
}
