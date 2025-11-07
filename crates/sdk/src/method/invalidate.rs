use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::opt::auth::Token;
use crate::types::{SurrealValue, Value};
use crate::{Connection, Result, Surreal};

/// A session invalidate future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Invalidate<'r, C: Connection, T = ()> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) token: Value,
	pub(super) typ: PhantomData<T>,
}

impl<C, T> Invalidate<'_, C, T>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Invalidate<'static, C, T> {
		Invalidate {
			client: Cow::Owned(self.client.into_owned()),
			token: self.token,
			typ: PhantomData,
		}
	}
}

impl<'r, Client> IntoFuture for Invalidate<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			router.execute_unit(Command::Invalidate, self.client.session_id).await
		})
	}
}

impl<'r, Client> Invalidate<'r, Client>
where
	Client: Connection,
{
	/// Converts this invalidation future into a token revocation operation.
	///
	/// This method changes the invalidation mode from session invalidation to
	/// explicit refresh token revocation. This is useful when you want to revoke
	/// a specific refresh token without invalidating the entire session.
	///
	/// # Arguments
	///
	/// * `token` - The token containing the refresh token to revoke
	///
	/// # Returns
	///
	/// An `Invalidate` future configured for token revocation.
	///
	/// # Examples
	///
	/// ```ignore
	/// // Get a token from signin
	/// let token = db.signin(credentials).await?;
	///
	/// // Later, explicitly revoke the refresh token
	/// db.invalidate().refresh(token).await?;
	/// ```
	pub fn refresh(self, token: Token) -> Invalidate<'r, Client, Token> {
		Invalidate {
			client: self.client,
			token: token.into_value(),
			typ: PhantomData,
		}
	}
}

impl<'r, Client> IntoFuture for Invalidate<'r, Client, Token>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			router
				.execute_unit(
					Command::Revoke {
						token: SurrealValue::from_value(self.token)?,
					},
					self.client.session_id,
				)
				.await
		})
	}
}
