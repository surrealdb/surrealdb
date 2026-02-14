use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use crate::conn::Command;
use crate::Error;
use crate::method::{BoxFuture, OnceLockExt};
use crate::opt::auth::{RefreshToken, Token};
use crate::types::SurrealValue;
use crate::{Connection, Result, Surreal};

/// An authentication future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Authenticate<'r, C: Connection, T = ()> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) token: Token,
	pub(super) token_type: PhantomData<T>,
}

impl<'r, Client> IntoFuture for Authenticate<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Token>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			let value = router
				.execute_value(
					self.client.session_id,
					Command::Authenticate {
						token: SurrealValue::from_value(self.token.into_value())?,
					},
				)
				.await?;
			Ok(Token::from_value(value)?)
		})
	}
}

impl<'r, Client, T> Authenticate<'r, Client, T>
where
	Client: Connection,
{
	pub fn into_owned(self) -> Authenticate<'r, Client, T> {
		Authenticate {
			client: self.client,
			token: self.token,
			token_type: PhantomData,
		}
	}
}

impl<'r, Client> Authenticate<'r, Client>
where
	Client: Connection,
{
	/// Converts this authentication future into a token refresh operation.
	///
	/// This method changes the authentication mode from standard authentication
	/// to token refresh. When awaited, the future will use the refresh token
	/// to obtain a new access token instead of authenticating with the access token.
	///
	/// # Returns
	///
	/// An `Authenticate` future configured for token refresh.
	///
	/// # Examples
	///
	/// ```ignore
	/// // Get a token from signin
	/// let token = db.signin(credentials).await?;
	///
	/// // Later, refresh the token
	/// let new_token = db.authenticate(token).refresh().await?;
	/// ```
	pub fn refresh(self) -> Authenticate<'r, Client, RefreshToken> {
		Authenticate {
			client: self.client,
			token: self.token,
			token_type: PhantomData,
		}
	}
}

impl<'r, Client> IntoFuture for Authenticate<'r, Client, RefreshToken>
where
	Client: Connection,
{
	type Output = Result<Token>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			// Validate that the token has a refresh component.
			// If not, return an error since we can't perform a refresh.
			let token = match self.token.refresh.is_some() {
				true => self.token.into_value(),
				false => {
					return Err(Error::internal("Missing refresh token".to_string()));
				}
			};
			// Execute the refresh command to obtain new tokens.
			let value = router
				.execute_value(
					self.client.session_id,
					Command::Refresh {
						token: SurrealValue::from_value(token)?,
					},
				)
				.await?;
			Ok(Token::from_value(value)?)
		})
	}
}
