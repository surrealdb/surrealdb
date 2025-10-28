use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use crate::conn::Command;
use crate::err::Error;
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
				.execute_value(Command::Authenticate {
					token: SurrealValue::from_value(self.token.into_value())?,
				})
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
			let token = match self.token.refresh.is_some() {
				true => self.token.into_value(),
				false => {
					return Err(Error::MissingRefreshToken);
				}
			};
			let value = router
				.execute_value(Command::Refresh {
					token: SurrealValue::from_value(token)?,
				})
				.await?;
			Ok(Token::from_value(value)?)
		})
	}
}
