use crate::Surreal;

use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{Value as Value, to_value as to_core_value};

use super::Content;
use super::ensure_values_are_objects;

/// A record create future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Create<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Resource,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Create<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Create<'static, C, R> {
		Create {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Create {
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let router = client.inner.router.extract()?;
				let cmd = Command::Create {
					what: resource,
					data: None,
				};
				router.$method(cmd).await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Create<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, Option<R>>
where
	Client: Connection,
	R: TryFromValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, C> Create<'r, C, Value>
where
	C: Connection,
{
	/// Sets content of a record
	pub fn content(self, data: impl Into<Value>) -> anyhow::Result<Content<'r, C, Value>> {
		let content = data.into();

		ensure_values_are_objects(
			&content,
		)?;

		let data = match content {
			Value::None | Value::Null => None,
			content => Some(content),
		};

		let command = Command::Create {
			what: self.resource,
			data,
		};

		Ok(Content::new(self.client, command))
	}
}

impl<'r, C, R> Create<'r, C, Option<R>>
where
	C: Connection,
{
	/// Sets content of a record
	pub fn content(self, data: impl Into<Value>) -> anyhow::Result<Content<'r, C, Option<R>>>
	{
		let content = data.into();

		ensure_values_are_objects(
			&content,
		)?;

		let data = match content {
			Value::None | Value::Null => None,
			content => Some(content),
		};

		let command = Command::Create {
			what: self.resource,
			data,
		};

		Ok(Content::new(self.client, command))
	}
}
