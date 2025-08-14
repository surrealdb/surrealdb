use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::transaction::WithTransaction;
use super::{Content, validate_data};
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use crate::api::{self, Connection, Result};
use crate::core::val;
use crate::method::OnceLockExt;
use crate::{Surreal, Value};

/// A record create future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Create<'r, C: Connection, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> WithTransaction for Create<'_, C, R>
where
	C: Connection,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}
impl<C, R> Create<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
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
				txn,
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let router = client.inner.router.extract()?;
				let cmd = Command::Create {
					txn,
					what: resource?,
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
	R: DeserializeOwned,
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
	pub fn content<D>(self, data: D) -> Content<'r, C, Value>
	where
		D: Serialize + 'static,
	{
		Content::from_closure(self.client, self.txn, || {
			let content = api::value::to_core_value(data)?;

			validate_data(
				&content,
				"Tried to create non-object-like data as content, only structs and objects are supported",
			)?;

			let data = match content {
				val::Value::None | val::Value::Null => None,
				content => Some(content),
			};

			Ok(Command::Create {
				txn: self.txn,
				what: self.resource?,
				data,
			})
		})
	}
}

impl<'r, C, R> Create<'r, C, Option<R>>
where
	C: Connection,
{
	/// Sets content of a record
	pub fn content<D>(self, data: D) -> Content<'r, C, Option<R>>
	where
		D: Serialize + 'static,
	{
		Content::from_closure(self.client, self.txn, || {
			let content = api::value::to_core_value(data)?;

			validate_data(
				&content,
				"Tried to create non-object-like data as content, only structs and objects are supported",
			)?;

			let data = match content {
				val::Value::None | val::Value::Null => None,
				content => Some(content),
			};

			Ok(Command::Create {
				txn: self.txn,
				what: self.resource?,
				data,
			})
		})
	}
}
