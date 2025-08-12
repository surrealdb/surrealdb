use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::transaction::WithTransaction;
use super::validate_data;
use crate::api::conn::Command;
use crate::api::method::{BoxFuture, Content, Merge, Patch};
use crate::api::opt::{PatchOp, Resource};
use crate::api::{self, Connection, Result};
use crate::core::val;
use crate::method::OnceLockExt;
use crate::opt::KeyRange;
use crate::{Surreal, Value};

/// An upsert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Upsert<'r, C: Connection, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> WithTransaction for Upsert<'_, C, R>
where
	C: Connection,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<C, R> Upsert<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Upsert<'static, C, R> {
		Upsert {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Upsert {
				txn,
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let router = client.inner.router.extract()?;
				router
					.$method(Command::Upsert {
						txn,
						what: resource?,
						data: None,
					})
					.await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Upsert<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Upsert<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Upsert<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<C> Upsert<'_, C, Value>
where
	C: Connection,
{
	/// Restricts the records to upsert to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<C, R> Upsert<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records to upsert to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<'r, C, R> Upsert<'r, C, R>
where
	C: Connection,
	R: DeserializeOwned,
{
	/// Replaces the current document / record data with the specified data
	pub fn content<D>(self, data: D) -> Content<'r, C, R>
	where
		D: Serialize + 'static,
	{
		Content::from_closure(self.client, self.txn, || {
			let data = api::value::to_core_value(data)?;

			validate_data(
				&data,
				"Tried to upsert non-object-like data as content, only structs and objects are supported",
			)?;

			let data = match data {
				val::Value::None => None,
				content => Some(content),
			};

			Ok(Command::Upsert {
				txn: self.txn,
				what: self.resource?,
				data,
			})
		})
	}

	/// Merges the current document / record data with the specified data
	pub fn merge<D>(self, data: D) -> Merge<'r, C, D, R>
	where
		D: Serialize,
	{
		Merge {
			txn: self.txn,
			client: self.client,
			resource: self.resource,
			content: data,
			upsert: true,
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch
	/// data
	pub fn patch(self, patch: impl Into<PatchOp>) -> Patch<'r, C, R> {
		let PatchOp(result) = patch.into();
		let patches = match result {
			Ok(serde_content::Value::Seq(values)) => values.into_iter().map(Ok).collect(),
			Ok(value) => vec![Ok(value)],
			Err(error) => vec![Err(error)],
		};
		Patch {
			patches,
			txn: self.txn,
			client: self.client,
			resource: self.resource,
			upsert: true,
			response_type: PhantomData,
		}
	}
}
