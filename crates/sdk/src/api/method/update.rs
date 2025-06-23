use crate::method::merge::Merge;
use crate::method::patch::Patch;
use crate::opt::PatchOp;
use crate::opt::PatchOps;
use crate::Surreal;

use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::method::Content;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use crate::opt::KeyRange;
use serde::Serialize;
use surrealdb_core::expr::Data;
use surrealdb_core::expr::TryFromValue;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::{Value as Value, to_value as to_core_value};

use super::ensure_values_are_objects;

/// An update future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Update<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Resource,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Update<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Update<'static, C, R> {
		Update {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Update {
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let router = client.inner.router.extract()?;
				router
					.$method(Command::Update {
						what: resource,
						data: None,
					})
					.await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Update<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Update<'r, Client, Option<R>>
where
	Client: Connection,
	R: TryFromValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Update<'r, Client, Vec<R>>
where
	Client: Connection,
	R: TryFromValue,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<C> Update<'_, C, Value>
where
	C: Connection,
{
	/// Restricts the records to update to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.with_range(range.into());
		self
	}
}

impl<C, R> Update<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records to update to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.with_range(range.into());
		self
	}
}

impl<'r, C, R> Update<'r, C, R>
where
	C: Connection,
	R: TryFromValue,
{
	/// Replaces the current document / record data with the specified data
	pub fn content(self, data: Value) -> Content<'r, C, R> {
		ensure_values_are_objects(
			&data,
		)?;

		let what = self.resource;

		let data = match data {
			Value::None => None,
			content => Some(Data::ContentExpression(content)),
		};

		let cmd = Command::Update {
			what,
			data,
		};

		Content::new(self.client, cmd)
	}

	/// Merges the current document / record data with the specified data
	pub fn merge(self, data: Value) -> Merge<'r, C, R>
	{
		Merge {
			client: self.client,
			resource: self.resource,
			content: data,
			upsert: false,
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch data
	pub fn patch(self, patch: impl Into<PatchOp>) -> Patch<'r, C, R> {
		Patch {
			patches: PatchOps(vec![patch.into()]),
			client: self.client,
			resource: self.resource,
			upsert: false,
			response_type: PhantomData,
		}
	}
}
