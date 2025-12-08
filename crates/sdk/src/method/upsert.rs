use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use uuid::Uuid;

use super::transaction::WithTransaction;
use super::validate_data;
use crate::conn::Command;
use crate::method::{BoxFuture, Content, Merge, OnceLockExt, Patch};
use crate::opt::{PatchOps, Resource};
use crate::types::{RecordIdKeyRange, SurrealValue, Value, Variables};
use crate::{Connection, Result, Surreal};

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

				let what = resource?;

				let mut variables = Variables::new();
				let what = what.for_sql_query(&mut variables)?;

				router
					.$method(
						client.session_id,
						Command::Query {
							txn,
							query: Cow::Owned(format!("UPSERT {what}")),
							variables,
						},
					)
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
	R: SurrealValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Upsert<'r, Client, Vec<R>>
where
	Client: Connection,
	R: SurrealValue,
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
	pub fn range(mut self, range: impl Into<RecordIdKeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<C, R> Upsert<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records to upsert to those in the specified range
	pub fn range(mut self, range: impl Into<RecordIdKeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<'r, C, R> Upsert<'r, C, R>
where
	C: Connection,
	R: SurrealValue,
{
	/// Replaces the current document / record data with the specified data
	pub fn content<D>(self, data: D) -> Content<'r, C, R>
	where
		D: SurrealValue,
	{
		let data = data.into_value();

		Content::from_closure(self.client, self.txn, || {
			validate_data(
				&data,
				"Tried to upsert non-object-like data as content, only structs and objects are supported",
			)?;

			let data = match data {
				Value::None => None,
				content => Some(content),
			};

			let what = self.resource?;

			let mut variables = Variables::new();
			let what = what.for_sql_query(&mut variables)?;

			let query = match data {
				None => Cow::Owned(format!("UPSERT {what}")),
				Some(content) => {
					variables.insert("_content", content);
					Cow::Owned(format!("UPSERT {what} CONTENT $_content"))
				}
			};

			Ok(Command::Query {
				txn: self.txn,
				query,
				variables,
			})
		})
	}

	/// Merges the current document / record data with the specified data
	pub fn merge<D>(self, data: D) -> Merge<'r, C, D, R>
	where
		D: SurrealValue,
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
	pub fn patch(self, patches: impl Into<PatchOps>) -> Patch<'r, C, R> {
		Patch {
			patches: patches.into(),
			txn: self.txn,
			client: self.client,
			resource: self.resource,
			upsert: true,
			response_type: PhantomData,
		}
	}
}
