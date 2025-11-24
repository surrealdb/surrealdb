use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::ops::Bound;

use uuid::Uuid;

use super::transaction::WithTransaction;
use super::validate_data;
use crate::conn::Command;
use crate::method::{BoxFuture, Content, Merge, OnceLockExt, Patch};
use crate::opt::{PatchOps, Resource};
use crate::types::{RecordId, RecordIdKeyRange, SurrealValue, Value, Variables};
use crate::{Connection, Error, Result, Surreal};

/// An update future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Update<'r, C: Connection, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> WithTransaction for Update<'_, C, R>
where
	C: Connection,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<C, R> Update<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
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
							query: Cow::Owned(format!("UPDATE {what}")),
							variables,
						},
					)
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
	R: SurrealValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Update<'r, Client, Vec<R>>
where
	Client: Connection,
	R: SurrealValue,
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
	pub fn range(mut self, range: impl Into<RecordIdKeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<C, R> Update<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records to update to those in the specified range
	pub fn range(mut self, range: impl Into<RecordIdKeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<'r, C, R> Update<'r, C, R>
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
				"Tried to update non-object-like data as content, only structs and objects are supported",
			)?;

			let what_resource = self.resource?;

			let mut variables = Variables::new();
			let what = what_resource.for_sql_query(&mut variables)?;

			let content_str = match data {
				Value::None => "",
				content => {
					variables.insert("_content".to_string(), content);
					"CONTENT $_content"
				}
			};

			let query = match what_resource {
				Resource::Table(_) => Cow::Owned(format!("UPDATE {what} {content_str}")),
				Resource::RecordId(_) => Cow::Owned(format!("UPDATE {what} {content_str}")),
				Resource::Range(range) => {
					let mut conditions = Vec::new();
					match range.range.start {
						Bound::Included(start) => {
							variables.insert(
								"_start".to_string(),
								Value::RecordId(RecordId::new(range.table.clone(), start)),
							);
							conditions.push("id >= $_start");
						}
						Bound::Excluded(start) => {
							variables.insert(
								"_start".to_string(),
								Value::RecordId(RecordId::new(range.table.clone(), start)),
							);
							conditions.push("id > $_start");
						}
						Bound::Unbounded => {}
					}
					match range.range.end {
						Bound::Included(end) => {
							variables.insert(
								"_end".to_string(),
								Value::RecordId(RecordId::new(range.table, end)),
							);
							conditions.push("id <= $_end");
						}
						Bound::Excluded(end) => {
							variables.insert(
								"_end".to_string(),
								Value::RecordId(RecordId::new(range.table, end)),
							);
							conditions.push("id < $_end");
						}
						Bound::Unbounded => {}
					}

					Cow::Owned(format!(
						"UPDATE {what} {content_str} WHERE {}",
						conditions.join(" AND ")
					))
				}
				Resource::Object(_) => {
					return Err(Error::InvalidParams("Update on object not supported".to_string()));
				}
				Resource::Array(_) => {
					return Err(Error::InvalidParams("Update on array not supported".to_string()));
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
			upsert: false,
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
			upsert: false,
			response_type: PhantomData,
		}
	}
}
