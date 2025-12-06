use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use surrealdb_types::{SurrealValue, Value, Variables};
use uuid::Uuid;

use super::insert_relation::InsertRelation;
use super::transaction::WithTransaction;
use super::validate_data;
use crate::conn::Command;
use crate::err::Error;
use crate::method::{BoxFuture, Content, OnceLockExt};
use crate::opt::Resource;
use crate::{Connection, Result, Surreal};

/// An insert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Insert<'r, C: Connection, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> WithTransaction for Insert<'_, C, R>
where
	C: Connection,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<C, R> Insert<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Insert<'static, C, R> {
		Insert {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Insert {
				txn,
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let router = client.inner.router.extract()?;

				let what_resource = resource?;

				let mut variables = Variables::new();
				let what = what_resource.for_sql_query(&mut variables)?;

				let query = match what_resource {
					Resource::Table(_) => {
						// CREATE accepts a table name expression and works without content
						Cow::Owned(format!("CREATE {what}"))
					}
					Resource::RecordId(_) => Cow::Owned(format!("CREATE {what}")),
					Resource::Object(_) => return Err(Error::InsertOnObject.into()),
					Resource::Array(_) => return Err(Error::InsertOnArray.into()),
					Resource::Range(_) => return Err(Error::InsertOnRange.into()),
				};

				router
					.$method(Command::Query {
						txn,
						query,
						variables,
					})
					.await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Insert<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Insert<'r, Client, Option<R>>
where
	Client: Connection,
	R: SurrealValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Insert<'r, Client, Vec<R>>
where
	Client: Connection,
	R: SurrealValue,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<'r, C, R> Insert<'r, C, R>
where
	C: Connection,
	R: SurrealValue,
{
	/// Specifies the data to insert into the table
	pub fn content<D>(self, data: D) -> Content<'r, C, R>
	where
		D: SurrealValue,
	{
		let mut data = data.into_value();
		Content::from_closure(self.client, self.txn, || {
			validate_data(
				&data,
				"Tried to insert non-object-like data as content, only structs and objects are supported",
			)?;

			let what_resource = self.resource?;

			let mut variables = Variables::new();
			let what = what_resource.for_sql_query(&mut variables)?;

			let query = match what_resource {
				Resource::Table(_) => {
					if data.is_array() {
						Cow::Owned(format!("INSERT INTO {what} $_data"))
					} else {
						// Single object - use CREATE with CONTENT
						Cow::Owned(format!("CREATE {what} CONTENT $_data"))
					}
				}
				Resource::RecordId(record_id) => {
					if data.is_array() {
						return Err(Error::InvalidParams(
							"Tried to insert multiple records on a record ID".to_owned(),
						));
					}

					if let Value::Object(ref mut x) = data {
						x.insert("id".to_string(), record_id.key.into_value());
					}

					Cow::Owned(format!("CREATE {what} CONTENT $_data"))
				}
				Resource::Object(_) => return Err(Error::InsertOnObject),
				Resource::Array(_) => return Err(Error::InsertOnArray),
				Resource::Range(_) => return Err(Error::InsertOnRange),
			};

			variables.insert("_data".to_string(), data);

			Ok(Command::Query {
				txn: self.txn,
				query,
				variables,
			})
		})
	}
}

impl<'r, C, R> Insert<'r, C, R>
where
	C: Connection,
	R: SurrealValue,
{
	/// Specifies the data to insert into the table
	pub fn relation<D>(self, data: D) -> InsertRelation<'r, C, R>
	where
		D: SurrealValue,
	{
		InsertRelation::from_closure(self.client, || {
			let mut data = data.into_value();
			validate_data(
				&data,
				"Tried to insert non-object-like data as relation data, only structs and objects are supported",
			)?;

			let what_resource = self.resource?;

			let mut variables = Variables::new();
			let what = what_resource.for_sql_query(&mut variables)?;

			let query = match what_resource {
				Resource::Table(_) => Cow::Owned(format!("INSERT RELATION INTO {what} $_data;")),
				Resource::RecordId(record_id) => {
					if data.is_array() {
						return Err(Error::InvalidParams(
							"Tried to insert multiple records on a record ID".to_owned(),
						));
					}

					if let Value::Object(ref mut x) = data {
						x.insert("id".to_string(), record_id.key.into_value());
					}

					Cow::Owned(format!("INSERT RELATION INTO {what} $_data RETURN AFTER"))
				}
				Resource::Array(_) => return Err(Error::InsertOnArray),
				Resource::Range(_) => return Err(Error::InsertOnRange),
				Resource::Object(_) => return Err(Error::InsertOnObject),
			};

			variables.insert("_data".to_string(), data);

			Ok(Command::Query {
				txn: self.txn,
				query,
				variables,
			})
		})
	}
}
