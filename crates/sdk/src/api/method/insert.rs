use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::insert_relation::InsertRelation;
use super::transaction::WithTransaction;
use super::validate_data;
use crate::api::conn::Command;
use crate::api::err::Error;
use crate::api::method::{BoxFuture, Content};
use crate::api::opt::Resource;
use crate::api::{self, Connection, Result};
use crate::core::val;
use crate::method::OnceLockExt;
use crate::{Surreal, Value};

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
				let (table, data) = match resource? {
					Resource::Table(table) => (table.into(), val::Object::default()),
					Resource::RecordId(record_id) => {
						let record_id = record_id.into_inner();
						let mut map = val::Object::default();
						map.insert("id".to_string(), record_id.key.into_value());
						(record_id.table, map)
					}
					Resource::Object(_) => return Err(Error::InsertOnObject.into()),
					Resource::Array(_) => return Err(Error::InsertOnArray.into()),
					Resource::Range {
						..
					} => return Err(Error::InsertOnRange.into()),
					Resource::Unspecified => return Err(Error::InsertOnUnspecified.into()),
				};
				let cmd = Command::Insert {
					txn,
					what: Some(table.to_string()),
					data: data.into(),
				};

				let router = client.inner.router.extract()?;
				router.$method(cmd).await
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
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Insert<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<'r, C, R> Insert<'r, C, R>
where
	C: Connection,
	R: DeserializeOwned,
{
	/// Specifies the data to insert into the table
	pub fn content<D>(self, data: D) -> Content<'r, C, R>
	where
		D: Serialize + 'static,
	{
		Content::from_closure(self.client, self.txn, || {
			let mut data = api::value::to_core_value(data)?;
			validate_data(
				&data,
				"Tried to insert non-object-like data as content, only structs and objects are supported",
			)?;
			match self.resource? {
				Resource::Table(table) => Ok(Command::Insert {
					txn: self.txn,
					what: Some(table),
					data,
				}),
				Resource::RecordId(thing) => {
					if data.is_array() {
						Err(Error::InvalidParams(
							"Tried to insert multiple records on a record ID".to_owned(),
						)
						.into())
					} else {
						let thing = thing.into_inner();
						if let val::Value::Object(ref mut x) = data {
							x.insert("id".to_string(), thing.key.into_value());
						}

						Ok(Command::Insert {
							txn: self.txn,
							what: Some(thing.table),
							data,
						})
					}
				}
				Resource::Object(_) => Err(Error::InsertOnObject.into()),
				Resource::Array(_) => Err(Error::InsertOnArray.into()),
				Resource::Range(_) => Err(Error::InsertOnRange.into()),
				Resource::Unspecified => Ok(Command::Insert {
					txn: self.txn,
					what: None,
					data,
				}),
			}
		})
	}
}

impl<'r, C, R> Insert<'r, C, R>
where
	C: Connection,
	R: DeserializeOwned,
{
	/// Specifies the data to insert into the table
	pub fn relation<D>(self, data: D) -> InsertRelation<'r, C, R>
	where
		D: Serialize + 'static,
	{
		InsertRelation::from_closure(self.client, || {
			let mut data = api::value::to_core_value(data)?;
			validate_data(
				&data,
				"Tried to insert non-object-like data as relation data, only structs and objects are supported",
			)?;
			match self.resource? {
				Resource::Table(table) => Ok(Command::InsertRelation {
					txn: self.txn,
					what: Some(table),
					data,
				}),
				Resource::RecordId(thing) => {
					if data.is_array() {
						Err(Error::InvalidParams(
							"Tried to insert multiple records on a record ID".to_owned(),
						)
						.into())
					} else {
						let thing = thing.into_inner();
						if let val::Value::Object(ref mut x) = data {
							x.insert("id".to_string(), thing.key.into_value());
						}

						Ok(Command::InsertRelation {
							txn: self.txn,
							what: Some(thing.table),
							data,
						})
					}
				}
				Resource::Unspecified => Ok(Command::InsertRelation {
					txn: self.txn,
					what: None,
					data,
				}),
				Resource::Object(_) => Err(Error::InsertOnObject.into()),
				Resource::Array(_) => Err(Error::InsertOnArray.into()),
				Resource::Range(_) => Err(Error::InsertOnRange.into()),
			}
		})
	}
}
