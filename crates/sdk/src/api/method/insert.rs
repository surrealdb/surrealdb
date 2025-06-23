use crate::Surreal;

use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::method::Content;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use surrealdb_core::expr::TryFromValue;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::{Object as Object, Value as Value, to_value as to_core_value};

use super::insert_relation::InsertRelation;
use super::ensure_values_are_objects;

/// An insert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Insert<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Insert<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
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
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let (table, data) = match resource? {
					Resource::Table(table) => (table.into(), Object::default()),
					Resource::RecordId(record_id) => {
						let mut map = Object::default();
						map.insert("id".to_string(), record_id.id.into());
						(record_id.tb, map)
					}
					Resource::Object(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Object".to_string()).into()),
					Resource::Array(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Array".to_string()).into()),
					Resource::Edge {
						..
					} => return Err(Error::InvalidInsertionResource("Attempted to insert on Edge".to_string()).into()),
					Resource::Range {
						..
					} => return Err(Error::InvalidInsertionResource("Attempted to insert on Range".to_string()).into()),
					Resource::Unspecified => return Err(Error::InvalidInsertionResource("Attempted to insert on Unspecified".to_string()).into()),
				};
				let cmd = Command::Insert {
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
	R: TryFromValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Insert<'r, Client, Vec<R>>
where
	Client: Connection,
	R: TryFromValue,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<'r, C, R> Insert<'r, C, R>
where
	C: Connection,
	R: TryFromValue,
{
	/// Specifies the data to insert into the table
	pub fn content(self, mut data: Value) -> Result<Content<'r, C, R>> {
		ensure_values_are_objects(
			&data,
		)?;
		let cmd = match self.resource? {
			Resource::Table(table) => Command::Insert {
				what: Some(table),
				data,
			},
			Resource::RecordId(thing) => {
				if data.is_array() {
					return Err(Error::InvalidParams(
						"Tried to insert multiple records on a record ID".to_owned(),
					)
					.into())
				}

				if let Value::Object(ref mut x) = data {
					x.insert("id".to_string(), thing.id.into());
				}

				Command::Insert {
					what: Some(thing.tb),
					data,
				}
			}
			Resource::Object(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Object".to_string()).into()),
			Resource::Array(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Array".to_string()).into()),
			Resource::Edge(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Edge".to_string()).into()),
			Resource::Range(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Range".to_string()).into()),
			Resource::Unspecified => Command::Insert {
				what: None,
				data,
			},
		};

		Ok(Content::new(self.client, cmd))
	}
}

impl<'r, C, R> Insert<'r, C, R>
where
	C: Connection,
	R: TryFromValue,
{
	/// Specifies the data to insert into the table
	pub fn relation(self, data: Value) -> InsertRelation<'r, C, R>
	{
		InsertRelation::from_closure(self.client, || {
			ensure_values_are_objects(
				&data,
			)?;
			todo!("STU: Implement InsertRelation for Insert");
			// match self.resource? {
			// 	Resource::Table(table) => Ok(Command::InsertRelation {
			// 		what: Some(table),
			// 		data,
			// 	}),
			// 	Resource::RecordId(thing) => {
			// 		if data.is_array() {
			// 			Err(Error::InvalidParams(
			// 				"Tried to insert multiple records on a record ID".to_owned(),
			// 			)
			// 			.into())
			// 		} else {
			// 			if let Value::Object(ref mut x) = data {
			// 				x.insert("id".to_string(), thing.id.into());
			// 			}

			// 			Ok(Command::InsertRelation {
			// 				what: Some(thing.tb),
			// 				data,
			// 			})
			// 		}
			// 	}
			// 	Resource::Unspecified => Ok(Command::InsertRelation {
			// 		what: None,
			// 		data,
			// 	}),
			// 	Resource::Object(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Object".to_string()).into()),
			// 	Resource::Array(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Array".to_string()).into()),
			// 	Resource::Edge(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Edge".to_string()).into()),
			// 	Resource::Range(_) => return Err(Error::InvalidInsertionResource("Attempted to insert on Range".to_string()).into()),
			// }
		})
	}
}
