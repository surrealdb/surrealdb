use crate::Surreal;
use crate::opt::InsertableResource;

use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::method::Content;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{Object, Value, to_value as to_core_value};

use super::insert_relation::InsertRelation;

/// An insert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Insert<'r, C: Connection, R: InsertableResource, RT> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: R,
	pub(super) response_type: PhantomData<RT>,
}

impl<C, R, RT> Insert<'_, C, R, RT>
where
	C: Connection,
	R: InsertableResource,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Insert<'static, C, R, RT> {
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
				let table = resource.table_name();
				let data = resource.default_content();
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

impl<'r, Client, R> IntoFuture for Insert<'r, Client, R, Value>
where
	Client: Connection,
	R: InsertableResource,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R, RT> IntoFuture for Insert<'r, Client, R, Option<RT>>
where
	Client: Connection,
	R: InsertableResource,
	RT: TryFromValue,
{
	type Output = Result<Option<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R, RT> IntoFuture for Insert<'r, Client, R, Vec<RT>>
where
	Client: Connection,
	R: InsertableResource,
	RT: TryFromValue,
{
	type Output = Result<Vec<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<'r, C, R, RT> Insert<'r, C, R, RT>
where
	C: Connection,
	R: InsertableResource,
	RT: TryFromValue,
{
	/// Specifies the data to insert into the table
	pub fn content(self, data: Value) -> Content<'r, C, RT> {
		let table = self.resource.table_name();

		Content::new(
			self.client,
			Command::Insert {
				what: Some(table.into()),
				data,
			},
		)
	}
}

impl<'r, C, R, RT> Insert<'r, C, R, RT>
where
	C: Connection,
	R: InsertableResource,
	RT: TryFromValue,
{
	/// Specifies the data to insert into the table
	pub fn relation(self, data: Value) -> InsertRelation<'r, C, RT> {
		InsertRelation::from_closure(self.client, || {
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
