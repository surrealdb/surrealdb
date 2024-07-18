use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::method::Content;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::Ident;
use crate::sql::Part;
use crate::sql::Table;
use crate::sql::Value;
use crate::Surreal;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

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
					Resource::Table(table) => (table.into(), Value::Object(Default::default())),
					Resource::RecordId(record_id) => {
						let mut table = Table::default();
						table.0 = record_id.tb.clone();
						(table.into(), map! { String::from("id") => record_id.into() }.into())
					}
					Resource::Object(obj) => return Err(Error::InsertOnObject(obj).into()),
					Resource::Array(arr) => return Err(Error::InsertOnArray(arr).into()),
					Resource::Edges(edges) => return Err(Error::InsertOnEdges(edges).into()),
				};
				let param = vec![table, data];
				let router = client.router.extract()?;
				router.$method(Method::Insert, Param::new(param)).await
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
	pub fn content<D>(self, data: D) -> Content<'r, C, Value, R>
	where
		D: Serialize,
	{
		let mut content = Content {
			client: self.client,
			method: Method::Insert,
			resource: self.resource,
			range: None,
			content: Value::None,
			response_type: PhantomData,
		};
		match crate::sql::to_value(data) {
			Ok(mut data) => match content.resource {
				Ok(Resource::Table(table)) => {
					content.resource = Ok(table.into());
					content.content = data;
				}
				Ok(Resource::RecordId(record_id)) => match data.is_array() {
					true => {
						content.resource = Err(Error::InvalidParams(
							"Tried to insert multiple records on a record ID".to_owned(),
						)
						.into());
					}
					false => {
						let mut table = Table::default();
						table.0.clone_from(&record_id.tb);
						content.resource = Ok(table.into());
						let mut ident = Ident::default();
						"id".clone_into(&mut ident.0);
						let id = Part::Field(ident);
						data.put(&[id], record_id.into());
						content.content = data;
					}
				},
				Ok(Resource::Object(obj)) => {
					content.resource = Err(Error::InsertOnObject(obj).into());
				}
				Ok(Resource::Array(arr)) => {
					content.resource = Err(Error::InsertOnArray(arr).into());
				}
				Ok(Resource::Edges(edges)) => {
					content.resource = Err(Error::InsertOnEdges(edges).into());
				}
				Err(error) => {
					content.resource = Err(error);
				}
			},
			Err(error) => {
				content.resource = Err(error.into());
			}
		};
		content
	}
}
