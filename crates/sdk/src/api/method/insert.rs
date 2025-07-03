use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::InsertableResource;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;

use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::{Data, TryFromValue};
use surrealdb_core::expr::{Object, Value, to_value as to_core_value};
use surrealdb_protocol::proto::rpc::v1::InsertRequest;
use uuid::Uuid;

use super::insert_relation::InsertRelation;

/// An insert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Insert<R: InsertableResource, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) what: R,
	pub(super) data: Data,
	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> WithTransaction for Insert<R, RT>
where
	R: InsertableResource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT> IntoFuture for Insert<R, RT>
where
	R: InsertableResource,
	RT: TryFromValue,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Insert {
			txn,
			client,
			what,
			data,
			..
		} = self;
		Box::pin(async move {
			let client = client.client;

			// let response = client.insert(InsertRequest {
			// 	txn: txn.map(TryInto::try_into).transpose()?,
			// 	what: what.into_values().into_iter().map(TryInto::try_into).collect::<Result<Vec<_>>>()?,
			// 	data,
			// }).await?;

			todo!("STU: Implement InsertResponse");
		})
	}
}

impl<R, RT> Insert<R, RT>
where
	R: InsertableResource,
	RT: TryFromValue,
{
	/// Specifies the data to insert into the table
	pub fn content(self, data: Value) -> Insert<R, RT> {
		todo!("STU: Implement Insert::content");
		// let table = self.what.table_name();

		// Self {
		// 	txn: self.txn,
		// 	client: self.client,
		// 	what: self.what,
		// 	data: Some(data.try_into()?),
		// 	response_type: PhantomData,
		// }
	}
}

impl<R, RT> Insert<R, RT>
where
	R: InsertableResource,
	RT: TryFromValue,
{
	/// Specifies the data to insert into the table
	pub fn relation(self, data: Value) -> InsertRelation<RT> {
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
