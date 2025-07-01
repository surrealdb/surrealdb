use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::PatchOp;
use crate::opt::PatchOps;
use crate::opt::RangeableResource;


use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;

use crate::opt::KeyRange;
use serde::Serialize;
use surrealdb_protocol::proto::rpc::v1::UpdateRequest;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::Data;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{Value, to_value as to_core_value};
use uuid::Uuid;

/// An update future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Update<R: Resource, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) what: R,
	pub(super) data: Data,
	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> WithTransaction for Update<R, RT>
where 	R: Resource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT> IntoFuture for Update<R, RT>
where
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Update {
			txn,
			client,
			what,
			data,
			..
		} = self;
		Box::pin(async move {
			let client = client.client;

			// let response = client.update(UpdateRequest {
			// 	txn: txn.map(TryInto::try_into).transpose()?,
			// 	what: what.into_values(),
			// 	data: data.map(TryInto::try_into).transpose()?,
			// 	..Default::default()
			// }).await?;
			// let response = response.into_inner();

			todo!("STUB: Update<R, RT> future");
			// Ok(RT::try_from_value(response.data)?)
		})
	}
}

// impl<C, R> Update<'_, C, R, Value>
// where
// 	C
// 	R: RangeableResource,
// {
// 	/// Restricts the records to update to those in the specified range
// 	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
// 		self.resource = self.resource.with_range(range.into());
// 		self
// 	}
// }

// impl<C, R, RT> Update<'_, C, R, Vec<RT>>
// where
// 	C
// 	R: RangeableResource,
// {
// 	/// Restricts the records to update to those in the specified range
// 	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
// 		self.resource = self.resource.with_range(range.into());
// 		self
// 	}
// }

impl<R, RT> Update<R, RT>
where R: Resource,
	RT: TryFromValue,
{
	/// Replaces the current document / record data with the specified data
	pub fn content(self, data: Value) -> Update<R, RT> {
		let data = match data {
			Value::None => Data::EmptyExpression,
			content => Data::ContentExpression(content),
		};


		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data,
			response_type: PhantomData,
		}
	}

	/// Merges the current document / record data with the specified data
	pub fn merge(self, data: Value) -> Update<R, RT> {
		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data: Data::MergeExpression(data),
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch data
	pub fn patch(self, patch: impl Into<PatchOp>) -> Update<R, RT> {
		let patch = patch.into().into();
		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data: Data::PatchExpression(patch),
			response_type: PhantomData,
		}
	}
}
