use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::RangeableResource;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;

use crate::opt::KeyRange;
use serde::de::DeserializeOwned;
use surrealdb_protocol::proto::rpc::v1::DeleteRequest;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::Value;
use uuid::Uuid;

/// A record delete future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Delete<R, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) resource: R,
	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> WithTransaction for Delete<R, RT>
where R: Resource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT> Delete<R, RT>
where R: Resource,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Delete<R, RT> {
		Delete {
			client: self.client,
			..self
		}
	}
}


impl<R, RT> IntoFuture for Delete<R, RT>
where
	R: Resource + 'static,
	RT: TryFromValue,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Delete {
			txn,
			mut client,
			resource,
			..
		} = self;
		Box::pin(async move {
			let what = resource.into_values().into_iter().map(TryInto::try_into).collect::<Result<Vec<_>>>()?;
			let client = &mut client.client;

			let response = client.delete(DeleteRequest {
				txn: txn.map(|id| id.try_into()).transpose()?,
				what,
				..Default::default()
			}).await?;

			todo!("STU: Implement DeleteResponse");
		})
	}
}

// impl<C, R, NewResource> Delete<'_, C, R, Value>
// where
// 	C
// 	R: RangeableResource,
// {
// 	/// Restricts a range of records to delete
// 	pub fn range(self, range: impl Into<KeyRange>) -> Self {
// 		Self {
// 			resource: self.resource.with_range(range.into()),
// 			..self
// 		}
// 	}
// }

// impl<C, R, RT, NewResource> Delete<'_, C, R, Vec<RT>>
// where
// 	C
// 	R: RangeableResource,
// 	NewResource: RangeableResource,
// {
// 	/// Restricts a range of records to delete
// 	pub fn range<'a>(self, range: impl Into<KeyRange>) -> Delete<'a, C, NewResource, Vec<RT>> {
// 		Delete {
// 			resource: self.resource.with_range(range.into()),
// 			client: Cow::Borrowed(self.client.as_ref()),
// 			response_type: PhantomData,
// 		}
// 	}
// }
