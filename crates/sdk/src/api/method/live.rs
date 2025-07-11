use crate::Surreal;
use crate::api::Result;
use crate::api::method::BoxFuture;
use crate::opt::Resource;
use crate::value::Notification;
use futures::Stream;
use futures::StreamExt;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use surrealdb_core::dbs::{Action, Notification as CoreNotification};
use surrealdb_core::protocol::TryFromValue;
use surrealdb_core::sql::statements::LiveStatement;
use surrealdb_protocol::proto::rpc::v1::Action as ActionProto;
use surrealdb_protocol::proto::rpc::v1::Notification as NotificationProto;
use surrealdb_protocol::proto::rpc::v1::SubscribeRequest;
use surrealdb_protocol::proto::v1::Value as ValueProto;
use uuid::Uuid;

#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

const ID: &str = "id";

fn deserialize<R>(notification: NotificationProto) -> Option<Result<crate::Notification<R>>>
where
	R: TryFromValue,
{
	match notification.action() {
		ActionProto::Killed => None,
		action => {
			let NotificationProto {
				live_query_id,
				action: _,
				record_id,
				value,
			} = notification;
			let query_id = live_query_id.map(TryInto::try_into)?.ok()?;
			let Some(value) = value else {
				return None;
			};
			let action = action.try_into().ok()?;
			match R::try_from_value(value) {
				Ok(data) => Some(Ok(Notification {
					query_id,
					data,
					action,
				})),
				Err(error) => Some(Err(error)),
			}
		}
	}
}

/// A select future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Subscribe<R, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) resource: R,

	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> IntoFuture for Subscribe<R, RT>
where
	R: Resource,
	RT: TryFrom<ValueProto> + Debug,
	<RT as TryFrom<ValueProto>>::Error: StdError + Send + Sync + 'static,
{
	// type Output = Stream<Result<Notification<RT>>>;
	type Output = Result<Pin<Box<dyn Stream<Item = Result<Notification<RT>>> + Send>>>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Subscribe {
			client,

			resource,
			..
		} = self;

		let what = resource.into_values();
		let what = what.0[0].clone();

		Box::pin(async move {
			let mut client = client.client.clone();

			let mut stmt = LiveStatement::default();
			stmt.what = what.into();

			let response = client
				.subscribe(SubscribeRequest {
					query: stmt.to_string(),
					variables: None,
				})
				.await
				.map_err(anyhow::Error::from)?;

			let stream = response.into_inner();

			let stream = stream.map(move |resp| {
				let resp = resp?;

				let Some(notification) = resp.notification else {
					return Err(anyhow::anyhow!("Notification missing from response"));
				};

				let action = notification.action().try_into()?;

				let Some(live_query_id) = notification.live_query_id else {
					return Err(anyhow::anyhow!("Live query ID missing from response"));
				};

				let live_query_id = live_query_id.try_into()?;
				let Some(value) = notification.value else {
					return Err(anyhow::anyhow!("Value missing from response"));
				};

				let value = RT::try_from(value)?;

				let notification = Notification {
					query_id: live_query_id,
					action,
					data: value,
				};

				return Ok(notification);
			});

			Ok(Box::pin(stream) as Pin<Box<dyn Stream<Item = Result<Notification<RT>>> + Send>>)
		})
	}
}
