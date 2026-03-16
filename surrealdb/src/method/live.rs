use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_channel::Receiver;
use futures::StreamExt;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

use crate::conn::{Command, Router};
use crate::engine::any::Any;
use crate::method::{BoxFuture, Live, OnceLockExt, Select};
use crate::notification::Notification;
use crate::opt::Resource;
use crate::types::{
	Action, Notification as CoreNotification, RecordId, SurrealValue, Value, Variables,
};
use crate::{Connection, Error, ExtraFeatures, Result, Surreal};

fn into_future<C, O>(this: Select<C, O, Live>) -> BoxFuture<Result<Stream<O>>>
where
	C: Connection,
{
	let Select {
		client,
		resource,
		..
	} = this;
	Box::pin(async move {
		let router = client.inner.router.extract()?;
		if !router.features.contains(&ExtraFeatures::LiveQueries) {
			return Err(Error::internal(
				"The protocol or storage engine does not support live queries on this architecture"
					.to_string(),
			));
		}

		let what_resource = resource?;

		let mut variables = Variables::new();
		let what = what_resource.for_sql_query(&mut variables)?;

		// Generate the LIVE SELECT SQL based on resource type
		let query = match what_resource {
			Resource::Table(table) => {
				variables.insert("_table".to_string(), Value::Table(table));
				format!("LIVE SELECT * FROM {what}")
			}
			Resource::RecordId(record) => {
				// For a specific record, we need to query the table with a WHERE clause
				// because LIVE queries don't support record IDs directly
				variables.insert("_table".to_string(), Value::Table(record.table.clone()));
				variables.insert("_record_id".to_string(), Value::RecordId(record));
				"LIVE SELECT * FROM $_table WHERE id = $_record_id".to_string()
			}
			Resource::Object(_) => {
				return Err(Error::internal("Live queries on objects not supported".to_string()));
			}
			Resource::Array(_) => {
				return Err(Error::internal("Live queries on arrays not supported".to_string()));
			}
			Resource::Range(query_range) => {
				// For live queries with ranges, we can't use the range in FROM clause
				// We need to use the table and add WHERE conditions
				variables.insert("_table".to_string(), Value::Table(query_range.table.clone()));
				let table_expr = "$_table";

				// Build WHERE clause for range queries
				let mut conditions = Vec::new();

				// Handle start bound
				match &query_range.range.start {
					std::ops::Bound::Included(key) => {
						variables.insert(
							"_start".to_string(),
							Value::RecordId(RecordId::new(query_range.table.clone(), key.clone())),
						);
						conditions.push("id >= $_start");
					}
					std::ops::Bound::Excluded(key) => {
						variables.insert(
							"_start".to_string(),
							Value::RecordId(RecordId::new(query_range.table.clone(), key.clone())),
						);
						conditions.push("id > $_start");
					}
					std::ops::Bound::Unbounded => {}
				}

				// Handle end bound
				match &query_range.range.end {
					std::ops::Bound::Included(key) => {
						variables.insert(
							"_end".to_string(),
							Value::RecordId(RecordId::new(query_range.table.clone(), key.clone())),
						);
						conditions.push("id <= $_end");
					}
					std::ops::Bound::Excluded(key) => {
						variables.insert(
							"_end".to_string(),
							Value::RecordId(RecordId::new(query_range.table.clone(), key.clone())),
						);
						conditions.push("id < $_end");
					}
					std::ops::Bound::Unbounded => {}
				}

				// Build final query
				if conditions.is_empty() {
					format!("LIVE SELECT * FROM {table_expr}")
				} else {
					format!("LIVE SELECT * FROM {table_expr} WHERE {}", conditions.join(" AND "))
				}
			}
		};

		// Execute the LIVE SELECT query directly to get the UUID
		let results = router
			.execute_query(
				client.session_id,
				Command::Query {
					query: Cow::Owned(query),
					txn: None,
					variables,
				},
			)
			.await?;

		// Get the first result which should be the UUID
		let result = results
			.into_iter()
			.next()
			.ok_or_else(|| Error::internal("LIVE query returned no results".to_string()))?;

		let id = match result.result? {
			Value::Uuid(id) => *id,
			Value::Array(mut arr) if arr.len() == 1 => match arr.pop() {
				Some(Value::Uuid(id)) => *id,
				_ => {
					return Err(Error::internal(
						"successful live query didn't return a uuid".to_string(),
					));
				}
			},
			other => {
				return Err(Error::internal(format!(
					"successful live query didn't return a uuid, got: {:?}",
					other
				)));
			}
		};

		let rx = register(router, id, client.session_id).await?;
		Ok(Stream::new(client.inner.clone().into(), id, Some(rx)))
	})
}

pub(crate) async fn register(
	router: &Router,
	id: Uuid,
	session_id: Uuid,
) -> Result<Receiver<Result<CoreNotification>>> {
	let (tx, rx) = async_channel::unbounded();
	router
		.execute_unit(
			session_id,
			Command::SubscribeLive {
				uuid: id,
				notification_sender: tx,
			},
		)
		.await?;
	Ok(rx)
}

impl<'r, Client> IntoFuture for Select<'r, Client, Value, Live>
where
	Client: Connection,
{
	type Output = Result<Stream<Value>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		into_future(self)
	}
}

impl<'r, Client, R> IntoFuture for Select<'r, Client, Option<R>, Live>
where
	Client: Connection,
	R: SurrealValue,
{
	type Output = Result<Stream<Option<R>>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		into_future(self)
	}
}

impl<'r, Client, R> IntoFuture for Select<'r, Client, Vec<R>, Live>
where
	Client: Connection,
	R: SurrealValue,
{
	type Output = Result<Stream<Vec<R>>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		into_future(self)
	}
}

/// A stream of live query notifications
#[derive(Debug)]
#[must_use = "streams do nothing unless you poll them"]
pub struct Stream<R> {
	pub(crate) client: Surreal<Any>,
	// We no longer need the lifetime and the type parameter
	// Leaving them in for backwards compatibility
	pub(crate) id: Uuid,
	pub(crate) rx: Option<Pin<Box<Receiver<Result<CoreNotification>>>>>,
	pub(crate) response_type: PhantomData<R>,
}

impl<R> Stream<R> {
	pub(crate) fn new(
		client: Surreal<Any>,
		id: Uuid,
		rx: Option<Receiver<Result<CoreNotification>>>,
	) -> Self {
		Self {
			id,
			rx: rx.map(Box::pin),
			client,
			response_type: PhantomData,
		}
	}
}

macro_rules! poll_next {
	($result:ident => $body:expr_2021) => {
		fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
			let Some(ref mut rx) = self.as_mut().rx else {
				return Poll::Ready(None);
			};
			match rx.poll_next_unpin(cx) {
				Poll::Ready(Some($result)) => $body,
				Poll::Ready(None) => Poll::Ready(None),
				Poll::Pending => Poll::Pending,
			}
		}
	};
}

impl futures::Stream for Stream<Value> {
	type Item = Result<Notification<Value>>;

	poll_next! {
		result => match result {
			Ok(notification) => {
				match notification.action {
					Action::Killed => Poll::Ready(None),
					action => Poll::Ready(Some(Ok(Notification {
						query_id: notification.id,
						action,
						data: notification.result,
					}))),
				}
			}
			Err(error) => {
				Poll::Ready(Some(Err(error)))
			}
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		(0, None)
	}
}

macro_rules! poll_next_and_convert {
	() => {
		poll_next! {
			result => match result {
				Ok(notification) => {
					Poll::Ready(deserialize(notification))
				}
				Err(error) => {
					Poll::Ready(Some(Err(error)))
				}
			}
		}
	};
}

impl<R> futures::Stream for Stream<Option<R>>
where
	R: SurrealValue + Unpin,
{
	type Item = Result<Notification<R>>;

	poll_next_and_convert! {}
}

impl<R> futures::Stream for Stream<Vec<R>>
where
	R: SurrealValue + Unpin,
{
	type Item = Result<Notification<R>>;

	poll_next_and_convert! {}
}

impl<R> futures::Stream for Stream<Notification<R>>
where
	R: SurrealValue + Unpin,
{
	type Item = Result<Notification<R>>;

	poll_next_and_convert! {}
}

pub(crate) fn kill<Client>(client: &Surreal<Client>, uuid: Uuid)
where
	Client: Connection,
{
	let client = client.clone();
	spawn(async move {
		if let Ok(router) = client.inner.router.extract() {
			router
				.execute_unit(
					client.session_id,
					Command::Kill {
						uuid,
					},
				)
				.await
				.ok();
		}
	});
}

impl<R> Drop for Stream<R> {
	/// Close the live query stream
	///
	/// This kills the live query process responsible for this stream.
	fn drop(&mut self) {
		if self.rx.is_some() {
			kill(&self.client, self.id);
		}
	}
}

fn deserialize<R>(notification: CoreNotification) -> Option<Result<Notification<R>>>
where
	R: SurrealValue,
{
	let query_id = notification.id;
	let action = notification.action;
	match action {
		Action::Killed => None,
		action => match R::from_value(notification.result) {
			Ok(data) => Some(Ok(Notification {
				query_id,
				data,
				action,
			})),
			Err(error) => Some(Err(Error::internal(error.to_string()))),
		},
	}
}
