use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_channel::Receiver;
use futures::StreamExt;
use serde::de::DeserializeOwned;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

use crate::api::conn::{Command, Router};
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::{self, Connection, ExtraFeatures, Result};
use crate::core::dbs::{Action as CoreAction, Notification as CoreNotification};
use crate::core::expr::{
	BinaryOperator, Cond, Expr, Fields, Ident, Idiom, Literal, LiveStatement, TopLevelExpr,
};
use crate::core::val;
use crate::engine::any::Any;
use crate::method::{Live, OnceLockExt, Query, Select};
use crate::opt::Resource;
use crate::value::Notification;
use crate::{Action, Surreal, Value};

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
			return Err(Error::LiveQueriesNotSupported.into());
		}
		let mut stmt = LiveStatement::new(Fields::all());
		match resource? {
			Resource::Table(table) => {
				stmt.what = Expr::Table(unsafe { Ident::new_unchecked(table) });
			}
			Resource::RecordId(record) => {
				let record = record.into_inner();
				stmt.what = Expr::Table(unsafe { Ident::new_unchecked(record.table.clone()) });
				let ident = Ident::new("id".to_string()).unwrap();
				let cond = Expr::Binary {
					left: Box::new(Expr::Idiom(Idiom::field(ident))),
					op: BinaryOperator::Equal,
					right: Box::new(Expr::Literal(Literal::RecordId(record.into_literal()))),
				};
				stmt.cond = Some(Cond(cond));
			}
			Resource::Object(_) => return Err(Error::LiveOnObject.into()),
			Resource::Array(_) => return Err(Error::LiveOnArray.into()),
			Resource::Range(range) => {
				let record = range.into_inner();

				let val::RecordIdKey::Range(range) = record.key else {
					panic!("invalid resource?");
				};

				stmt.what = Expr::Table(unsafe { Ident::new_unchecked(record.table.clone()) });

				let id = Expr::Idiom(Idiom::field(Ident::new("id".to_string()).unwrap()));

				let left = match range.start {
					std::ops::Bound::Included(x) => Some(Expr::Binary {
						left: Box::new(id.clone()),
						op: BinaryOperator::MoreThanEqual,
						right: Box::new(Expr::Literal(Literal::RecordId(
							crate::core::expr::RecordIdLit {
								table: record.table.clone(),
								key: x.into_literal(),
							},
						))),
					}),
					std::ops::Bound::Excluded(x) => Some(Expr::Binary {
						left: Box::new(id.clone()),
						op: BinaryOperator::MoreThan,
						right: Box::new(Expr::Literal(Literal::RecordId(
							crate::core::expr::RecordIdLit {
								table: record.table.clone(),
								key: x.into_literal(),
							},
						))),
					}),
					std::ops::Bound::Unbounded => None,
				};
				let right = match range.end {
					std::ops::Bound::Included(x) => Some(Expr::Binary {
						left: Box::new(id),
						op: BinaryOperator::LessThanEqual,
						right: Box::new(Expr::Literal(Literal::RecordId(
							crate::core::expr::RecordIdLit {
								table: record.table,
								key: x.into_literal(),
							},
						))),
					}),
					std::ops::Bound::Excluded(x) => Some(Expr::Binary {
						left: Box::new(id),
						op: BinaryOperator::LessThan,
						right: Box::new(Expr::Literal(Literal::RecordId(
							crate::core::expr::RecordIdLit {
								table: record.table,
								key: x.into_literal(),
							},
						))),
					}),
					std::ops::Bound::Unbounded => None,
				};

				let cond = match (left, right) {
					(Some(l), Some(r)) => Some(Cond(Expr::Binary {
						left: Box::new(l),
						op: BinaryOperator::And,
						right: Box::new(r),
					})),
					(Some(x), None) | (None, Some(x)) => Some(Cond(x)),
					_ => None,
				};

				stmt.cond = cond
			}
			Resource::Unspecified => return Err(Error::LiveOnUnspecified.into()),
		}
		let query = Query::normal(
			client.clone(),
			vec![TopLevelExpr::Live(Box::new(stmt))],
			Default::default(),
			false,
		);
		let val::Value::Uuid(id) = query.await?.take::<Value>(0)?.into_inner() else {
			return Err(Error::InternalError(
				"successufull live query didn't return a uuid".to_string(),
			)
			.into());
		};
		let rx = register(router, *id).await?;
		Ok(Stream::new(client.inner.clone().into(), *id, Some(rx)))
	})
}

pub(crate) async fn register(router: &Router, id: Uuid) -> Result<Receiver<CoreNotification>> {
	let (tx, rx) = async_channel::unbounded();
	router
		.execute_unit(Command::SubscribeLive {
			uuid: id,
			notification_sender: tx,
		})
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
	R: DeserializeOwned,
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
	R: DeserializeOwned,
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
	pub(crate) rx: Option<Pin<Box<Receiver<CoreNotification>>>>,
	pub(crate) response_type: PhantomData<R>,
}

impl<R> Stream<R> {
	pub(crate) fn new(
		client: Surreal<Any>,
		id: Uuid,
		rx: Option<Receiver<CoreNotification>>,
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
	($notification:ident => $body:expr_2021) => {
		fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
			let Some(ref mut rx) = self.as_mut().rx else {
				return Poll::Ready(None);
			};
			match rx.poll_next_unpin(cx) {
				Poll::Ready(Some($notification)) => $body,
				Poll::Ready(None) => Poll::Ready(None),
				Poll::Pending => Poll::Pending,
			}
		}
	};
}

impl futures::Stream for Stream<Value> {
	type Item = Notification<Value>;

	poll_next! {
		notification => {
			match notification.action {
				CoreAction::Killed => Poll::Ready(None),
				action => Poll::Ready(Some(Notification {
					query_id: *notification.id,
					action: Action::from_core(action),
					data: Value::from_inner(notification.result),
				})),
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
			notification => Poll::Ready(deserialize(notification))
		}
	};
}

impl<R> futures::Stream for Stream<Option<R>>
where
	R: DeserializeOwned + Unpin,
{
	type Item = Result<Notification<R>>;

	poll_next_and_convert! {}
}

impl<R> futures::Stream for Stream<Vec<R>>
where
	R: DeserializeOwned + Unpin,
{
	type Item = Result<Notification<R>>;

	poll_next_and_convert! {}
}

impl<R> futures::Stream for Stream<Notification<R>>
where
	R: DeserializeOwned + Unpin,
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
				.execute_unit(Command::Kill {
					uuid,
				})
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

fn deserialize<R>(notification: CoreNotification) -> Option<Result<crate::Notification<R>>>
where
	R: DeserializeOwned,
{
	let query_id = *notification.id;
	let action = notification.action;
	match action {
		CoreAction::Killed => None,
		action => match api::value::from_core_value(notification.result) {
			Ok(data) => Some(Ok(Notification {
				query_id,
				data,
				action: Action::from_core(action),
			})),
			Err(error) => Some(Err(error)),
		},
	}
}
