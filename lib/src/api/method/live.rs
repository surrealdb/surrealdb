use crate::api::conn::Command;
use crate::api::conn::Router;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::Connection;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::dbs;
use crate::engine::any::Any;
use crate::method::Live;
use crate::method::OnceLockExt;
use crate::method::Query;
use crate::method::Select;
use crate::opt::Resource;
use crate::sql::from_value;
use crate::sql::statements::LiveStatement;
use crate::sql::Field;
use crate::sql::Fields;
use crate::sql::Ident;
use crate::sql::Idiom;
use crate::sql::Part;
use crate::sql::Statement;
use crate::sql::Table;
use crate::sql::Value;
use crate::Notification;
use crate::Surreal;
use channel::Receiver;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn;
use uuid::Uuid;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local as spawn;

const ID: &str = "id";

macro_rules! into_future {
	() => {
		fn into_future(self) -> Self::IntoFuture {
			let Select {
				client,
				resource,
				range,
				..
			} = self;
			Box::pin(async move {
				let router = client.router.extract()?;
				if !router.features.contains(&ExtraFeatures::LiveQueries) {
					return Err(Error::LiveQueriesNotSupported.into());
				}
				let mut fields = Fields::default();
				fields.0 = vec![Field::All];
				let mut stmt = LiveStatement::new(fields);
				let mut table = Table::default();
				match range {
					Some(range) => {
						let record = resource?.with_range(range)?;
						table.0 = record.tb.clone();
						stmt.what = table.into();
						stmt.cond = record.to_cond();
					}
					None => match resource? {
						Resource::Table(table) => {
							stmt.what = table.into();
						}
						Resource::RecordId(record) => {
							table.0 = record.tb.clone();
							stmt.what = table.into();
							let mut ident = Ident::default();
							ident.0 = ID.to_owned();
							let mut idiom = Idiom::default();
							idiom.0 = vec![Part::from(ident)];
							stmt.cond = record.to_cond();
						}
						Resource::Object(object) => return Err(Error::LiveOnObject(object).into()),
						Resource::Array(array) => return Err(Error::LiveOnArray(array).into()),
						Resource::Edges(edges) => return Err(Error::LiveOnEdges(edges).into()),
					},
				}
				let query = Query::new(
					client.clone(),
					vec![Statement::Live(stmt)],
					Default::default(),
					false,
				);
				let Value::Uuid(id) = query.await?.take(0)? else {
					return Err(Error::InternalError(
						"successufull live query didn't return a uuid".to_string(),
					)
					.into());
				};
				let rx = register(router, id.0).await?;
				Ok(Stream::new(
					Surreal::new_from_router_waiter(client.router.clone(), client.waiter.clone()),
					id.0,
					Some(rx),
				))
			})
		}
	};
}

pub(crate) async fn register(router: &Router, id: Uuid) -> Result<Receiver<dbs::Notification>> {
	let (tx, rx) = channel::unbounded();
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

	into_future! {}
}

impl<'r, Client, R> IntoFuture for Select<'r, Client, Option<R>, Live>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Stream<Option<R>>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, Client, R> IntoFuture for Select<'r, Client, Vec<R>, Live>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Stream<Vec<R>>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

/// A stream of live query notifications
#[derive(Debug)]
#[must_use = "streams do nothing unless you poll them"]
pub struct Stream<R> {
	pub(crate) client: Surreal<Any>,
	// We no longer need the lifetime and the type parameter
	// Leaving them in for backwards compatibility
	pub(crate) id: Uuid,
	pub(crate) rx: Option<Receiver<dbs::Notification>>,
	pub(crate) response_type: PhantomData<R>,
}

impl<R> Stream<R> {
	pub(crate) fn new(
		client: Surreal<Any>,
		id: Uuid,
		rx: Option<Receiver<dbs::Notification>>,
	) -> Self {
		Self {
			id,
			rx,
			client,
			response_type: PhantomData,
		}
	}
}

macro_rules! poll_next {
	($notification:ident => $body:expr) => {
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
		notification => Poll::Ready(Some(Notification {
			query_id: notification.id.0,
			action: notification.action.into(),
			data: notification.result,
		}))
	}
}

macro_rules! poll_next_and_convert {
	() => {
		poll_next! {
			notification => match from_value(notification.result) {
				Ok(data) => Poll::Ready(Some(Ok(Notification {
					data,
					query_id: notification.id.0,
					action: notification.action.into(),
				}))),
				Err(error) => Poll::Ready(Some(Err(error.into()))),
			}
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
		if let Ok(router) = client.router.extract() {
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
