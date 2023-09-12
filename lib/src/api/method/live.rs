use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::err::Error;
use crate::api::opt::Range;
use crate::api::Connection;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::dbs;
use crate::dbs::Action;
use crate::opt::from_value;
use crate::opt::Resource;
use crate::sql::Id;
use crate::sql::Table;
use crate::sql::Value;
use channel::Receiver;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use uuid::Uuid;

/// A live query future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Live<'r, C: Connection, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) response_type: PhantomData<R>,
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Live {
				router,
				resource,
				range,
				..
			} = self;
			Box::pin(async move {
				let router = router?;
				if !router.features.contains(&ExtraFeatures::LiveQueries) {
					return Err(Error::LiveQueriesNotSupported.into());
				}
				let payload = match range {
					Some(range) => {
						let _range = resource?.with_range(range)?;
						return Err(crate::err::Error::FeatureNotYetImplemented {
							feature: "live queries on ranges".to_owned(),
						}
						.into());
					}
					None => match resource? {
						Resource::Table(table) => vec![Value::Table(Table(table.0))],
						Resource::RecordId(_record) => {
							return Err(crate::err::Error::FeatureNotYetImplemented {
								feature: "live queries on record IDs".to_owned(),
							}
							.into())
						}
						Resource::Object(object) => return Err(Error::LiveOnObject(object).into()),
						Resource::Array(array) => return Err(Error::LiveOnArray(array).into()),
						Resource::Edges(edges) => return Err(Error::LiveOnEdges(edges).into()),
					},
				};
				let (tx, rx) = channel::unbounded();
				let mut conn = Client::new(Method::Live);
				let mut param = Param::notification_sender(tx);
				param.other = payload;
				let id = conn.$method(router, param).await?;
				Ok(Stream {
					router,
					id,
					rx,
					response_type: PhantomData,
				})
			})
		}
	};
}

impl<'r, Client> IntoFuture for Live<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Stream<'r, Client, Value>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute}
}

impl<'r, Client, R> IntoFuture for Live<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Stream<'r, Client, Option<R>>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute}
}

impl<'r, Client, R> IntoFuture for Live<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Stream<'r, Client, Vec<R>>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {execute}
}

/// A stream of exported data
#[derive(Debug)]
#[must_use = "streams do nothing unless you poll them"]
pub struct Stream<'r, C: Connection, R> {
	router: &'r Router<C>,
	id: Uuid,
	rx: Receiver<dbs::Notification>,
	response_type: PhantomData<R>,
}

impl<Client, R> Drop for Stream<'_, Client, R>
where
	Client: Connection,
{
	fn drop(&mut self) {
		futures::executor::block_on(async move {
			let mut conn = Client::new(Method::Kill);
			if let Err(error) =
				conn.execute_unit(self.router, Param::new(vec![self.id.into()])).await
			{
				error!("Failed to kill live query '{}': {error}", self.id);
			}
		});
	}
}

#[derive(Debug)]
pub struct Notification<R> {
	pub action: Action,
	pub data: R,
}

macro_rules! poll_next {
	($action:ident, $result:ident => $body:expr) => {
		fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
			match self.as_mut().rx.poll_next_unpin(cx) {
				Poll::Ready(Some(dbs::Notification {
					$action,
					$result,
					..
				})) => $body,
				Poll::Ready(None) => Poll::Ready(None),
				Poll::Pending => Poll::Pending,
			}
		}
	};
}

impl<C> futures::Stream for Stream<'_, C, Value>
where
	C: Connection,
{
	type Item = Notification<Value>;

	poll_next! {
		action, result => Poll::Ready(Some(Notification { action, data: result }))
	}
}

macro_rules! poll_next_and_convert {
	() => {
		poll_next! {
			action, result => match from_value(result) {
				Ok(data) => Poll::Ready(Some(Ok(Notification { action, data }))),
				Err(error) => Poll::Ready(Some(Err(error.into()))),
			}
		}
	};
}

impl<C, R> futures::Stream for Stream<'_, C, Option<R>>
where
	C: Connection,
	R: DeserializeOwned + Unpin,
{
	type Item = Result<Notification<R>>;

	poll_next_and_convert! {}
}

impl<C, R> futures::Stream for Stream<'_, C, Vec<R>>
where
	C: Connection,
	R: DeserializeOwned + Unpin,
{
	type Item = Result<Notification<R>>;

	poll_next_and_convert! {}
}
