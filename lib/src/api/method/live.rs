use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::err::Error;
use crate::api::opt::Range;
use crate::api::Connection;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::dbs;
use crate::method::OnceLockExt;
use crate::method::Query;
use crate::opt::from_value;
use crate::opt::Resource;
use crate::sql::cond::Cond;
use crate::sql::expression::Expression;
use crate::sql::field::Field;
use crate::sql::field::Fields;
use crate::sql::ident::Ident;
use crate::sql::idiom::Idiom;
use crate::sql::operator::Operator;
use crate::sql::part::Part;
use crate::sql::statement::Statement;
use crate::sql::statements::live::LiveStatement;
use crate::sql::Id;
use crate::sql::Table;
use crate::sql::Thing;
use crate::sql::Uuid;
use crate::sql::Value;
use crate::Notification;
use crate::Surreal;
use channel::Receiver;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::ops::Bound;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

const ID: &str = "id";

/// A live query future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Live<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Live<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Live<'static, C, R> {
		Live {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	() => {
		fn into_future(self) -> Self::IntoFuture {
			let Live {
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
				let mut stmt = LiveStatement {
					id: Uuid::new_v4(),
					node: Uuid::new_v4(),
					expr: Fields(vec![Field::All], false),
					..Default::default()
				};
				match range {
					Some(range) => {
						let range = resource?.with_range(range)?;
						stmt.what = Table(range.tb.clone()).into();
						stmt.cond = cond_from_range(range);
					}
					None => match resource? {
						Resource::Table(table) => {
							stmt.what = table.into();
						}
						Resource::RecordId(record) => {
							stmt.what = Table(record.tb.clone()).into();
							stmt.cond = Some(Cond(Value::Expression(Box::new(Expression::new(
								Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
								Operator::Equal,
								record.into(),
							)))));
						}
						Resource::Object(object) => return Err(Error::LiveOnObject(object).into()),
						Resource::Array(array) => return Err(Error::LiveOnArray(array).into()),
						Resource::Edges(edges) => return Err(Error::LiveOnEdges(edges).into()),
					},
				}
				let query = Query {
					client: client.clone(),
					query: vec![Ok(vec![Statement::Live(stmt)])],
					bindings: Ok(Default::default()),
				};
				let id: Value = query.await?.take(0)?;
				let mut conn = Client::new(Method::Live);
				let (tx, rx) = channel::unbounded();
				let mut param = Param::notification_sender(tx);
				param.other = vec![id.clone()];
				conn.execute_unit(router, param).await?;
				Ok(Stream {
					id,
					rx,
					client,
					response_type: PhantomData,
				})
			})
		}
	};
}

fn cond_from_range(range: crate::sql::Range) -> Option<Cond> {
	match (range.beg, range.end) {
		(Bound::Unbounded, Bound::Unbounded) => None,
		(Bound::Unbounded, Bound::Excluded(id)) => {
			Some(Cond(Value::Expression(Box::new(Expression::new(
				Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
				Operator::LessThan,
				Thing::from((range.tb, id)).into(),
			)))))
		}
		(Bound::Unbounded, Bound::Included(id)) => {
			Some(Cond(Value::Expression(Box::new(Expression::new(
				Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
				Operator::LessThanOrEqual,
				Thing::from((range.tb, id)).into(),
			)))))
		}
		(Bound::Excluded(id), Bound::Unbounded) => {
			Some(Cond(Value::Expression(Box::new(Expression::new(
				Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
				Operator::MoreThan,
				Thing::from((range.tb, id)).into(),
			)))))
		}
		(Bound::Included(id), Bound::Unbounded) => {
			Some(Cond(Value::Expression(Box::new(Expression::new(
				Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
				Operator::MoreThanOrEqual,
				Thing::from((range.tb, id)).into(),
			)))))
		}
		(Bound::Included(lid), Bound::Included(rid)) => {
			Some(Cond(Value::Expression(Box::new(Expression::new(
				Value::Expression(Box::new(Expression::new(
					Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
					Operator::MoreThanOrEqual,
					Thing::from((range.tb.clone(), lid)).into(),
				))),
				Operator::And,
				Value::Expression(Box::new(Expression::new(
					Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
					Operator::LessThanOrEqual,
					Thing::from((range.tb, rid)).into(),
				))),
			)))))
		}
		(Bound::Included(lid), Bound::Excluded(rid)) => {
			Some(Cond(Value::Expression(Box::new(Expression::new(
				Value::Expression(Box::new(Expression::new(
					Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
					Operator::MoreThanOrEqual,
					Thing::from((range.tb.clone(), lid)).into(),
				))),
				Operator::And,
				Value::Expression(Box::new(Expression::new(
					Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
					Operator::LessThan,
					Thing::from((range.tb, rid)).into(),
				))),
			)))))
		}
		(Bound::Excluded(lid), Bound::Included(rid)) => {
			Some(Cond(Value::Expression(Box::new(Expression::new(
				Value::Expression(Box::new(Expression::new(
					Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
					Operator::MoreThan,
					Thing::from((range.tb.clone(), lid)).into(),
				))),
				Operator::And,
				Value::Expression(Box::new(Expression::new(
					Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
					Operator::LessThanOrEqual,
					Thing::from((range.tb, rid)).into(),
				))),
			)))))
		}
		(Bound::Excluded(lid), Bound::Excluded(rid)) => {
			Some(Cond(Value::Expression(Box::new(Expression::new(
				Value::Expression(Box::new(Expression::new(
					Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
					Operator::MoreThan,
					Thing::from((range.tb.clone(), lid)).into(),
				))),
				Operator::And,
				Value::Expression(Box::new(Expression::new(
					Idiom(vec![Part::from(Ident(ID.to_owned()))]).into(),
					Operator::LessThan,
					Thing::from((range.tb, rid)).into(),
				))),
			)))))
		}
	}
}

impl<'r, Client> IntoFuture for Live<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Stream<'r, Client, Value>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {}
}

impl<'r, Client, R> IntoFuture for Live<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Stream<'r, Client, Option<R>>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {}
}

impl<'r, Client, R> IntoFuture for Live<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Stream<'r, Client, Vec<R>>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	into_future! {}
}

/// A stream of live query notifications
#[derive(Debug)]
#[must_use = "streams do nothing unless you poll them"]
pub struct Stream<'r, C: Connection, R> {
	client: Cow<'r, Surreal<C>>,
	id: Value,
	rx: Receiver<dbs::Notification>,
	response_type: PhantomData<R>,
}

impl<Client, R> Stream<'_, Client, R>
where
	Client: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Stream<'static, Client, R> {
		Stream {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}

	/// Close the live query stream
	///
	/// This kills the live query process responsible for this stream.
	/// If the stream is dropped without calling this method, the process
	/// will be killed next time it tries to send a notification to the stream.
	pub async fn close(self) -> Result<()> {
		let router = self.client.router.extract()?;
		let mut conn = Client::new(Method::Kill);
		conn.execute_unit(router, Param::new(vec![self.id])).await
	}
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
		action, result => Poll::Ready(Some(Notification { action: action.into(), data: result }))
	}
}

macro_rules! poll_next_and_convert {
	() => {
		poll_next! {
			action, result => match from_value(result) {
				Ok(data) => Poll::Ready(Some(Ok(Notification { action: action.into(), data }))),
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
