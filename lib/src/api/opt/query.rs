use crate::api::{err::Error, opt::from_value, Response as QueryResponse, Result};
use crate::method::query::{InnerStream, QueryStreamFuture};
use crate::method::{self, live};
use crate::method::{Stats, Stream};
use crate::sql::{self, statements::*, Array, Object, Statement, Statements, Value};
use crate::{syn, Notification};
use futures::future::Either;
use futures::stream::{select_all, FuturesUnordered};
use futures::Future;
use futures::TryStreamExt;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;

/// A trait for converting inputs into SQL statements
pub trait IntoQuery {
	/// Converts an input into SQL statements
	fn into_query(self) -> Result<Vec<Statement>>;
}

impl IntoQuery for sql::Query {
	fn into_query(self) -> Result<Vec<Statement>> {
		let sql::Query(Statements(statements)) = self;
		Ok(statements)
	}
}

impl IntoQuery for Statements {
	fn into_query(self) -> Result<Vec<Statement>> {
		let Statements(statements) = self;
		Ok(statements)
	}
}

impl IntoQuery for Vec<Statement> {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(self)
	}
}

impl IntoQuery for Statement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![self])
	}
}

impl IntoQuery for UseStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Use(self)])
	}
}

impl IntoQuery for SetStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Set(self)])
	}
}

impl IntoQuery for InfoStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Info(self)])
	}
}

impl IntoQuery for LiveStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Live(self)])
	}
}

impl IntoQuery for KillStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Kill(self)])
	}
}

impl IntoQuery for BeginStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Begin(self)])
	}
}

impl IntoQuery for CancelStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Cancel(self)])
	}
}

impl IntoQuery for CommitStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Commit(self)])
	}
}

impl IntoQuery for OutputStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Output(self)])
	}
}

impl IntoQuery for IfelseStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Ifelse(self)])
	}
}

impl IntoQuery for SelectStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Select(self)])
	}
}

impl IntoQuery for CreateStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Create(self)])
	}
}

impl IntoQuery for UpdateStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Update(self)])
	}
}

impl IntoQuery for RelateStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Relate(self)])
	}
}

impl IntoQuery for DeleteStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Delete(self)])
	}
}

impl IntoQuery for InsertStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Insert(self)])
	}
}

impl IntoQuery for DefineStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Define(self)])
	}
}

impl IntoQuery for RemoveStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Remove(self)])
	}
}

impl IntoQuery for OptionStatement {
	fn into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Option(self)])
	}
}

impl IntoQuery for &str {
	fn into_query(self) -> Result<Vec<Statement>> {
		syn::parse(self)?.into_query()
	}
}

impl IntoQuery for &String {
	fn into_query(self) -> Result<Vec<Statement>> {
		syn::parse(self)?.into_query()
	}
}

impl IntoQuery for String {
	fn into_query(self) -> Result<Vec<Statement>> {
		syn::parse(&self)?.into_query()
	}
}

/// Represents a way to take a single query result from a list of responses
pub trait QueryResult<Response>
where
	Response: DeserializeOwned,
{
	/// Extracts and deserializes a query result from a query response
	fn query_result(self, response: &mut QueryResponse) -> Result<Response>;

	/// Extracts the statistics from a query response
	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(&0).map(|x| x.0)
	}
}

impl QueryResult<Value> for usize {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		match response.results.remove(&self) {
			Some((_, result)) => Ok(result?),
			None => Ok(Value::None),
		}
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(self).map(|x| x.0)
	}
}

impl<T> QueryResult<Option<T>> for usize
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		let value = match response.results.get_mut(&self) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					response.results.remove(&self);
					return Err(error);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let result = match value {
			Value::Array(Array(vec)) => match &mut vec[..] {
				[] => Ok(None),
				[value] => {
					let value = mem::take(value);
					from_value(value).map_err(Into::into)
				}
				_ => Err(Error::LossyTake(QueryResponse {
					results: mem::take(&mut response.results),
					live_queries: mem::take(&mut response.live_queries),
					..QueryResponse::new()
				})
				.into()),
			},
			_ => {
				let value = mem::take(value);
				from_value(value).map_err(Into::into)
			}
		};
		response.results.remove(&self);
		result
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(self).map(|x| x.0)
	}
}

impl QueryResult<Value> for (usize, &str) {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		let (index, key) = self;
		let value = match response.results.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					response.results.remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(Value::None);
			}
		};

		let value = match value {
			Value::Object(Object(object)) => object.remove(key).unwrap_or_default(),
			_ => Value::None,
		};

		Ok(value)
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl<T> QueryResult<Option<T>> for (usize, &str)
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		let (index, key) = self;
		let value = match response.results.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					response.results.remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let value = match value {
			Value::Array(Array(vec)) => match &mut vec[..] {
				[] => {
					response.results.remove(&index);
					return Ok(None);
				}
				[value] => value,
				_ => {
					return Err(Error::LossyTake(QueryResponse {
						results: mem::take(&mut response.results),
						live_queries: mem::take(&mut response.live_queries),
						..QueryResponse::new()
					})
					.into());
				}
			},
			value => value,
		};
		match value {
			Value::None | Value::Null => {
				response.results.remove(&index);
				Ok(None)
			}
			Value::Object(Object(object)) => {
				if object.is_empty() {
					response.results.remove(&index);
					return Ok(None);
				}
				let Some(value) = object.remove(key) else {
					return Ok(None);
				};
				from_value(value).map_err(Into::into)
			}
			_ => Ok(None),
		}
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl<T> QueryResult<Vec<T>> for usize
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		let vec = match response.results.remove(&self) {
			Some((_, result)) => match result? {
				Value::Array(Array(vec)) => vec,
				vec => vec![vec],
			},
			None => {
				return Ok(vec![]);
			}
		};
		from_value(vec.into()).map_err(Into::into)
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(self).map(|x| x.0)
	}
}

impl<T> QueryResult<Vec<T>> for (usize, &str)
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		let (index, key) = self;
		let mut response = match response.results.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => match val {
					Value::Array(Array(vec)) => mem::take(vec),
					val => {
						let val = mem::take(val);
						vec![val]
					}
				},
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					response.results.remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(vec![]);
			}
		};
		let mut vec = Vec::with_capacity(response.len());
		for value in response.iter_mut() {
			if let Value::Object(Object(object)) = value {
				if let Some(value) = object.remove(key) {
					vec.push(value);
				}
			}
		}
		from_value(vec.into()).map_err(Into::into)
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl QueryResult<Value> for &str {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		(0, self).query_result(response)
	}
}

impl<T> QueryResult<Option<T>> for &str
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		(0, self).query_result(response)
	}
}

impl<T> QueryResult<Vec<T>> for &str
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		(0, self).query_result(response)
	}
}

/// A way to take a query stream future from a query response
pub trait QueryStream<R> {
	/// Retrieves the query stream future
	fn query_stream(self, response: &mut QueryResponse) -> QueryStreamFuture<R>;
}

impl QueryStream<Value> for usize {
	fn query_stream(self, response: &mut QueryResponse) -> QueryStreamFuture<Value> {
		let statement = response.live_queries.remove(&self).unwrap_or_else(|| {
			match response.results.contains_key(&self) {
				true => Err(Error::NotLiveQuery(self).into()),
				false => Err(Error::QueryIndexOutOfBounds(self).into()),
			}
		});
		let stream = InnerStream::One(Stream {
			statement: Some(statement),
			id: Value::None,
			rx: None,
			client: response.client.clone(),
			response_type: PhantomData,
			engine: PhantomData,
			stream_type: PhantomData,
		});
		QueryStreamFuture(stream)
	}
}

impl QueryStream<Value> for () {
	fn query_stream(self, response: &mut QueryResponse) -> QueryStreamFuture<Value> {
		let mut streams = Vec::with_capacity(response.live_queries.len());
		for (_, statement) in mem::take(&mut response.live_queries) {
			streams.push(Stream {
				statement: Some(statement),
				id: Value::None,
				rx: None,
				client: response.client.clone(),
				response_type: PhantomData,
				engine: PhantomData,
				stream_type: PhantomData,
			});
		}
		QueryStreamFuture(InnerStream::Many(streams))
	}
}

impl<R> QueryStream<Notification<R>> for usize
where
	R: DeserializeOwned + Unpin,
{
	fn query_stream(self, response: &mut QueryResponse) -> QueryStreamFuture<Notification<R>> {
		let statement = response.live_queries.remove(&self).unwrap_or_else(|| {
			match response.results.contains_key(&self) {
				true => Err(Error::NotLiveQuery(self).into()),
				false => Err(Error::QueryIndexOutOfBounds(self).into()),
			}
		});
		let stream = InnerStream::One(Stream {
			statement: Some(statement),
			id: Value::None,
			rx: None,
			client: response.client.clone(),
			response_type: PhantomData,
			engine: PhantomData,
			stream_type: PhantomData,
		});
		QueryStreamFuture(stream)
	}
}

impl<R> QueryStream<Notification<R>> for ()
where
	R: DeserializeOwned + Unpin,
{
	fn query_stream(self, response: &mut QueryResponse) -> QueryStreamFuture<Notification<R>> {
		let mut streams = Vec::with_capacity(response.live_queries.len());
		for (_, statement) in mem::take(&mut response.live_queries) {
			streams.push(Stream {
				statement: Some(statement),
				id: Value::None,
				rx: None,
				client: response.client.clone(),
				response_type: PhantomData,
				engine: PhantomData,
				stream_type: PhantomData,
			});
		}
		QueryStreamFuture(InnerStream::Many(streams))
	}
}

macro_rules! into_future {
	() => {
		fn into_future(mut self) -> Self::IntoFuture {
			match &mut self.0 {
				InnerStream::One(stream) => {
					let statement =
						mem::take(&mut stream.statement).unwrap_or_else(|| unreachable!());
					let client = stream.client.clone();
					Box::pin(async move {
						let (id, rx) = live::query(&Cow::Borrowed(&client), statement?).await?;
						Ok(method::QueryStream(Either::Left(Stream {
							id,
							rx: Some(rx),
							client,
							statement: None,
							response_type: PhantomData,
							engine: PhantomData,
							stream_type: PhantomData,
						})))
					})
				}
				InnerStream::Many(vec) => {
					let mut vec = mem::take(vec);
					Box::pin(async move {
						let mut streams = Vec::with_capacity(vec.len());
						for stream in &mut vec {
							let statement = mem::take(&mut stream.statement)
								.unwrap_or_else(|| unreachable!())?;
							let client = stream.client.clone();
							streams.push(async move {
								match live::query(&Cow::Borrowed(&client), statement).await {
									Ok((id, rx)) => Ok(Stream {
										id,
										rx: Some(rx),
										client,
										statement: None,
										response_type: PhantomData,
										engine: PhantomData,
										stream_type: PhantomData,
									}),
									Err(error) => Err(error),
								}
							});
						}
						let streams: Vec<_> =
							FuturesUnordered::from_iter(streams).try_collect().await?;
						Ok(method::QueryStream(Either::Right(select_all(streams))))
					})
				}
			}
		}
	};
}

impl IntoFuture for QueryStreamFuture<Value> {
	type Output = Result<method::QueryStream<Value>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync>>;

	into_future! {}
}

impl<R> IntoFuture for QueryStreamFuture<Notification<R>>
where
	R: DeserializeOwned + Unpin + Send + Sync + 'static,
{
	type Output = Result<method::QueryStream<Notification<R>>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync>>;

	into_future! {}
}
