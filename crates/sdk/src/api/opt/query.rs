use super::Raw;
use crate::{
	api::{err::Error, Response as QueryResponse, Result},
	method::{self, query::ValidQuery, Stats, Stream},
	value::Notification,
	Value,
};
use anyhow::bail;
use futures::future::Either;
use futures::stream::select_all;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::mem;
use surrealdb_core::sql::{
	self, from_value as from_core_value, statements::*, Statement, Statements, Value as CoreValue,
};

pub struct Query(pub(crate) ValidQuery);
/// A trait for converting inputs into SQL statements
pub trait IntoQuery: into_query::Sealed {}

pub(crate) mod into_query {
	pub trait Sealed {
		/// Converts an input into SQL statements
		fn into_query(self) -> super::Query;

		/// Not public API
		#[doc(hidden)]
		fn as_str(&self) -> Option<&str> {
			None
		}
	}
}

impl IntoQuery for sql::Query {}
impl into_query::Sealed for sql::Query {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: self.0 .0,
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for Statements {}
impl into_query::Sealed for Statements {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: self.0,
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for Vec<Statement> {}
impl into_query::Sealed for Vec<Statement> {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: self,
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for Statement {}
impl into_query::Sealed for Statement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![self],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for UseStatement {}
impl into_query::Sealed for UseStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Use(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for SetStatement {}
impl into_query::Sealed for SetStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Set(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for InfoStatement {}
impl into_query::Sealed for InfoStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Info(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for LiveStatement {}
impl into_query::Sealed for LiveStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Live(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for KillStatement {}
impl into_query::Sealed for KillStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Kill(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for BeginStatement {}
impl into_query::Sealed for BeginStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Begin(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for CancelStatement {}
impl into_query::Sealed for CancelStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Cancel(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for CommitStatement {}
impl into_query::Sealed for CommitStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Commit(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for OutputStatement {}
impl into_query::Sealed for OutputStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Output(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for IfelseStatement {}
impl into_query::Sealed for IfelseStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Ifelse(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for SelectStatement {}
impl into_query::Sealed for SelectStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Select(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for CreateStatement {}
impl into_query::Sealed for CreateStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Create(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for UpdateStatement {}
impl into_query::Sealed for UpdateStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Update(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for RelateStatement {}
impl into_query::Sealed for RelateStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Relate(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for DeleteStatement {}
impl into_query::Sealed for DeleteStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Delete(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for InsertStatement {}
impl into_query::Sealed for InsertStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Insert(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for DefineStatement {}
impl into_query::Sealed for DefineStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Define(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for AlterStatement {}
impl into_query::Sealed for AlterStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Alter(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for RemoveStatement {}
impl into_query::Sealed for RemoveStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Remove(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for OptionStatement {}
impl into_query::Sealed for OptionStatement {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: vec![Statement::Option(self)],
			register_live_queries: true,
			bindings: Default::default(),
		})
	}
}

impl IntoQuery for &str {}
impl into_query::Sealed for &str {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: Vec::new(),
			register_live_queries: true,
			bindings: Default::default(),
		})
	}

	fn as_str(&self) -> Option<&str> {
		Some(self)
	}
}

impl IntoQuery for &String {}
impl into_query::Sealed for &String {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: Vec::new(),
			register_live_queries: true,
			bindings: Default::default(),
		})
	}

	fn as_str(&self) -> Option<&str> {
		Some(self)
	}
}

impl IntoQuery for String {}
impl into_query::Sealed for String {
	fn into_query(self) -> Query {
		Query(ValidQuery::Normal {
			query: Vec::new(),
			register_live_queries: true,
			bindings: Default::default(),
		})
	}

	fn as_str(&self) -> Option<&str> {
		Some(self)
	}
}

impl IntoQuery for Raw {}
impl into_query::Sealed for Raw {
	fn into_query(self) -> Query {
		Query(ValidQuery::Raw {
			query: self.0,
			bindings: Default::default(),
		})
	}
}

/// Represents a way to take a single query result from a list of responses
pub trait QueryResult<Response>: query_result::Sealed<Response>
where
	Response: DeserializeOwned,
{
}

mod query_result {
	pub trait Sealed<Response>
	where
		Response: super::DeserializeOwned,
	{
		/// Extracts and deserializes a query result from a query response
		fn query_result(self, response: &mut super::QueryResponse) -> super::Result<Response>;

		/// Extracts the statistics from a query response
		fn stats(&self, response: &super::QueryResponse) -> Option<super::Stats> {
			response.results.get(&0).map(|x| x.0)
		}
	}
}

impl QueryResult<Value> for usize {}
impl query_result::Sealed<Value> for usize {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		match response.results.swap_remove(&self) {
			Some((_, result)) => Ok(Value::from_inner(result?)),
			None => Ok(Value::from_inner(CoreValue::None)),
		}
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(self).map(|x| x.0)
	}
}

impl<T> QueryResult<Option<T>> for usize where T: DeserializeOwned {}
impl<T> query_result::Sealed<Option<T>> for usize
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		let value = match response.results.get_mut(&self) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					response.results.swap_remove(&self);
					return Err(error);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let result = match value {
			CoreValue::Array(vec) => match &mut vec.0[..] {
				[] => Ok(None),
				[value] => {
					let value = mem::take(value);
					from_core_value(value)
				}
				_ => Err(Error::LossyTake(QueryResponse {
					results: mem::take(&mut response.results),
					live_queries: mem::take(&mut response.live_queries),
				})
				.into()),
			},
			_ => {
				let value = mem::take(value);
				from_core_value(value)
			}
		};
		response.results.swap_remove(&self);
		result
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(self).map(|x| x.0)
	}
}

impl QueryResult<Value> for (usize, &str) {}
impl query_result::Sealed<Value> for (usize, &str) {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		let (index, key) = self;
		let value = match response.results.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					response.results.swap_remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(Value::from_inner(CoreValue::None));
			}
		};

		let value = match value {
			CoreValue::Object(object) => object.remove(key).unwrap_or_default(),
			_ => CoreValue::None,
		};

		Ok(Value::from_inner(value))
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl<T> QueryResult<Option<T>> for (usize, &str) where T: DeserializeOwned {}
impl<T> query_result::Sealed<Option<T>> for (usize, &str)
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
					response.results.swap_remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let value = match value {
			CoreValue::Array(vec) => match &mut vec.0[..] {
				[] => {
					response.results.swap_remove(&index);
					return Ok(None);
				}
				[value] => value,
				_ => {
					return Err(Error::LossyTake(QueryResponse {
						results: mem::take(&mut response.results),
						live_queries: mem::take(&mut response.live_queries),
					})
					.into());
				}
			},
			value => value,
		};
		match value {
			CoreValue::None => {
				response.results.swap_remove(&index);
				Ok(None)
			}
			CoreValue::Object(object) => {
				if object.is_empty() {
					response.results.swap_remove(&index);
					return Ok(None);
				}
				let Some(value) = object.remove(key) else {
					return Ok(None);
				};
				from_core_value(value)
			}
			_ => Ok(None),
		}
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl<T> QueryResult<Vec<T>> for usize where T: DeserializeOwned {}
impl<T> query_result::Sealed<Vec<T>> for usize
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		let vec = match response.results.swap_remove(&self) {
			Some((_, result)) => match result? {
				CoreValue::Array(vec) => vec.0,
				vec => vec![vec],
			},
			None => {
				return Ok(vec![]);
			}
		};
		from_core_value(vec.into())
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(self).map(|x| x.0)
	}
}

impl<T> QueryResult<Vec<T>> for (usize, &str) where T: DeserializeOwned {}
impl<T> query_result::Sealed<Vec<T>> for (usize, &str)
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		let (index, key) = self;
		match response.results.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => match val {
					CoreValue::Array(vec) => {
						let mut responses = Vec::with_capacity(vec.len());
						for value in vec.iter_mut() {
							if let CoreValue::Object(object) = value {
								if let Some(value) = object.remove(key) {
									responses.push(value);
								}
							}
						}
						from_core_value(responses.into())
					}
					val => {
						if let CoreValue::Object(object) = val {
							if let Some(value) = object.remove(key) {
								return from_core_value(vec![value].into());
							}
						}
						Ok(vec![])
					}
				},
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					response.results.swap_remove(&index);
					Err(error)
				}
			},
			None => Ok(vec![]),
		}
	}

	fn stats(&self, response: &QueryResponse) -> Option<Stats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl QueryResult<Value> for &str {}
impl query_result::Sealed<Value> for &str {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		(0, self).query_result(response)
	}
}

impl<T> QueryResult<Option<T>> for &str where T: DeserializeOwned {}
impl<T> query_result::Sealed<Option<T>> for &str
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		(0, self).query_result(response)
	}
}

impl<T> QueryResult<Vec<T>> for &str where T: DeserializeOwned {}
impl<T> query_result::Sealed<Vec<T>> for &str
where
	T: DeserializeOwned,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		(0, self).query_result(response)
	}
}

/// A way to take a query stream future from a query response
pub trait QueryStream<R>: query_stream::Sealed<R> {}

mod query_stream {
	pub trait Sealed<R> {
		/// Retrieves the query stream future
		fn query_stream(
			self,
			response: &mut super::QueryResponse,
		) -> super::Result<super::method::QueryStream<R>>;
	}
}

impl QueryStream<Value> for usize {}
impl query_stream::Sealed<Value> for usize {
	fn query_stream(self, response: &mut QueryResponse) -> Result<method::QueryStream<Value>> {
		let stream = response
			.live_queries
			.swap_remove(&self)
			.and_then(|result| match result {
				Err(e) => {
					if matches!(e.downcast_ref(), Some(Error::NotLiveQuery(..))) {
						response.results.swap_remove(&self).and_then(|x| x.1.err().map(Err))
					} else {
						Some(Err(e))
					}
				}
				result => Some(result),
			})
			.unwrap_or_else(|| match response.results.contains_key(&self) {
				true => Err(Error::NotLiveQuery(self).into()),
				false => Err(Error::QueryIndexOutOfBounds(self).into()),
			})?;
		Ok(method::QueryStream(Either::Left(stream)))
	}
}

impl QueryStream<Value> for () {}
impl query_stream::Sealed<Value> for () {
	fn query_stream(self, response: &mut QueryResponse) -> Result<method::QueryStream<Value>> {
		let mut streams = Vec::with_capacity(response.live_queries.len());
		for (index, result) in mem::take(&mut response.live_queries) {
			match result {
				Ok(stream) => streams.push(stream),
				Err(e) => {
					if matches!(e.downcast_ref(), Some(Error::NotLiveQuery(..))) {
						match response.results.swap_remove(&index) {
							Some((stats, Err(error))) => {
								response.results.insert(index, (stats, Err(Error::ResponseAlreadyTaken.into())));
								return Err(error);
							}
							Some((_, Ok(..))) => unreachable!("the internal error variant indicates that an error occurred in the `LIVE SELECT` query"),
							None => { bail!(Error::ResponseAlreadyTaken); }
						}
					} else {
						return Err(e);
					}
				}
			}
		}
		Ok(method::QueryStream(Either::Right(select_all(streams))))
	}
}

impl<R> QueryStream<Notification<R>> for usize where R: DeserializeOwned + Unpin {}
impl<R> query_stream::Sealed<Notification<R>> for usize
where
	R: DeserializeOwned + Unpin,
{
	fn query_stream(
		self,
		response: &mut QueryResponse,
	) -> Result<method::QueryStream<Notification<R>>> {
		let mut stream = response
			.live_queries
			.swap_remove(&self)
			.and_then(|result| match result {
				Err(e) => {
					if matches!(e.downcast_ref(), Some(Error::NotLiveQuery(..))) {
						response.results.swap_remove(&self).and_then(|x| x.1.err().map(Err))
					} else {
						Some(Err(e))
					}
				}
				result => Some(result),
			})
			.unwrap_or_else(|| match response.results.contains_key(&self) {
				true => Err(Error::NotLiveQuery(self).into()),
				false => Err(Error::QueryIndexOutOfBounds(self).into()),
			})?;
		Ok(method::QueryStream(Either::Left(Stream {
			client: stream.client.clone(),
			id: mem::take(&mut stream.id),
			rx: stream.rx.take(),
			response_type: PhantomData,
		})))
	}
}

impl<R> QueryStream<Notification<R>> for () where R: DeserializeOwned + Unpin {}
impl<R> query_stream::Sealed<Notification<R>> for ()
where
	R: DeserializeOwned + Unpin,
{
	fn query_stream(
		self,
		response: &mut QueryResponse,
	) -> Result<method::QueryStream<Notification<R>>> {
		let mut streams = Vec::with_capacity(response.live_queries.len());
		for (index, result) in mem::take(&mut response.live_queries) {
			let mut stream = match result {
				Ok(stream) => stream,
				Err(e) => {
					if matches!(e.downcast_ref(), Some(Error::NotLiveQuery(..))) {
						match response.results.swap_remove(&index) {
							Some((stats, Err(error))) => {
								response.results.insert(index, (stats, Err(Error::ResponseAlreadyTaken.into())));
								return Err(error);
							}
							Some((_, Ok(..))) => unreachable!("the internal error variant indicates that an error occurred in the `LIVE SELECT` query"),
							None => { bail!(Error::ResponseAlreadyTaken); }
						}
					} else {
						return Err(e);
					}
				}
			};
			streams.push(Stream {
				client: stream.client.clone(),
				id: mem::take(&mut stream.id),
				rx: stream.rx.take(),
				response_type: PhantomData,
			});
		}
		Ok(method::QueryStream(Either::Right(select_all(streams))))
	}
}
