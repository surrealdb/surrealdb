use std::marker::PhantomData;
use std::mem;

// Removed anyhow::bail - using return Err() instead
use futures::future::Either;
use futures::stream::select_all;
use surrealdb_core::rpc::DbResultStats;

use crate::err::Error;
use crate::method::live::Stream;
use crate::notification::Notification;
use crate::types::{SurrealValue, Value};
use crate::{IndexedResults as QueryResponse, Result};

/// Represents a way to take a single query result from a list of responses
pub trait QueryResult<Response>: query_result::Sealed<Response>
where
	Response: SurrealValue,
{
}

mod query_result {
	use surrealdb_core::rpc::DbResultStats;

	pub trait Sealed<Response>
	where
		Response: super::SurrealValue,
	{
		/// Extracts and deserializes a query result from a query response
		fn query_result(self, response: &mut super::QueryResponse) -> super::Result<Response>;

		/// Extracts the statistics from a query response
		fn stats(&self, response: &super::QueryResponse) -> Option<DbResultStats> {
			response.results.get(&0).map(|x| x.0)
		}
	}
}

impl QueryResult<Value> for usize {}
impl query_result::Sealed<Value> for usize {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		match response.results.swap_remove(&self) {
			Some((_, result)) => Ok(result?),
			None => Ok(Value::None),
		}
	}

	fn stats(&self, response: &QueryResponse) -> Option<DbResultStats> {
		response.results.get(self).map(|x| x.0)
	}
}

impl<T> QueryResult<Option<T>> for usize where T: SurrealValue {}
impl<T> query_result::Sealed<Option<T>> for usize
where
	T: SurrealValue,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		let value = match response.results.get_mut(&self) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(_) => {
					response.results.swap_remove(&self);
					return Err(Error::ConnectionUninitialised);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let result = match value {
			Value::Array(vec) => match &mut vec[..] {
				[] => Ok(None),
				[value] => {
					let value = mem::take(value);
					match value {
						Value::None => Ok(None),
						v => Ok(Some(T::from_value(v)?)),
					}
				}
				_ => Err(Error::LossyTake(Box::new(QueryResponse {
					results: mem::take(&mut response.results),
					live_queries: mem::take(&mut response.live_queries),
				}))),
			},
			value => {
				let value = mem::take(value);
				match value {
					Value::None => Ok(None),
					v => Ok(Some(T::from_value(v)?)),
				}
			}
		};
		response.results.swap_remove(&self);
		result
	}

	fn stats(&self, response: &QueryResponse) -> Option<DbResultStats> {
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
				Err(_) => {
					response.results.swap_remove(&index);
					return Err(Error::ConnectionUninitialised);
				}
			},
			None => {
				return Ok(Value::None);
			}
		};

		let value = match value {
			Value::Object(object) => object.remove(key).unwrap_or_default(),
			_ => Value::None,
		};

		Ok(value)
	}

	fn stats(&self, response: &QueryResponse) -> Option<DbResultStats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl<T> QueryResult<Option<T>> for (usize, &str) where T: SurrealValue {}
impl<T> query_result::Sealed<Option<T>> for (usize, &str)
where
	T: SurrealValue,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		let (index, key) = self;
		let value: &mut Value = match response.results.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => val,
				Err(_) => {
					response.results.swap_remove(&index);
					return Err(Error::ConnectionUninitialised);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let value = match value {
			Value::Array(vec) => match &mut vec[..] {
				[] => {
					response.results.swap_remove(&index);
					return Ok(None);
				}
				[value] => value,
				_ => {
					return Err(Error::LossyTake(Box::new(QueryResponse {
						results: mem::take(&mut response.results),
						live_queries: mem::take(&mut response.live_queries),
					})));
				}
			},
			value => value,
		};
		match value {
			Value::None => {
				response.results.swap_remove(&index);
				Ok(None)
			}
			Value::Object(object) => {
				if object.is_empty() {
					response.results.swap_remove(&index);
					return Ok(None);
				}
				let Some(value) = object.remove(key) else {
					return Ok(None);
				};
				Ok(Some(T::from_value(value)?))
			}
			_ => Ok(None),
		}
	}

	fn stats(&self, response: &QueryResponse) -> Option<DbResultStats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl<T> QueryResult<Vec<T>> for usize where T: SurrealValue {}
impl<T> query_result::Sealed<Vec<T>> for usize
where
	T: SurrealValue,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		let vec = match response.results.swap_remove(&self) {
			Some((_, result)) => match result? {
				Value::Array(arr) => arr.into_vec(),
				vec => vec![vec],
			},
			None => {
				return Ok(vec![]);
			}
		};

		vec.into_iter().map(|v| T::from_value(v).map_err(Into::into)).collect::<Result<Vec<T>>>()
	}

	fn stats(&self, response: &QueryResponse) -> Option<DbResultStats> {
		response.results.get(self).map(|x| x.0)
	}
}

impl<T> QueryResult<Vec<T>> for (usize, &str) where T: SurrealValue {}
impl<T> query_result::Sealed<Vec<T>> for (usize, &str)
where
	T: SurrealValue,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Vec<T>> {
		let (index, key) = self;
		match response.results.get_mut(&index) {
			Some((_, result)) => match result {
				Ok(val) => match val {
					Value::Array(vec) => {
						let mut responses = Vec::with_capacity(vec.len());
						for value in vec.iter_mut() {
							if let Value::Object(object) = value
								&& let Some(value) = object.remove(key)
							{
								responses.push(value);
							}
						}
						responses
							.into_iter()
							.map(|v| T::from_value(v).map_err(Into::into))
							.collect::<Result<Vec<T>>>()
					}
					val => {
						if let Value::Object(object) = val
							&& let Some(value) = object.remove(key)
						{
							return Ok(vec![T::from_value(value)?]);
						}
						Ok(vec![])
					}
				},
				Err(_) => {
					response.results.swap_remove(&index);
					Err(Error::ConnectionUninitialised)
				}
			},
			None => Ok(vec![]),
		}
	}

	fn stats(&self, response: &QueryResponse) -> Option<DbResultStats> {
		response.results.get(&self.0).map(|x| x.0)
	}
}

impl QueryResult<Value> for &str {}
impl query_result::Sealed<Value> for &str {
	fn query_result(self, response: &mut QueryResponse) -> Result<Value> {
		(0, self).query_result(response)
	}
}

impl<T> QueryResult<Option<T>> for &str where T: SurrealValue {}
impl<T> query_result::Sealed<Option<T>> for &str
where
	T: SurrealValue,
{
	fn query_result(self, response: &mut QueryResponse) -> Result<Option<T>> {
		(0, self).query_result(response)
	}
}

impl<T> QueryResult<Vec<T>> for &str where T: SurrealValue {}
impl<T> query_result::Sealed<Vec<T>> for &str
where
	T: SurrealValue,
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
		) -> super::Result<crate::method::QueryStream<R>>;
	}
}

impl QueryStream<Value> for usize {}
impl query_stream::Sealed<Value> for usize {
	fn query_stream(
		self,
		response: &mut QueryResponse,
	) -> Result<crate::method::QueryStream<Value>> {
		let stream = response
			.live_queries
			.swap_remove(&self)
			.and_then(|result| match result {
				Err(e) => {
					if matches!(e, Error::NotLiveQuery(..)) {
						response.results.swap_remove(&self);
						None
					} else {
						Some(Err(e))
					}
				}
				result => Some(result),
			})
			.unwrap_or_else(|| match response.results.contains_key(&self) {
				true => Err(Error::NotLiveQuery(self)),
				false => Err(Error::QueryIndexOutOfBounds(self)),
			})?;
		Ok(crate::method::QueryStream(Either::Left(stream)))
	}
}

impl QueryStream<Value> for () {}
impl query_stream::Sealed<Value> for () {
	fn query_stream(
		self,
		response: &mut QueryResponse,
	) -> Result<crate::method::QueryStream<Value>> {
		let mut streams = Vec::with_capacity(response.live_queries.len());
		for (index, result) in mem::take(&mut response.live_queries) {
			match result {
				Ok(stream) => streams.push(stream),
				Err(e) => {
					if matches!(e, Error::NotLiveQuery(..)) {
						match response.results.swap_remove(&index) {
							Some((_, Err(_))) => {
								return Err(Error::ConnectionUninitialised);
							}
							Some((_, Ok(..))) => unreachable!(
								"the internal error variant indicates that an error occurred in the `LIVE SELECT` query"
							),
							None => {
								return Err(Error::ResponseAlreadyTaken);
							}
						}
					} else {
						return Err(e);
					}
				}
			}
		}
		Ok(crate::method::QueryStream(Either::Right(select_all(streams))))
	}
}

impl<R> QueryStream<Notification<R>> for usize where R: SurrealValue + Unpin {}
impl<R> query_stream::Sealed<Notification<R>> for usize
where
	R: SurrealValue + Unpin,
{
	fn query_stream(
		self,
		response: &mut QueryResponse,
	) -> Result<crate::method::QueryStream<Notification<R>>> {
		let mut stream = response
			.live_queries
			.swap_remove(&self)
			.and_then(|result| match result {
				Err(e) => {
					if matches!(e, Error::NotLiveQuery(..)) {
						response.results.swap_remove(&self);
						None
					} else {
						Some(Err(e))
					}
				}
				result => Some(result),
			})
			.unwrap_or_else(|| match response.results.contains_key(&self) {
				true => Err(Error::NotLiveQuery(self)),
				false => Err(Error::QueryIndexOutOfBounds(self)),
			})?;
		Ok(crate::method::QueryStream(Either::Left(Stream {
			client: stream.client.clone(),
			id: mem::take(&mut stream.id),
			rx: stream.rx.take(),
			response_type: PhantomData,
		})))
	}
}

impl<R> QueryStream<Notification<R>> for () where R: SurrealValue + Unpin {}
impl<R> query_stream::Sealed<Notification<R>> for ()
where
	R: SurrealValue + Unpin,
{
	fn query_stream(
		self,
		response: &mut QueryResponse,
	) -> Result<crate::method::QueryStream<Notification<R>>> {
		let mut streams = Vec::with_capacity(response.live_queries.len());
		for (index, result) in mem::take(&mut response.live_queries) {
			let mut stream = match result {
				Ok(stream) => stream,
				Err(e) => {
					if matches!(e, Error::NotLiveQuery(..)) {
						match response.results.swap_remove(&index) {
							Some((_, Err(_))) => {
								return Err(Error::ConnectionUninitialised);
							}
							Some((_, Ok(..))) => unreachable!(
								"the internal error variant indicates that an error occurred in the `LIVE SELECT` query"
							),
							None => {
								return Err(Error::ResponseAlreadyTaken);
							}
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
		Ok(crate::method::QueryStream(Either::Right(select_all(streams))))
	}
}
