use super::Raw;
use crate::{
	api::{QueryResults, Result, err::Error},
	method,
};
use anyhow::{Context, bail};
use futures::future::Either;
use futures::stream::select_all;
use std::borrow::Cow;
use std::mem;
use surrealdb_core::expr::statements::*;
use surrealdb_core::{
	dbs::{QueryStats, Variables},
	expr::{TryFromValue, Value},
};

/// A trait for converting inputs into SQL statements
pub trait IntoQuery {
	fn into_query(self) -> String;
}

impl IntoQuery for String {
	fn into_query(self) -> String {
		self
	}
}
impl IntoQuery for &str {
	fn into_query(self) -> String {
		self.to_string()
	}
}

macro_rules! impl_into_query_for_statement {
	($($stmt:ident),*) => {
			$(
				impl IntoQuery for $stmt {
					fn into_query(self) -> String {
						self.to_string()
					}
				}
			)*
	};
}
impl_into_query_for_statement!(BeginStatement, CommitStatement, SelectStatement);

pub trait IntoVariables {
	fn into_variables(self) -> Variables;
}

impl IntoVariables for Variables {
	fn into_variables(self) -> Variables {
		self
	}
}

impl IntoVariables for (&str, &str) {
	fn into_variables(self) -> Variables {
		let (key, value) = self;
		let mut variables = Variables::new();
		variables.insert(key.to_string(), Value::Strand(value.into()));
		variables
	}
}

/// Represents a way to take a single query result from a list of responses
pub trait QueryAccessor<R>: query_accessor::Sealed<R> {}

mod query_accessor {
	pub trait Sealed<R> {
		/// Extracts and deserializes a query result from a query response
		fn query_result(self, results: &mut super::QueryResults) -> super::Result<R>;

		/// Extracts the statistics from a query response
		fn stats(&self, response: &super::QueryResults) -> Option<super::QueryStats> {
			response.results.get(&0).map(|x| x.stats.clone())
		}
	}
}

impl QueryAccessor<Value> for usize {}
impl query_accessor::Sealed<Value> for usize {
	fn query_result(self, results: &mut QueryResults) -> Result<Value> {
		match results.results.swap_remove(&self) {
			Some(query_result) => {
				let mut values = query_result.values?;
				if values.is_empty() {
					bail!("No values found for index {}", self);
				}

				if values.len() > 1 {
					bail!("{} values found for index {}, but expected 1", values.len(), self);
				}

				Ok(values.pop().unwrap())
			},
			None => Ok(Value::None),
		}
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStats> {
		results.results.get(self).map(|x| x.stats.clone())
	}
}

impl<T> QueryAccessor<Option<T>> for usize where T: TryFromValue {}
impl<T> query_accessor::Sealed<Option<T>> for usize
where
	T: TryFromValue,
{
	fn query_result(self, results: &mut QueryResults) -> Result<Option<T>> {
		let mut values = match results.results.swap_remove(&self) {
			Some(query_result) => match query_result.values {
				Ok(val) => val,
				Err(error) => {
					return Err(error.into());
				}
			},
			None => {
				return Ok(None);
			}
		};

		if values.is_empty() {
			return Ok(None);
		}

		if values.len() > 1 {
			return Err(Error::LossyTake(QueryResults {
				results: mem::take(&mut results.results),
			})
			.into());
		}

		let value = values.pop().unwrap();
		Option::<T>::try_from_value(value)
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStats> {
		results.results.get(self).map(|x| x.stats.clone())
	}
}

impl QueryAccessor<Value> for (usize, &str) {}
impl query_accessor::Sealed<Value> for (usize, &str) {
	fn query_result(self, response: &mut QueryResults) -> Result<Value> {
		let (index, key) = self;
		let mut values = match response.results.swap_remove(&index) {
			Some(query_result) => match query_result.values {
				Ok(val) => val,
				Err(error) => {
					return Err(error.into());
				}
			},
			None => {
				return Ok(Value::None);
			}
		};

		if values.is_empty() {
			return Ok(Value::None);
		}

		if values.len() > 1 {
			bail!("{} values found for index {}, but expected 1", values.len(), index);
		}

		let mut value = values.pop().unwrap().try_into()?;

		let value = match &mut value {
			Value::Object(object) => object.remove(key).unwrap_or_default(),
			_ => Value::None,
		};

		Ok(value)
	}

	fn stats(&self, response: &QueryResults) -> Option<QueryStats> {
		response.results.get(&self.0).map(|x| x.stats.clone())
	}
}

impl<T> QueryAccessor<Option<T>> for (usize, &str) where T: TryFromValue {}
impl<T> query_accessor::Sealed<Option<T>> for (usize, &str)
where
	T: TryFromValue,
{
	fn query_result(self, results: &mut QueryResults) -> Result<Option<T>> {
		let (index, key) = self;
		let mut values = match results.results.swap_remove(&index) {
			Some(query_result) => match query_result.values {
				Ok(val) => val,
				Err(error) => {
					return Err(error.into());
				}
			},
			None => {
				return Ok(None);
			}
		};

		if values.is_empty() {
			return Ok(None);
		}

		if values.len() > 1 {
			bail!("{} values found for index {}, but expected 1", values.len(), index);
		}

		let mut value = values.pop().unwrap().try_into()?;

		match &mut value {
			Value::None => {
				Ok(None)
			}
			Value::Object(object) => {
				if object.is_empty() {
					return Ok(None);
				}
				let Some(value) = object.remove(key) else {
					return Ok(None);
				};
				Option::<T>::try_from_value(value)
			}
			_ => Ok(None),
		}
	}

	fn stats(&self, response: &QueryResults) -> Option<QueryStats> {
		response.results.get(&self.0).map(|x| x.stats.clone())
	}
}

impl<T> QueryAccessor<Vec<T>> for usize where T: TryFromValue {}
impl<T> query_accessor::Sealed<Vec<T>> for usize
where
	T: TryFromValue,
{
	fn query_result(self, results: &mut QueryResults) -> Result<Vec<T>> {
		let vec = match results.results.swap_remove(&self) {
			Some(query_result) => match query_result.values {
				Ok(vec) => vec,
				Err(err) => {
					return Err(err.into());
				}
			},
			None => {
				return Ok(vec![]);
			}
		};
		vec.into_iter().map(T::try_from_value).collect::<Result<Vec<T>>>()
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStats> {
		results.results.get(self).map(|x| x.stats.clone())
	}
}

impl<T> QueryAccessor<Vec<T>> for (usize, &str) where T: TryFromValue {}
impl<T> query_accessor::Sealed<Vec<T>> for (usize, &str)
where
	T: TryFromValue,
{
	fn query_result(self, results: &mut QueryResults) -> Result<Vec<T>> {
		let (index, key) = self;
		let values = match results.results.get_mut(&index) {
			Some(query_result) => match &mut query_result.values {
				Ok(values) => values,
				Err(error) => {
					bail!(error.clone());
				}
			},
			None => return Ok(vec![]),
		};

		let mut responses = Vec::with_capacity(values.len());
		for value in values.iter_mut() {
			if let Value::Object(object) = value {
				if let Some(value) = object.remove(key) {
					responses.push(T::try_from_value(value)?);
				}
			}
		}
		Ok(responses)
	}

	fn stats(&self, response: &QueryResults) -> Option<QueryStats> {
		response.results.get(&self.0).map(|x| x.stats.clone())
	}
}

impl QueryAccessor<Value> for &str {}
impl query_accessor::Sealed<Value> for &str {
	fn query_result(self, results: &mut QueryResults) -> Result<Value> {
		(0, self).query_result(results)
	}
}

impl<T> QueryAccessor<Option<T>> for &str where T: TryFromValue {}
impl<T> query_accessor::Sealed<Option<T>> for &str
where
	T: TryFromValue,
{
	fn query_result(self, response: &mut QueryResults) -> Result<Option<T>> {
		(0, self).query_result(response)
	}
}

impl<T> QueryAccessor<Vec<T>> for &str where T: TryFromValue {}
impl<T> query_accessor::Sealed<Vec<T>> for &str
where
	T: TryFromValue,
{
	fn query_result(self, response: &mut QueryResults) -> Result<Vec<T>> {
		(0, self).query_result(response)
	}
}

// impl QueryStream<Value> for usize {}
// impl query_stream::Sealed<Value> for usize {
// 	fn query_stream(self, response: &mut QueryResponse) -> Result<method::QueryStream<Value>> {
// 		let stream = response
// 			.live_queries
// 			.swap_remove(&self)
// 			.and_then(|result| match result {
// 				Err(e) => {
// 					if matches!(e.downcast_ref(), Some(Error::NotLiveQuery(..))) {
// 						response.results.swap_remove(&self).and_then(|x| x.result.err().map(Err))
// 					} else {
// 						Some(Err(e))
// 					}
// 				}
// 				result => Some(result),
// 			})
// 			.unwrap_or_else(|| match response.results.contains_key(&self) {
// 				true => Err(Error::NotLiveQuery(self).into()),
// 				false => Err(Error::QueryIndexOutOfBounds(self).into()),
// 			})?;
// 		Ok(method::QueryStream(Either::Left(stream)))
// 	}
// }

// impl QueryStream<Value> for () {}
// impl query_stream::Sealed<Value> for () {
// 	fn query_stream(self, results: &mut QueryResults) -> Result<method::QueryStream<Value>> {
// 		let mut streams = Vec::with_capacity(results.live_queries.len());

// 		for (index, result) in mem::take(&mut results.live_queries) {
// 			match result {
// 				Ok(stream) => streams.push(stream),
// 				Err(e) => {
// 					if matches!(e.downcast_ref(), Some(Error::NotLiveQuery(..))) {
// 						match results.results.swap_remove(&index) {
// 							Some(query_result) => {
// 								results.results.insert(
// 									index,
// 									None,
// 								);
// 								return Err(error);
// 							}
// 							None => {
// 								bail!(Error::ResponseAlreadyTaken);
// 							}
// 						}
// 					} else {
// 						return Err(e);
// 					}
// 				}
// 			}
// 		}
// 		Ok(method::QueryStream(Either::Right(select_all(streams))))
// 	}
// }

// impl<R> QueryStream<Notification<R>> for usize where R: DeserializeOwned + Unpin {}
// impl<R> query_stream::Sealed<Notification<R>> for usize
// where
// 	R: DeserializeOwned + Unpin,
// {
// 	fn query_stream(
// 		self,
// 		response: &mut QueryResponse,
// 	) -> Result<method::QueryStream<Notification<R>>> {
// 		let mut stream = response
// 			.live_queries
// 			.swap_remove(&self)
// 			.and_then(|result| match result {
// 				Err(e) => {
// 					if matches!(e.downcast_ref(), Some(Error::NotLiveQuery(..))) {
// 						response.results.swap_remove(&self).and_then(|x| x.1.err().map(Err))
// 					} else {
// 						Some(Err(e))
// 					}
// 				}
// 				result => Some(result),
// 			})
// 			.unwrap_or_else(|| match response.results.contains_key(&self) {
// 				true => Err(Error::NotLiveQuery(self).into()),
// 				false => Err(Error::QueryIndexOutOfBounds(self).into()),
// 			})?;
// 		Ok(method::QueryStream(Either::Left(Stream {
// 			client: stream.client.clone(),
// 			id: mem::take(&mut stream.id),
// 			rx: stream.rx.take(),
// 			response_type: PhantomData,
// 		})))
// 	}
// }

// impl<R> QueryStream<Notification<R>> for () where R: DeserializeOwned + Unpin {}
// impl<R> query_stream::Sealed<Notification<R>> for ()
// where
// 	R: DeserializeOwned + Unpin,
// {
// 	fn query_stream(
// 		self,
// 		response: &mut QueryResponse,
// 	) -> Result<method::QueryStream<Notification<R>>> {
// 		let mut streams = Vec::with_capacity(response.live_queries.len());
// 		for (index, result) in mem::take(&mut response.live_queries) {
// 			let mut stream = match result {
// 				Ok(stream) => stream,
// 				Err(e) => {
// 					if matches!(e.downcast_ref(), Some(Error::NotLiveQuery(..))) {
// 						match response.results.swap_remove(&index) {
// 							Some((stats, Err(error))) => {
// 								response.results.insert(
// 									index,
// 									(stats, Err(Error::ResponseAlreadyTaken.into())),
// 								);
// 								return Err(error);
// 							}
// 							Some((_, Ok(..))) => unreachable!(
// 								"the internal error variant indicates that an error occurred in the `LIVE SELECT` query"
// 							),
// 							None => {
// 								bail!(Error::ResponseAlreadyTaken);
// 							}
// 						}
// 					} else {
// 						return Err(e);
// 					}
// 				}
// 			};
// 			streams.push(Stream {
// 				client: stream.client.clone(),
// 				id: mem::take(&mut stream.id),
// 				rx: stream.rx.take(),
// 				response_type: PhantomData,
// 			});
// 		}
// 		Ok(method::QueryStream(Either::Right(select_all(streams))))
// 	}
// }
