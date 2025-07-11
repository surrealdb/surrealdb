use crate::api::{QueryResults, Result};
use anyhow::bail;
use surrealdb_core::expr::statements::*;
use surrealdb_core::{dbs::Variables, expr::Value, protocol::TryFromValue};
use surrealdb_protocol::proto::rpc::v1::QueryStats as QueryStatsProto;
use surrealdb_protocol::proto::v1::Value as ValueProto;
use surrealdb_protocol::proto::v1::value::Value as ValueProtoEnum;

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
pub trait QueryAccessor<RT: TryFromValue>: query_accessor::Sealed<RT> {}

mod query_accessor {
	pub trait Sealed<RT> {
		/// Extracts and deserializes a query result from a query response
		fn take(self, results: &mut super::QueryResults) -> anyhow::Result<RT>;

		fn stats(&self, results: &super::QueryResults) -> Option<super::QueryStatsProto>;
	}
}

impl QueryAccessor<Value> for usize {}
impl query_accessor::Sealed<Value> for usize {
	fn take(self, results: &mut QueryResults) -> Result<Value> {
		match results.results.swap_remove(&self) {
			Some(query_result) => {
				if let Some(error) = &query_result.error {
					return Err(error.clone().into());
				}

				let mut values = query_result.values;
				if values.is_empty() {
					bail!("No values found for index {}", self);
				}

				if values.len() > 1 {
					bail!("{} values found for index {}, but expected 1", values.len(), self);
				}

				Ok(values.pop().unwrap().try_into()?)
			}
			None => Ok(Value::None),
		}
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStatsProto> {
		results.results.get(self).map(|x| x.stats.clone())
	}
}

impl<T> QueryAccessor<Option<T>> for usize where T: TryFromValue {}
impl<T> query_accessor::Sealed<Option<T>> for usize
where
	T: TryFromValue,
{
	fn take(self, results: &mut QueryResults) -> Result<Option<T>> {
		let mut values = match results.results.swap_remove(&self) {
			Some(query_result) => query_result.values,
			None => {
				return Ok(None);
			}
		};

		if values.is_empty() {
			return Ok(None);
		}

		if values.len() > 1 {
			bail!("Multiple values found for index {}", self);
		}

		let value = values.pop().unwrap().try_into()?;
		Option::<T>::try_from_value(value)
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStatsProto> {
		results.results.get(self).map(|x| x.stats.clone())
	}
}

impl QueryAccessor<Value> for (usize, &str) {}
impl query_accessor::Sealed<Value> for (usize, &str) {
	fn take(self, response: &mut QueryResults) -> Result<Value> {
		let (index, key) = self;
		let mut values = match response.results.swap_remove(&index) {
			Some(query_result) => query_result.values,
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

		let mut value = values.pop().unwrap();

		let value = match &mut value.value {
			Some(ValueProtoEnum::Object(object)) => object.items.remove(key).unwrap_or_default(),
			_ => ValueProto::none(),
		};

		Ok(value.try_into()?)
	}

	fn stats(&self, response: &QueryResults) -> Option<QueryStatsProto> {
		response.results.get(&self.0).map(|x| x.stats.clone())
	}
}

impl<T> QueryAccessor<Option<T>> for (usize, &str) where T: TryFromValue {}
impl<T> query_accessor::Sealed<Option<T>> for (usize, &str)
where
	T: TryFromValue,
{
	fn take(self, results: &mut QueryResults) -> Result<Option<T>> {
		let (index, key) = self;
		let mut values = match results.results.swap_remove(&index) {
			Some(query_result) => query_result.values,
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

		let mut value = values.pop().unwrap();

		match &mut value.value {
			Some(ValueProtoEnum::Object(object)) => {
				if object.items.is_empty() {
					return Ok(None);
				}
				let Some(value) = object.items.remove(key) else {
					return Ok(None);
				};
				Option::<T>::try_from_value(value)
			}
			_ => Ok(None),
		}
	}

	fn stats(&self, response: &QueryResults) -> Option<QueryStatsProto> {
		response.results.get(&self.0).map(|x| x.stats.clone())
	}
}

impl<T> QueryAccessor<Vec<T>> for usize where T: TryFromValue {}
impl<T> query_accessor::Sealed<Vec<T>> for usize
where
	T: TryFromValue,
{
	fn take(self, results: &mut QueryResults) -> Result<Vec<T>> {
		let vec = match results.results.swap_remove(&self) {
			Some(query_result) => query_result.values,
			None => {
				return Ok(vec![]);
			}
		};
		vec.into_iter().map(|v| T::try_from_value(v)).collect::<Result<Vec<T>>>()
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStatsProto> {
		results.results.get(self).map(|x| x.stats.clone())
	}
}

impl<T> QueryAccessor<Vec<T>> for (usize, &str) where T: TryFromValue {}
impl<T> query_accessor::Sealed<Vec<T>> for (usize, &str)
where
	T: TryFromValue,
{
	fn take(self, results: &mut QueryResults) -> Result<Vec<T>> {
		use surrealdb_protocol::proto::v1::value::Value as ValueProtoEnum;

		let (index, key) = self;
		let values = match results.results.get_mut(&index) {
			Some(query_result) => &mut query_result.values,
			None => return Err(anyhow::anyhow!("Index out of bounds: {index}")),
		};

		let mut responses = Vec::with_capacity(values.len());
		for value in values.iter_mut() {
			if let Some(ValueProtoEnum::Object(object)) = &mut value.value {
				if let Some(value) = object.items.remove(key) {
					responses.push(T::try_from_value(value)?);
				}
			}
		}
		Ok(responses)
	}

	fn stats(&self, response: &QueryResults) -> Option<QueryStatsProto> {
		response.results.get(&self.0).map(|x| x.stats.clone())
	}
}

impl QueryAccessor<Value> for &str {}
impl query_accessor::Sealed<Value> for &str {
	fn take(self, results: &mut QueryResults) -> Result<Value> {
		(0, self).take(results)
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStatsProto> {
		<(usize, &str) as query_accessor::Sealed<Value>>::stats(&(0_usize, *self), results)
	}
}

impl<T> QueryAccessor<Option<T>> for &str where T: TryFromValue {}
impl<T> query_accessor::Sealed<Option<T>> for &str
where
	T: TryFromValue,
{
	fn take(self, results: &mut QueryResults) -> Result<Option<T>> {
		(0, self).take(results)
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStatsProto> {
		<(usize, &str) as query_accessor::Sealed<Option<T>>>::stats(&(0_usize, *self), results)
	}
}

impl<T> QueryAccessor<Vec<T>> for &str where T: TryFromValue {}
impl<T> query_accessor::Sealed<Vec<T>> for &str
where
	T: TryFromValue,
{
	fn take(self, results: &mut QueryResults) -> Result<Vec<T>> {
		(0, self).take(results)
	}

	fn stats(&self, results: &QueryResults) -> Option<QueryStatsProto> {
		<(usize, &str) as query_accessor::Sealed<Vec<T>>>::stats(&(0_usize, *self), results)
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
