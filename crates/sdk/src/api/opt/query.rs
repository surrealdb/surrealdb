use std::marker::PhantomData;
use std::mem;

use anyhow::bail;
use futures::future::Either;
use futures::stream::select_all;
use serde::de::DeserializeOwned;

use super::Raw;
use crate::api::err::Error;
use crate::api::{OnceLockExt, Response as QueryResponse, Result};
use crate::core::expr::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, Expr, IfelseStatement,
	InfoStatement, InsertStatement, KillStatement, LiveStatement, OptionStatement, OutputStatement,
	RelateStatement, RemoveStatement, SelectStatement, TopLevelExpr, UpdateStatement, UseStatement,
};
use crate::core::sql::Ast;
use crate::core::val;
use crate::method::query::ValidQuery;
use crate::method::{self, Stats, Stream};
use crate::value::Notification;
use crate::{Connection, Surreal, Value, api};

pub struct Query(pub(crate) Result<ValidQuery>);
/// A trait for converting inputs into SQL statements
pub trait IntoQuery: into_query::Sealed {}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for Ast {}
impl into_query::Sealed for Ast {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: self.expressions.into_iter().map(From::from).collect(),
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for TopLevelExpr {}
impl into_query::Sealed for TopLevelExpr {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![self],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for UseStatement {}
impl into_query::Sealed for UseStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Use(self)],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for InfoStatement {}
impl into_query::Sealed for InfoStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Info(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for LiveStatement {}
impl into_query::Sealed for LiveStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Live(Box::new(self))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for KillStatement {}
impl into_query::Sealed for KillStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Kill(self)],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for OutputStatement {}
impl into_query::Sealed for OutputStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Return(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for IfelseStatement {}
impl into_query::Sealed for IfelseStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::IfElse(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for SelectStatement {}
impl into_query::Sealed for SelectStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Select(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for CreateStatement {}
impl into_query::Sealed for CreateStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Create(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for UpdateStatement {}
impl into_query::Sealed for UpdateStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Update(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for RelateStatement {}
impl into_query::Sealed for RelateStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Relate(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for DeleteStatement {}
impl into_query::Sealed for DeleteStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Delete(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for InsertStatement {}
impl into_query::Sealed for InsertStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Insert(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for DefineStatement {}
impl into_query::Sealed for DefineStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Define(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for AlterStatement {}
impl into_query::Sealed for AlterStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Alter(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for RemoveStatement {}
impl into_query::Sealed for RemoveStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Expr(Expr::Remove(Box::new(self)))],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

#[diagnostic::do_not_recommend]
#[doc(hidden)]
impl IntoQuery for OptionStatement {}
impl into_query::Sealed for OptionStatement {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Normal {
			query: vec![TopLevelExpr::Option(self)],
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

pub(crate) mod into_query {
	use crate::{Connection, Surreal};

	pub trait Sealed {
		/// Converts an input into SQL statements
		fn into_query<C: Connection>(self, conn: &Surreal<C>) -> super::Query;
	}
}

impl IntoQuery for &str {}
impl into_query::Sealed for &str {
	fn into_query<C: Connection>(self, conn: &Surreal<C>) -> Query {
		let query = conn.inner.router.extract().and_then(|router| {
			let capabilities = &router.config.capabilities;
			crate::core::syn::parse_with_capabilities(self, capabilities)
		});

		Query(query.map(|x| ValidQuery::Normal {
			//TODO: Figure out what type to actually use, core::expr, or core::sql
			query: x.expressions.into_iter().map(From::from).collect(),
			register_live_queries: true,
			bindings: Default::default(),
		}))
	}
}

impl IntoQuery for &String {}
impl into_query::Sealed for &String {
	fn into_query<C: Connection>(self, conn: &Surreal<C>) -> Query {
		self.as_str().into_query(conn)
	}
}

impl IntoQuery for String {}
impl into_query::Sealed for String {
	fn into_query<C: Connection>(self, conn: &Surreal<C>) -> Query {
		self.as_str().into_query(conn)
	}
}

impl IntoQuery for Raw {}
impl into_query::Sealed for Raw {
	fn into_query<C: Connection>(self, _conn: &Surreal<C>) -> Query {
		Query(Ok(ValidQuery::Raw {
			query: self.0,
			bindings: Default::default(),
		}))
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
			None => Ok(Value::from_inner(val::Value::None)),
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
			val::Value::Array(vec) => match &mut vec.0[..] {
				[] => Ok(None),
				[value] => {
					let value = mem::take(value);
					api::value::from_core_value(value)
				}
				_ => Err(Error::LossyTake(QueryResponse {
					results: mem::take(&mut response.results),
					live_queries: mem::take(&mut response.live_queries),
				})
				.into()),
			},
			_ => {
				let value = mem::take(value);
				api::value::from_core_value(value)
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
				return Ok(Value::from_inner(val::Value::None));
			}
		};

		let value = match value {
			val::Value::Object(object) => object.remove(key).unwrap_or_default(),
			_ => val::Value::None,
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
		let value: &mut val::Value = match response.results.get_mut(&index) {
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
			val::Value::Array(vec) => match &mut vec.0[..] {
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
			val::Value::None => {
				response.results.swap_remove(&index);
				Ok(None)
			}
			val::Value::Object(object) => {
				if object.is_empty() {
					response.results.swap_remove(&index);
					return Ok(None);
				}
				let Some(value) = object.remove(key) else {
					return Ok(None);
				};
				api::value::from_core_value(value)
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
				val::Value::Array(vec) => vec.0,
				vec => vec![vec],
			},
			None => {
				return Ok(vec![]);
			}
		};
		api::value::from_core_value(vec.into())
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
					val::Value::Array(vec) => {
						let mut responses = Vec::with_capacity(vec.len());
						for value in vec.iter_mut() {
							if let val::Value::Object(object) = value {
								if let Some(value) = object.remove(key) {
									responses.push(value);
								}
							}
						}
						api::value::from_core_value(responses.into())
					}
					val => {
						if let val::Value::Object(object) = val {
							if let Some(value) = object.remove(key) {
								return api::value::from_core_value(vec![value].into());
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
								response.results.insert(
									index,
									(stats, Err(Error::ResponseAlreadyTaken.into())),
								);
								return Err(error);
							}
							Some((_, Ok(..))) => unreachable!(
								"the internal error variant indicates that an error occurred in the `LIVE SELECT` query"
							),
							None => {
								bail!(Error::ResponseAlreadyTaken);
							}
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
								response.results.insert(
									index,
									(stats, Err(Error::ResponseAlreadyTaken.into())),
								);
								return Err(error);
							}
							Some((_, Ok(..))) => unreachable!(
								"the internal error variant indicates that an error occurred in the `LIVE SELECT` query"
							),
							None => {
								bail!(Error::ResponseAlreadyTaken);
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
		Ok(method::QueryStream(Either::Right(select_all(streams))))
	}
}
