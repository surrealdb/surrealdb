use crate::api::err::Error;
use crate::api::opt::from_value;
use crate::api::Result;
use crate::sql;
use crate::sql::statements::*;
use crate::sql::Object;
use crate::sql::Statement;
use crate::sql::Statements;
use crate::sql::Value;
use crate::QueryResponse;
use serde::de::DeserializeOwned;
use std::mem;

/// A trait for converting inputs into SQL statements
pub trait Query {
	/// Converts an input into SQL statements
	fn try_into_query(self) -> Result<Vec<Statement>>;
}

impl Query for sql::Query {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		let sql::Query(Statements(statements)) = self;
		Ok(statements)
	}
}

impl Query for Statements {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		let Statements(statements) = self;
		Ok(statements)
	}
}

impl Query for Vec<Statement> {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(self)
	}
}

impl Query for Statement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![self])
	}
}

impl Query for UseStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Use(self)])
	}
}

impl Query for SetStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Set(self)])
	}
}

impl Query for InfoStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Info(self)])
	}
}

impl Query for LiveStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Live(self)])
	}
}

impl Query for KillStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Kill(self)])
	}
}

impl Query for BeginStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Begin(self)])
	}
}

impl Query for CancelStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Cancel(self)])
	}
}

impl Query for CommitStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Commit(self)])
	}
}

impl Query for OutputStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Output(self)])
	}
}

impl Query for IfelseStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Ifelse(self)])
	}
}

impl Query for SelectStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Select(self)])
	}
}

impl Query for CreateStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Create(self)])
	}
}

impl Query for UpdateStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Update(self)])
	}
}

impl Query for RelateStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Relate(self)])
	}
}

impl Query for DeleteStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Delete(self)])
	}
}

impl Query for InsertStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Insert(self)])
	}
}

impl Query for DefineStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Define(self)])
	}
}

impl Query for RemoveStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Remove(self)])
	}
}

impl Query for OptionStatement {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		Ok(vec![Statement::Option(self)])
	}
}

impl Query for &str {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		sql::parse(self)?.try_into_query()
	}
}

impl Query for &String {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		sql::parse(self)?.try_into_query()
	}
}

impl Query for String {
	fn try_into_query(self) -> Result<Vec<Statement>> {
		sql::parse(&self)?.try_into_query()
	}
}

/// Represents a way to take a single query result from a list of responses
pub trait QueryResult<Response>
where
	Response: DeserializeOwned,
{
	/// Extracts and deserializes a query result from a query response
	fn query_result(self, response: &mut QueryResponse) -> Result<Response>;
}

impl<T> QueryResult<Option<T>> for usize
where
	T: DeserializeOwned,
{
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Option<T>> {
		let vec = match map.get_mut(&self) {
			Some(result) => match result {
				Ok(vec) => vec,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					map.remove(&self);
					return Err(error);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let result = match &mut vec[..] {
			[] => Ok(None),
			[value] => {
				let value = mem::take(value);
				from_value(value)
			}
			_ => Err(Error::LossyTake(QueryResponse(mem::take(map))).into()),
		};
		map.remove(&self);
		result
	}
}

impl<T> QueryResult<Option<T>> for (usize, &str)
where
	T: DeserializeOwned,
{
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Option<T>> {
		let (index, key) = self;
		let vec = match map.get_mut(&index) {
			Some(result) => match result {
				Ok(vec) => vec,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					map.remove(&index);
					return Err(error);
				}
			},
			None => {
				return Ok(None);
			}
		};
		let mut value = match &mut vec[..] {
			[] => {
				map.remove(&index);
				return Ok(None);
			}
			[value] => value,
			_ => {
				return Err(Error::LossyTake(QueryResponse(mem::take(map))).into());
			}
		};
		match &mut value {
			Value::None | Value::Null => {
				map.remove(&index);
				Ok(None)
			}
			Value::Object(Object(object)) => {
				if object.is_empty() {
					map.remove(&index);
					return Ok(None);
				}
				let Some(value) = object.remove(key) else {
                    return Ok(None);
                };
				from_value(value)
			}
			_ => Ok(None),
		}
	}
}

impl<T> QueryResult<Vec<T>> for usize
where
	T: DeserializeOwned,
{
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Vec<T>> {
		let vec = match map.remove(&self) {
			Some(result) => result?,
			None => {
				return Ok(vec![]);
			}
		};
		from_value(vec.into())
	}
}

impl<T> QueryResult<Vec<T>> for (usize, &str)
where
	T: DeserializeOwned,
{
	fn query_result(self, QueryResponse(map): &mut QueryResponse) -> Result<Vec<T>> {
		let (index, key) = self;
		let response = match map.get_mut(&index) {
			Some(result) => match result {
				Ok(vec) => vec,
				Err(error) => {
					let error = mem::replace(error, Error::ConnectionUninitialised.into());
					map.remove(&index);
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
		from_value(vec.into())
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
