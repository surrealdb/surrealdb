use crate::api::SurrealContext;
use crate::method::Query;
use crate::method::Request;
use crate::method::Select;
use crate::sql::Subject;

pub(crate) trait Queryable: SurrealContext {
	#[inline]
	fn query(&self, sql: impl Into<String>) -> Request<Query> {
		Request::new(self, Query::new(sql.into()))
	}

	#[inline]
	fn select(&self, subject: impl Into<Subject>) -> Request<Select> {
		Request::new(self, Select::new(subject))
	}
}
