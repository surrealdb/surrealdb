use crate::api::SurrealContext;
use crate::method::Query;
use crate::method::Request;
use crate::method::Select;
use crate::method::SelectSubject;

pub(crate) trait Queryable: SurrealContext {
	#[inline]
	fn query(&self, sql: impl Into<String>) -> Request<Query> {
		Request::new(self, Query::new(sql.into()))
	}

	#[inline]
	fn select(&self, subject: impl Into<SelectSubject>) -> Request<Select> {
		Request::new(self, Select::new(subject))
	}
}
