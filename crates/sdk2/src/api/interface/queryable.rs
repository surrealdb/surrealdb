use crate::api::SurrealContext;
use crate::method::{Create, Delete, Insert, Query, Relate, Request, Select, Update, Upsert};
use crate::sql::Subject;
use surrealdb_types::Table;

pub(crate) trait Queryable: SurrealContext {
	#[inline]
	fn query(&self, sql: impl Into<String>) -> Request<Query> {
		Request::new(self, Query::new(sql.into()))
	}

	#[inline]
	fn select(&self, subject: impl Into<Subject>) -> Request<Select> {
		Request::new(self, Select::new(subject))
	}

	#[inline]
	fn create(&self, subject: impl Into<Subject>) -> Request<Create> {
		Request::new(self, Create::new(subject))
	}

	#[inline]
	fn delete(&self, subject: impl Into<Subject>) -> Request<Delete> {
		Request::new(self, Delete::new(subject))
	}

	#[inline]
	fn insert(&self, subject: impl Into<Subject>) -> Request<Insert> {
		Request::new(self, Insert::new(subject))
	}

	#[inline]
	fn update(&self, subject: impl Into<Subject>) -> Request<Update> {
		Request::new(self, Update::new(subject))
	}

	#[inline]
	fn upsert(&self, subject: impl Into<Subject>) -> Request<Upsert> {
		Request::new(self, Upsert::new(subject))
	}

	#[inline]
	fn relate(&self, from: impl Into<Subject>, through: impl Into<Table>, to: impl Into<Subject>) -> Request<Relate> {
		Request::new(self, Relate::new(from, through, to))
	}
}
