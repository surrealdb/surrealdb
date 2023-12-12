use crate::sql::fmt::Pretty;
use crate::sql::statement::{Statement, Statements};
use crate::sql::Value;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Query";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
#[serde(rename = "$surrealdb::private::sql::Query")]
pub struct Query(pub Statements);

impl Deref for Query {
	type Target = Vec<Statement>;
	fn deref(&self) -> &Self::Target {
		&self.0 .0
	}
}

impl IntoIterator for Query {
	type Item = Statement;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl From<Query> for Value {
	fn from(q: Query) -> Self {
		Value::Query(q)
	}
}

impl Display for Query {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(Pretty::from(f), "{}", &self.0)
	}
}
