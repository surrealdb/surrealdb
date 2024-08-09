use crate::sql::fmt::Pretty;
use crate::sql::statements::{DefineStatement, RemoveStatement};
use crate::sql::{Statement, Statements};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Query";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[serde(rename = "$surrealdb::private::sql::Query")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Query(pub Statements);

impl From<DefineStatement> for Query {
	fn from(s: DefineStatement) -> Self {
		Query(Statements(vec![Statement::Define(s)]))
	}
}

impl From<RemoveStatement> for Query {
	fn from(s: RemoveStatement) -> Self {
		Query(Statements(vec![Statement::Remove(s)]))
	}
}

impl From<Statement> for Query {
	fn from(s: Statement) -> Self {
		Query(Statements(vec![s]))
	}
}

impl From<Vec<Statement>> for Query {
	fn from(s: Vec<Statement>) -> Self {
		Query(Statements(s))
	}
}

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

impl Display for Query {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(Pretty::from(f), "{}", &self.0)
	}
}
