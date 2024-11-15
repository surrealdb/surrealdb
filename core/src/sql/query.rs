use crate::sql::fmt::Pretty;
use crate::sql::function::Function;
use crate::sql::model::Model;
use crate::sql::statements::CreateStatement;
use crate::sql::statements::DeleteStatement;
use crate::sql::statements::InsertStatement;
use crate::sql::statements::KillStatement;
use crate::sql::statements::LiveStatement;
use crate::sql::statements::RelateStatement;
use crate::sql::statements::SelectStatement;
use crate::sql::statements::UpdateStatement;
use crate::sql::statements::UpsertStatement;
use crate::sql::statements::{DefineStatement, RemoveStatement};
use crate::sql::{Statement, Statements};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::fmt::{self, Display, Formatter};
use std::ops::{Deref, DerefMut};
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

impl From<SelectStatement> for Query {
	fn from(s: SelectStatement) -> Self {
		Query(Statements(vec![Statement::Select(s)]))
	}
}

impl From<CreateStatement> for Query {
	fn from(s: CreateStatement) -> Self {
		Query(Statements(vec![Statement::Create(s)]))
	}
}

impl From<UpsertStatement> for Query {
	fn from(s: UpsertStatement) -> Self {
		Query(Statements(vec![Statement::Upsert(s)]))
	}
}

impl From<UpdateStatement> for Query {
	fn from(s: UpdateStatement) -> Self {
		Query(Statements(vec![Statement::Update(s)]))
	}
}

impl From<RelateStatement> for Query {
	fn from(s: RelateStatement) -> Self {
		Query(Statements(vec![Statement::Relate(s)]))
	}
}

impl From<DeleteStatement> for Query {
	fn from(s: DeleteStatement) -> Self {
		Query(Statements(vec![Statement::Delete(s)]))
	}
}

impl From<InsertStatement> for Query {
	fn from(s: InsertStatement) -> Self {
		Query(Statements(vec![Statement::Insert(s)]))
	}
}

impl From<LiveStatement> for Query {
	fn from(s: LiveStatement) -> Self {
		Query(Statements(vec![Statement::Live(s)]))
	}
}

impl From<KillStatement> for Query {
	fn from(s: KillStatement) -> Self {
		Query(Statements(vec![Statement::Kill(s)]))
	}
}

impl From<Function> for Query {
	fn from(f: Function) -> Self {
		Query(Statements(vec![Statement::Value(f.into())]))
	}
}

impl From<Model> for Query {
	fn from(m: Model) -> Self {
		Query(Statements(vec![Statement::Value(m.into())]))
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

impl DerefMut for Query {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0 .0
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
