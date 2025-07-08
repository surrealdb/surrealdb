use crate::expr::fmt::Pretty;
use crate::expr::function::Function;
use crate::expr::model::Model;
use crate::expr::statements::CreateStatement;
use crate::expr::statements::DeleteStatement;
use crate::expr::statements::InsertStatement;
use crate::expr::statements::KillStatement;
use crate::expr::statements::LiveStatement;
use crate::expr::statements::RelateStatement;
use crate::expr::statements::SelectStatement;
use crate::expr::statements::UpdateStatement;
use crate::expr::statements::UpsertStatement;
use crate::expr::statements::{DefineStatement, RemoveStatement};
use crate::expr::{LogicalPlan, LogicalPlans};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::fmt::{self, Display, Formatter};
use std::ops::{Deref, DerefMut};
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Query";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Query")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Query(pub LogicalPlans);

impl From<DefineStatement> for Query {
	fn from(s: DefineStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Define(s)]))
	}
}

impl From<RemoveStatement> for Query {
	fn from(s: RemoveStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Remove(s)]))
	}
}

impl From<SelectStatement> for Query {
	fn from(s: SelectStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Select(s)]))
	}
}

impl From<CreateStatement> for Query {
	fn from(s: CreateStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Create(s)]))
	}
}

impl From<UpsertStatement> for Query {
	fn from(s: UpsertStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Upsert(s)]))
	}
}

impl From<UpdateStatement> for Query {
	fn from(s: UpdateStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Update(s)]))
	}
}

impl From<RelateStatement> for Query {
	fn from(s: RelateStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Relate(s)]))
	}
}

impl From<DeleteStatement> for Query {
	fn from(s: DeleteStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Delete(s)]))
	}
}

impl From<InsertStatement> for Query {
	fn from(s: InsertStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Insert(s)]))
	}
}

impl From<LiveStatement> for Query {
	fn from(s: LiveStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Live(s)]))
	}
}

impl From<KillStatement> for Query {
	fn from(s: KillStatement) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Kill(s)]))
	}
}

impl From<Function> for Query {
	fn from(f: Function) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Value(f.into())]))
	}
}

impl From<Model> for Query {
	fn from(m: Model) -> Self {
		Query(LogicalPlans(vec![LogicalPlan::Value(m.into())]))
	}
}

impl From<LogicalPlan> for Query {
	fn from(s: LogicalPlan) -> Self {
		Query(LogicalPlans(vec![s]))
	}
}

impl From<Vec<LogicalPlan>> for Query {
	fn from(s: Vec<LogicalPlan>) -> Self {
		Query(LogicalPlans(s))
	}
}

impl Deref for Query {
	type Target = Vec<LogicalPlan>;
	fn deref(&self) -> &Self::Target {
		&self.0.0
	}
}

impl DerefMut for Query {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0.0
	}
}

impl IntoIterator for Query {
	type Item = LogicalPlan;
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
