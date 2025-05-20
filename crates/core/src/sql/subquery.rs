use crate::sql::statements::rebuild::RebuildStatement;
use crate::sql::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, IfelseStatement,
	InsertStatement, OutputStatement, RelateStatement, RemoveStatement, SelectStatement,
	UpdateStatement, UpsertStatement,
};
use crate::sql::value::SqlValue;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};

use super::statements::InfoStatement;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Subquery";

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Subquery")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Subquery {
	Value(SqlValue),
	Ifelse(IfelseStatement),
	Output(OutputStatement),
	Select(SelectStatement),
	Create(CreateStatement),
	Update(UpdateStatement),
	Delete(DeleteStatement),
	Relate(RelateStatement),
	Insert(InsertStatement),
	Define(DefineStatement),
	Remove(RemoveStatement),
	#[revision(start = 2)]
	Rebuild(RebuildStatement),
	#[revision(start = 3)]
	Upsert(UpsertStatement),
	#[revision(start = 4)]
	Alter(AlterStatement),
	#[revision(start = 5)]
	Info(InfoStatement),
}

impl PartialOrd for Subquery {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Display for Subquery {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Value(v) => write!(f, "({v})"),
			Self::Output(v) => write!(f, "({v})"),
			Self::Select(v) => write!(f, "({v})"),
			Self::Create(v) => write!(f, "({v})"),
			Self::Upsert(v) => write!(f, "({v})"),
			Self::Update(v) => write!(f, "({v})"),
			Self::Delete(v) => write!(f, "({v})"),
			Self::Relate(v) => write!(f, "({v})"),
			Self::Insert(v) => write!(f, "({v})"),
			Self::Define(v) => write!(f, "({v})"),
			Self::Remove(v) => write!(f, "({v})"),
			Self::Rebuild(v) => write!(f, "({v})"),
			Self::Alter(v) => write!(f, "({v})"),
			Self::Info(v) => write!(f, "({v})"),
			Self::Ifelse(v) => Display::fmt(v, f),
		}
	}
}

impl From<Subquery> for crate::expr::Subquery {
	fn from(v: Subquery) -> Self {
		match v {
			Subquery::Value(v) => Self::Value(v.into()),
			Subquery::Ifelse(v) => Self::Ifelse(v.into()),
			Subquery::Output(v) => Self::Output(v.into()),
			Subquery::Select(v) => Self::Select(v.into()),
			Subquery::Create(v) => Self::Create(v.into()),
			Subquery::Update(v) => Self::Update(v.into()),
			Subquery::Delete(v) => Self::Delete(v.into()),
			Subquery::Relate(v) => Self::Relate(v.into()),
			Subquery::Insert(v) => Self::Insert(v.into()),
			Subquery::Define(v) => Self::Define(v.into()),
			Subquery::Remove(v) => Self::Remove(v.into()),
			Subquery::Rebuild(v) => Self::Rebuild(v.into()),
			Subquery::Upsert(v) => Self::Upsert(v.into()),
			Subquery::Alter(v) => Self::Alter(v.into()),
			Subquery::Info(v) => Self::Info(v.into()),
		}
	}
}

impl From<crate::expr::Subquery> for Subquery {
	fn from(v: crate::expr::Subquery) -> Self {
		match v {
			crate::expr::Subquery::Value(v) => Self::Value(v.into()),
			crate::expr::Subquery::Ifelse(v) => Self::Ifelse(v.into()),
			crate::expr::Subquery::Output(v) => Self::Output(v.into()),
			crate::expr::Subquery::Select(v) => Self::Select(v.into()),
			crate::expr::Subquery::Create(v) => Self::Create(v.into()),
			crate::expr::Subquery::Update(v) => Self::Update(v.into()),
			crate::expr::Subquery::Delete(v) => Self::Delete(v.into()),
			crate::expr::Subquery::Relate(v) => Self::Relate(v.into()),
			crate::expr::Subquery::Insert(v) => Self::Insert(v.into()),
			crate::expr::Subquery::Define(v) => Self::Define(v.into()),
			crate::expr::Subquery::Remove(v) => Self::Remove(v.into()),
			crate::expr::Subquery::Rebuild(v) => Self::Rebuild(v.into()),
			crate::expr::Subquery::Upsert(v) => Self::Upsert(v.into()),
			crate::expr::Subquery::Alter(v) => Self::Alter(v.into()),
			crate::expr::Subquery::Info(v) => Self::Info(v.into()),
		}
	}
}
