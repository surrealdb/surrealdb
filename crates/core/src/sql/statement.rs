use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::statements::AccessStatement;
use crate::sql::statements::rebuild::RebuildStatement;
use crate::sql::{
	fmt::{Fmt, Pretty},
	statements::{
		AlterStatement, AnalyzeStatement, BeginStatement, BreakStatement, CancelStatement,
		CommitStatement, ContinueStatement, CreateStatement, DefineStatement, DeleteStatement,
		ForeachStatement, IfelseStatement, InfoStatement, InsertStatement, KillStatement,
		LiveStatement, OptionStatement, OutputStatement, RelateStatement, RemoveStatement,
		SelectStatement, SetStatement, ShowStatement, SleepStatement, ThrowStatement,
		UpdateStatement, UpsertStatement, UseStatement,
	},
	value::SqlValue,
};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{
	fmt::{self, Display, Formatter, Write},
	ops::Deref,
};

use super::{ControlFlow, FlowResult};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Statements(pub Vec<Statement>);

impl Deref for Statements {
	type Target = Vec<Statement>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Statements {
	type Item = Statement;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Statements {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(
			&Fmt::one_line_separated(self.0.iter().map(|v| Fmt::new(v, |v, f| write!(f, "{v};")))),
			f,
		)
	}
}

impl From<Statements> for crate::expr::LogicalPlans {
	fn from(v: Statements) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::LogicalPlans> for Statements {
	fn from(v: crate::expr::LogicalPlans) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Statement {
	Value(SqlValue),
	Analyze(AnalyzeStatement),
	Begin(BeginStatement),
	Break(BreakStatement),
	Continue(ContinueStatement),
	Cancel(CancelStatement),
	Commit(CommitStatement),
	Create(CreateStatement),
	Define(DefineStatement),
	Delete(DeleteStatement),
	Foreach(ForeachStatement),
	Ifelse(IfelseStatement),
	Info(InfoStatement),
	Insert(InsertStatement),
	Kill(KillStatement),
	Live(LiveStatement),
	Option(OptionStatement),
	Output(OutputStatement),
	Relate(RelateStatement),
	Remove(RemoveStatement),
	Select(SelectStatement),
	Set(SetStatement),
	Show(ShowStatement),
	Sleep(SleepStatement),
	Update(UpdateStatement),
	Throw(ThrowStatement),
	Use(UseStatement),
	#[revision(start = 2)]
	Rebuild(RebuildStatement),
	#[revision(start = 3)]
	Upsert(UpsertStatement),
	#[revision(start = 4)]
	Alter(AlterStatement),
	// TODO(gguillemas): Document once bearer access is no longer experimental.
	#[revision(start = 5)]
	Access(AccessStatement),
}

impl Display for Statement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Value(v) => write!(Pretty::from(f), "{v}"),
			Self::Access(v) => write!(Pretty::from(f), "{v}"),
			Self::Alter(v) => write!(Pretty::from(f), "{v}"),
			Self::Analyze(v) => write!(Pretty::from(f), "{v}"),
			Self::Begin(v) => write!(Pretty::from(f), "{v}"),
			Self::Break(v) => write!(Pretty::from(f), "{v}"),
			Self::Cancel(v) => write!(Pretty::from(f), "{v}"),
			Self::Commit(v) => write!(Pretty::from(f), "{v}"),
			Self::Continue(v) => write!(Pretty::from(f), "{v}"),
			Self::Create(v) => write!(Pretty::from(f), "{v}"),
			Self::Define(v) => write!(Pretty::from(f), "{v}"),
			Self::Delete(v) => write!(Pretty::from(f), "{v}"),
			Self::Foreach(v) => write!(Pretty::from(f), "{v}"),
			Self::Insert(v) => write!(Pretty::from(f), "{v}"),
			Self::Ifelse(v) => write!(Pretty::from(f), "{v}"),
			Self::Info(v) => write!(Pretty::from(f), "{v}"),
			Self::Kill(v) => write!(Pretty::from(f), "{v}"),
			Self::Live(v) => write!(Pretty::from(f), "{v}"),
			Self::Option(v) => write!(Pretty::from(f), "{v}"),
			Self::Output(v) => write!(Pretty::from(f), "{v}"),
			Self::Rebuild(v) => write!(Pretty::from(f), "{v}"),
			Self::Relate(v) => write!(Pretty::from(f), "{v}"),
			Self::Remove(v) => write!(Pretty::from(f), "{v}"),
			Self::Select(v) => write!(Pretty::from(f), "{v}"),
			Self::Set(v) => write!(Pretty::from(f), "{v}"),
			Self::Show(v) => write!(Pretty::from(f), "{v}"),
			Self::Sleep(v) => write!(Pretty::from(f), "{v}"),
			Self::Throw(v) => write!(Pretty::from(f), "{v}"),
			Self::Update(v) => write!(Pretty::from(f), "{v}"),
			Self::Upsert(v) => write!(Pretty::from(f), "{v}"),
			Self::Use(v) => write!(Pretty::from(f), "{v}"),
		}
	}
}

impl From<Statement> for crate::expr::LogicalPlan {
	fn from(v: Statement) -> Self {
		match v {
			Statement::Value(v) => crate::expr::LogicalPlan::Value(v.into()),
			Statement::Analyze(v) => crate::expr::LogicalPlan::Analyze(v.into()),
			Statement::Begin(v) => crate::expr::LogicalPlan::Begin(v.into()),
			Statement::Break(v) => crate::expr::LogicalPlan::Break(v.into()),
			Statement::Continue(v) => crate::expr::LogicalPlan::Continue(v.into()),
			Statement::Cancel(v) => crate::expr::LogicalPlan::Cancel(v.into()),
			Statement::Commit(v) => crate::expr::LogicalPlan::Commit(v.into()),
			Statement::Create(v) => crate::expr::LogicalPlan::Create(v.into()),
			Statement::Define(v) => crate::expr::LogicalPlan::Define(v.into()),
			Statement::Delete(v) => crate::expr::LogicalPlan::Delete(v.into()),
			Statement::Foreach(v) => crate::expr::LogicalPlan::Foreach(v.into()),
			Statement::Ifelse(v) => crate::expr::LogicalPlan::Ifelse(v.into()),
			Statement::Info(v) => crate::expr::LogicalPlan::Info(v.into()),
			Statement::Insert(v) => crate::expr::LogicalPlan::Insert(v.into()),
			Statement::Kill(v) => crate::expr::LogicalPlan::Kill(v.into()),
			Statement::Live(v) => crate::expr::LogicalPlan::Live(v.into()),
			Statement::Option(v) => crate::expr::LogicalPlan::Option(v.into()),
			Statement::Output(v) => crate::expr::LogicalPlan::Output(v.into()),
			Statement::Relate(v) => crate::expr::LogicalPlan::Relate(v.into()),
			Statement::Remove(v) => crate::expr::LogicalPlan::Remove(v.into()),
			Statement::Select(v) => crate::expr::LogicalPlan::Select(v.into()),
			Statement::Set(v) => crate::expr::LogicalPlan::Set(v.into()),
			Statement::Show(v) => crate::expr::LogicalPlan::Show(v.into()),
			Statement::Sleep(v) => crate::expr::LogicalPlan::Sleep(v.into()),
			Statement::Update(v) => crate::expr::LogicalPlan::Update(v.into()),
			Statement::Throw(v) => crate::expr::LogicalPlan::Throw(v.into()),
			Statement::Use(v) => crate::expr::LogicalPlan::Use(v.into()),
			Statement::Rebuild(v) => crate::expr::LogicalPlan::Rebuild(v.into()),
			Statement::Upsert(v) => crate::expr::LogicalPlan::Upsert(v.into()),
			Statement::Alter(v) => crate::expr::LogicalPlan::Alter(v.into()),
			Statement::Access(v) => crate::expr::LogicalPlan::Access(v.into()),
		}
	}
}

impl From<crate::expr::LogicalPlan> for Statement {
	fn from(v: crate::expr::LogicalPlan) -> Self {
		match v {
			crate::expr::LogicalPlan::Value(v) => Self::Value(v.into()),
			crate::expr::LogicalPlan::Analyze(v) => Self::Analyze(v.into()),
			crate::expr::LogicalPlan::Begin(v) => Self::Begin(v.into()),
			crate::expr::LogicalPlan::Break(v) => Self::Break(v.into()),
			crate::expr::LogicalPlan::Continue(v) => Self::Continue(v.into()),
			crate::expr::LogicalPlan::Cancel(v) => Self::Cancel(v.into()),
			crate::expr::LogicalPlan::Commit(v) => Self::Commit(v.into()),
			crate::expr::LogicalPlan::Create(v) => Self::Create(v.into()),
			crate::expr::LogicalPlan::Define(v) => Self::Define(v.into()),
			crate::expr::LogicalPlan::Delete(v) => Self::Delete(v.into()),
			crate::expr::LogicalPlan::Foreach(v) => Self::Foreach(v.into()),
			crate::expr::LogicalPlan::Ifelse(v) => Self::Ifelse(v.into()),
			crate::expr::LogicalPlan::Info(v) => Self::Info(v.into()),
			crate::expr::LogicalPlan::Insert(v) => Self::Insert(v.into()),
			crate::expr::LogicalPlan::Kill(v) => Self::Kill(v.into()),
			crate::expr::LogicalPlan::Live(v) => Self::Live(v.into()),
			crate::expr::LogicalPlan::Option(v) => Self::Option(v.into()),
			crate::expr::LogicalPlan::Output(v) => Self::Output(v.into()),
			crate::expr::LogicalPlan::Relate(v) => Self::Relate(v.into()),
			crate::expr::LogicalPlan::Remove(v) => Self::Remove(v.into()),
			crate::expr::LogicalPlan::Select(v) => Self::Select(v.into()),
			crate::expr::LogicalPlan::Set(v) => Self::Set(v.into()),
			crate::expr::LogicalPlan::Show(v) => Self::Show(v.into()),
			crate::expr::LogicalPlan::Sleep(v) => Self::Sleep(v.into()),
			crate::expr::LogicalPlan::Update(v) => Self::Update(v.into()),
			crate::expr::LogicalPlan::Throw(v) => Self::Throw(v.into()),
			crate::expr::LogicalPlan::Use(v) => Self::Use(v.into()),
			crate::expr::LogicalPlan::Rebuild(v) => Self::Rebuild(v.into()),
			crate::expr::LogicalPlan::Upsert(v) => Self::Upsert(v.into()),
			crate::expr::LogicalPlan::Alter(v) => Self::Alter(v.into()),
			crate::expr::LogicalPlan::Access(v) => Self::Access(v.into()),
		}
	}
}
