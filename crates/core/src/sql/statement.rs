use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::sql::statements::rebuild::RebuildStatement;
use crate::sql::statements::AccessStatement;
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
	value::Value,
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

crate::sql::impl_display_from_sql!(Statements);

impl crate::sql::DisplaySql for Statements {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(
			&Fmt::one_line_separated(self.0.iter().map(|v| Fmt::new(v, |v, f| write!(f, "{v};")))),
			f,
		)
	}
}

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Statement {
	Value(Value),
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

impl Statement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Self::Value(v) => v.writeable(),
			Self::Access(_) => true,
			Self::Alter(_) => true,
			Self::Analyze(_) => false,
			Self::Break(_) => false,
			Self::Continue(_) => false,
			Self::Create(v) => v.writeable(),
			Self::Define(_) => true,
			Self::Delete(v) => v.writeable(),
			Self::Foreach(v) => v.writeable(),
			Self::Ifelse(v) => v.writeable(),
			Self::Info(_) => false,
			Self::Insert(v) => v.writeable(),
			Self::Kill(_) => true,
			Self::Live(_) => true,
			Self::Output(v) => v.writeable(),
			Self::Option(_) => false,
			Self::Rebuild(_) => true,
			Self::Relate(v) => v.writeable(),
			Self::Remove(_) => true,
			Self::Select(v) => v.writeable(),
			Self::Set(v) => v.writeable(),
			Self::Show(_) => false,
			Self::Sleep(_) => false,
			Self::Throw(_) => false,
			Self::Upsert(v) => v.writeable(),
			Self::Update(v) => v.writeable(),
			Self::Use(_) => false,
			_ => false,
		}
	}
}

crate::sql::impl_display_from_sql!(Statement);

impl crate::sql::DisplaySql for Statement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
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
