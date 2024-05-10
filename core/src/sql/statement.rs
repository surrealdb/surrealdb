use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::statements::rebuild::RebuildStatement;
use crate::sql::{
	fmt::{Fmt, Pretty},
	statements::{
		AnalyzeStatement, BeginStatement, BreakStatement, CancelStatement, CommitStatement,
		ContinueStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
		IfelseStatement, InfoStatement, InsertStatement, KillStatement, LiveStatement,
		OptionStatement, OutputStatement, RelateStatement, RemoveStatement, SelectStatement,
		SetStatement, ShowStatement, SleepStatement, ThrowStatement, UpdateStatement, UseStatement,
	},
	value::Value,
};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{
	fmt::{self, Display, Formatter, Write},
	ops::Deref,
	time::Duration,
};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
}

impl Statement {
	/// Get the statement timeout duration, if any
	pub fn timeout(&self) -> Option<Duration> {
		match self {
			Self::Create(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Delete(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Insert(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Relate(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Select(v) => v.timeout.as_ref().map(|v| *v.0),
			Self::Update(v) => v.timeout.as_ref().map(|v| *v.0),
			_ => None,
		}
	}
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Self::Value(v) => v.writeable(),
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
			Self::Update(v) => v.writeable(),
			Self::Use(_) => false,
			_ => unreachable!(),
		}
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self {
			Self::Analyze(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Break(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Continue(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Create(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Delete(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Define(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Foreach(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Ifelse(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Info(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Insert(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Kill(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Live(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Output(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Relate(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Rebuild(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Remove(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Select(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Set(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Show(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Sleep(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Throw(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Update(v) => v.compute(ctx, opt, txn, doc).await,
			Self::Value(v) => {
				// Ensure futures are processed
				let opt = &opt.new_with_futures(true);
				// Process the output value
				v.compute(ctx, opt, txn, doc).await
			}
			_ => unreachable!(),
		}
	}
}

impl Display for Statement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Value(v) => write!(Pretty::from(f), "{v}"),
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
			Self::Use(v) => write!(Pretty::from(f), "{v}"),
		}
	}
}
