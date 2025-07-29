use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::statements::AccessStatement;
use crate::expr::statements::rebuild::RebuildStatement;
use crate::expr::{
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
pub struct LogicalPlans(pub Vec<LogicalPlan>);

impl Deref for LogicalPlans {
	type Target = Vec<LogicalPlan>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for LogicalPlans {
	type Item = LogicalPlan;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for LogicalPlans {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
pub enum LogicalPlan {
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

impl LogicalPlan {
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
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		let stm = match (opt.import, self) {
			// All exports in SurrealDB 1.x are done with `UPDATE`, but
			// because `UPDATE` works different in SurrealDB 2.x, we need
			// to convert these statements into `UPSERT` statements.
			(true, Self::Update(stm)) => &LogicalPlan::Upsert(UpsertStatement {
				only: stm.only,
				what: stm.what.clone(),
				with: stm.with.clone(),
				data: stm.data.clone(),
				cond: stm.cond.clone(),
				output: stm.output.clone(),
				timeout: stm.timeout.clone(),
				parallel: stm.parallel,
				explain: stm.explain.clone(),
			}),
			(_, stm) => stm,
		};

		let res = match stm {
			Self::Access(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Alter(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Analyze(v) => v.compute(ctx, opt).await,
			Self::Break(v) => return v.compute(ctx, opt, doc).await,
			Self::Continue(v) => return v.compute(ctx, opt, doc).await,
			Self::Create(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Delete(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Define(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Foreach(v) => return v.compute(stk, ctx, opt, doc).await,
			Self::Ifelse(v) => return v.compute(stk, ctx, opt, doc).await,
			Self::Info(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Insert(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Kill(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Live(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Output(v) => return v.compute(stk, ctx, opt, doc).await,
			Self::Relate(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Rebuild(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Remove(v) => v.compute(ctx, opt, doc).await,
			Self::Select(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Set(v) => return v.compute(stk, ctx, opt, doc).await,
			Self::Show(v) => v.compute(ctx, opt, doc).await,
			Self::Sleep(v) => v.compute(ctx, opt, doc).await,
			Self::Throw(v) => return v.compute(stk, ctx, opt, doc).await,
			Self::Update(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Upsert(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Value(v) => {
				// Ensure futures are processed
				// Process the output value
				return v.compute(stk, ctx, opt, doc).await;
			}
			Self::Cancel(_) => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidStatement(
					"CANCEL cannot be used outside of a manual transaction".to_string(),
				))));
			}
			Self::Commit(_) => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidStatement(
					"COMMIT cannot be used outside of a manual transaction".to_string(),
				))));
			}
			_ => {
				return Err(ControlFlow::Err(anyhow::Error::new(Error::unreachable(
					format_args!("Unexpected statement type encountered: {self:?}"),
				))));
			}
		};

		res.map_err(ControlFlow::from)
	}
}

impl Display for LogicalPlan {
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
