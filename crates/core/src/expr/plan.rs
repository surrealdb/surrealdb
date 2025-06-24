use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::rebuild::RebuildStatement;
use crate::expr::statements::{
	InfoStatement, KillStatement, LiveStatement, OptionStatement, UseStatement,
};
use crate::expr::{Expr, Value};

use reblessive::tree::Stk;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;

use super::{ControlFlow, FlowResult};

pub struct LogicalPlan {
	pub expressions: Vec<TopLevelExpr>,
}

impl LogicalPlan {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.expressions.iter().all(|x| x.read_only())
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		todo!()
	}
}

impl Display for LogicalPlan {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		todo!()
	}
}

pub enum TopLevelExpr {
	Begin,
	Cancel,
	Commit,
	Info(InfoStatement),
	Kill(KillStatement),
	Live(Box<LiveStatement>),
	Option(OptionStatement),
	Use(UseStatement),
	Rebuild(RebuildStatement),
	Expr(Expr),
}

impl TopLevelExpr {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		match self {
			TopLevelExpr::Begin
			| TopLevelExpr::Cancel
			| TopLevelExpr::Commit
			| TopLevelExpr::Info(_)
			| TopLevelExpr::Use(_) => true,
			TopLevelExpr::Kill(_)
			| TopLevelExpr::Live(_)
			| TopLevelExpr::Option(_)
			| TopLevelExpr::Rebuild(_) => false,
			TopLevelExpr::Expr(expr) => expr.read_only(),
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
		todo!()
	}
}

impl Display for TopLevelExpr {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		todo!()
	}
}
