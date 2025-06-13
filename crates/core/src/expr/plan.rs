use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::rebuild::RebuildStatement;
use crate::expr::{
	Expr, Value,
	statements::{InfoStatement, KillStatement, LiveStatement, OptionStatement, UseStatement},
};

use reblessive::tree::Stk;
use std::{
	fmt::{self, Display, Formatter, Write},
	ops::Deref,
};

use super::{ControlFlow, FlowResult};

pub struct LogicalPlan {
	expressions: Vec<TopLevelExpr>,
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

impl LogicalPlan {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		todo!()
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
