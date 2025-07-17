use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::rebuild::RebuildStatement;
use crate::expr::statements::{KillStatement, LiveStatement, OptionStatement, UseStatement};
use crate::expr::{Expr, Value};

use reblessive::tree::Stk;
use std::fmt::{self, Display, Formatter};

use super::FlowResult;
use super::statements::AccessStatement;

pub struct LogicalPlan {
	pub expressions: Vec<TopLevelExpr>,
}

impl LogicalPlan {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.expressions.iter().all(|x| x.read_only())
	}
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum TopLevelExpr {
	Begin,
	Cancel,
	Commit,
	Access(Box<AccessStatement>),
	Kill(KillStatement),
	Live(Box<LiveStatement>),
	Option(OptionStatement),
	Use(UseStatement),
	Expr(Expr),
}

impl TopLevelExpr {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		match self {
			TopLevelExpr::Begin
			| TopLevelExpr::Cancel
			| TopLevelExpr::Commit
			| TopLevelExpr::Use(_) => true,
			TopLevelExpr::Kill(_)
			| TopLevelExpr::Live(_)
			| TopLevelExpr::Option(_)
			| TopLevelExpr::Access(_) => false,
			TopLevelExpr::Expr(expr) => expr.read_only(),
		}
	}
}

impl Display for TopLevelExpr {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			TopLevelExpr::Begin => write!(f, "BEGIN"),
			TopLevelExpr::Cancel => write!(f, "CANCEL"),
			TopLevelExpr::Commit => write!(f, "COMMIT"),
			TopLevelExpr::Access(s) => s.fmt(f),
			TopLevelExpr::Kill(s) => s.fmt(f),
			TopLevelExpr::Live(s) => s.fmt(f),
			TopLevelExpr::Option(s) => s.fmt(f),
			TopLevelExpr::Use(s) => s.fmt(f),
			TopLevelExpr::Expr(e) => e.fmt(f),
		}
	}
}
