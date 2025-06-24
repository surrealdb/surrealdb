use crate::sql::Expr;
use crate::sql::statements::{
	AccessStatement, KillStatement, LiveStatement, OptionStatement, RebuildStatement, UseStatement,
};

pub struct Ast {
	pub statements: Vec<TopLevelExpr>,
}

pub enum TopLevelExpr {
	Begin,
	Cancel,
	Commit,
	Access(AccessStatement),
	Kill(KillStatement),
	Live(Box<LiveStatement>),
	Option(OptionStatement),
	Use(UseStatement),
	Rebuild(RebuildStatement),
	Expr(Expr),
}
