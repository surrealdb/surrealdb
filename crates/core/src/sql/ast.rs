use crate::sql::{
	Expr,
	statements::{
		AccessStatement, InfoStatement, KillStatement, LiveStatement, OptionStatement,
		RebuildStatement, UseStatement,
	},
};

pub struct Ast {
	statements: Vec<TopLevelExpr>,
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
