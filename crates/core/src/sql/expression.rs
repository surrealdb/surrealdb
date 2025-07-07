use crate::sql::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, OutputStatement, RebuildStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, UpdateStatement,
	UpsertStatement,
};
use crate::sql::{
	BinaryOperator, Block, Closure, Constant, FunctionCall, Ident, Idiom, Literal, Mock, Param,
	PostfixOperator, PrefixOperator,
};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Expr {
	Literal(Literal),

	Param(Param),
	Idiom(Idiom),
	Table(Ident),
	Mock(Mock),
	// TODO(3.0) maybe unbox? check size.
	Block(Box<Block>),
	Constant(Constant),
	Prefix {
		op: PrefixOperator,
		expr: Box<Expr>,
	},
	Postfix {
		expr: Box<Expr>,
		op: PostfixOperator,
	},
	Binary {
		left: Box<Expr>,
		op: BinaryOperator,
		right: Box<Expr>,
	},
	// TODO: Factor out the call from the function expression.
	FunctionCall(Box<FunctionCall>),
	// TODO: Factor out the call from the function expression.
	Closure(Box<Closure>),

	Break,
	Continue,
	Throw(Box<Expr>),

	Return(Box<OutputStatement>),
	If(Box<IfelseStatement>),
	Select(Box<SelectStatement>),
	Create(Box<CreateStatement>),
	Update(Box<UpdateStatement>),
	Delete(Box<DeleteStatement>),
	Relate(Box<RelateStatement>),
	Insert(Box<InsertStatement>),
	Define(Box<DefineStatement>),
	Remove(Box<RemoveStatement>),
	Rebuild(Box<RebuildStatement>),
	Upsert(Box<UpsertStatement>),
	Alter(Box<AlterStatement>),
	Info(Box<InfoStatement>),
	Forach(Box<ForeachStatement>),
	Let(Box<SetStatement>),
}

impl fmt::Display for Expr {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		todo!()
	}
}

impl From<Expr> for crate::expr::Expr {
	fn from(v: Expr) -> Self {
		match v {
			Expr::Literal(l) => crate::expr::Expr::Literal(l.into()),
			Expr::Param(p) => crate::expr::Expr::Param(p.into()),
			Expr::Idiom(i) => crate::expr::Expr::Idiom(i.into()),
			Expr::Table(t) => crate::expr::Expr::Table(t.into()),
			Expr::Mock(m) => crate::expr::Expr::Mock(m.into()),
			Expr::Block(b) => crate::expr::Expr::Block(b.into()),
			Expr::Future(f) => crate::expr::Expr::Future(f.into()),
			Expr::Constant(c) => crate::expr::Expr::Constant(c.into()),
			Expr::Prefix {
				op,
				expr,
			} => crate::expr::Expr::Prefix {
				op: op.into(),
				expr: expr.into(),
			},
			Expr::Postfix {
				op,
				expr,
			} => crate::expr::Expr::Postfix {
				op: op.into(),
				expr: expr.into(),
			},

			Expr::Binary {
				left,
				op,
				right,
			} => crate::expr::Expr::Binary {
				left: left.into(),
				op: op.into(),
				right: right.into(),
			},
			Expr::FunctionCall(f) => crate::expr::Expr::FunctionCall(f.into()),
			Expr::Closure(s) => crate::expr::Expr::Closure(s.into()),
			Expr::Break => crate::expr::Expr::Break,
			Expr::Continue => crate::expr::Expr::Continue,
			Expr::Return(e) => crate::expr::Expr::Return(e.into()),
			Expr::Throw(e) => crate::expr::Expr::Throw(e.into()),
			Expr::If(s) => crate::expr::Expr::IfElse(s.into()),
			Expr::Select(s) => crate::expr::Expr::Select(s.into()),
			Expr::Create(s) => crate::expr::Expr::Create(s.into()),
			Expr::Update(s) => crate::expr::Expr::Update(s.into()),
			Expr::Delete(s) => crate::expr::Expr::Delete(s.into()),
			Expr::Relate(s) => crate::expr::Expr::Relate(s.into()),
			Expr::Insert(s) => crate::expr::Expr::Insert(s.into()),
			Expr::Define(s) => crate::expr::Expr::Define(s.into()),
			Expr::Remove(s) => crate::expr::Expr::Remove(s.into()),
			Expr::Rebuild(s) => crate::expr::Expr::Rebuild(s.into()),
			Expr::Upsert(s) => crate::expr::Expr::Upsert(s.into()),
			Expr::Alter(s) => crate::expr::Expr::Alter(s.into()),
			Expr::Info(s) => crate::expr::Expr::Info(s.into()),
			Expr::Forach(s) => crate::expr::Expr::Forach(s.into()),
			Expr::Let(s) => crate::expr::Expr::Let(s.into()),
		}
	}
}

impl From<crate::expr::Expr> for Expr {
	fn from(v: crate::expr::Expr) -> Self {
		match v {
			crate::expr::Expr::Literal(l) => Expr::Literal(l.into()),
			crate::expr::Expr::Param(p) => Expr::Param(p.into()),
			crate::expr::Expr::Idiom(i) => Expr::Idiom(i.into()),
			crate::expr::Expr::Table(t) => Expr::Table(t.into()),
			crate::expr::Expr::Mock(m) => Expr::Mock(m.into()),
			crate::expr::Expr::Block(b) => Expr::Block(b.into()),
			crate::expr::Expr::Future(f) => Expr::Future(f.into()),
			crate::expr::Expr::Constant(c) => Expr::Constant(c.into()),
			crate::expr::Expr::Prefix {
				op,
				expr,
			} => Expr::Prefix {
				op: op.into(),
				expr: expr.into(),
			},
			crate::expr::Expr::Postfix {
				expr,
				op,
			} => Expr::Postfix {
				expr: expr.into(),
				op: op.into(),
			},

			crate::expr::Expr::Binary {
				left,
				op,
				right,
			} => Expr::Binary {
				left: left.into(),
				op: op.into(),
				right: right.into(),
			},
			crate::expr::Expr::FunctionCall(f) => Expr::FunctionCall(f.into()),
			crate::expr::Expr::Closure(s) => Expr::Closure(s.into()),
			crate::expr::Expr::Break => Expr::Break,
			crate::expr::Expr::Continue => Expr::Continue,
			crate::expr::Expr::Return(e) => Expr::Return(e.into()),
			crate::expr::Expr::Throw(e) => Expr::Throw(e.into()),
			crate::expr::Expr::IfElse(s) => Expr::If(s.into()),
			crate::expr::Expr::Select(s) => Expr::Select(s.into()),
			crate::expr::Expr::Create(s) => Expr::Create(s.into()),
			crate::expr::Expr::Update(s) => Expr::Update(s.into()),
			crate::expr::Expr::Delete(s) => Expr::Delete(s.into()),
			crate::expr::Expr::Relate(s) => Expr::Relate(s.into()),
			crate::expr::Expr::Insert(s) => Expr::Insert(s.into()),
			crate::expr::Expr::Define(s) => Expr::Define(s.into()),
			crate::expr::Expr::Remove(s) => Expr::Remove(s.into()),
			crate::expr::Expr::Rebuild(s) => Expr::Rebuild(s.into()),
			crate::expr::Expr::Upsert(s) => Expr::Upsert(s.into()),
			crate::expr::Expr::Alter(s) => Expr::Alter(s.into()),
			crate::expr::Expr::Info(s) => Expr::Info(s.into()),
			crate::expr::Expr::Forach(s) => Expr::Forach(s.into()),
			crate::expr::Expr::Let(s) => Expr::Let(s.into()),
		}
	}
}
