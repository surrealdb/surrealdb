use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::{
	AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
	IfelseStatement, InfoStatement, InsertStatement, RebuildStatement, RelateStatement,
	RemoveStatement, SelectStatement, SetStatement, UpdateStatement, UpsertStatement,
};
use crate::expr::{
	BinaryOperator, Block, Constant, FlowResult, FunctionCall, Ident, Idiom, Literal, Mock, Param,
	PrefixOperator,
};
use crate::val::{Closure, Value};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::PostfixOperator;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Value")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Expr {
	Literal(Literal),
	Param(Param),
	Idiom(Idiom),
	Table(Ident),
	Mock(Mock),
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

	Closure(Box<Closure>),

	Break,
	Continue,
	Return(Box<Expr>),
	Throw(Box<Expr>),

	IfElse(Box<IfelseStatement>),
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

impl Expr {
	/// Check if this expression does only reads.
	pub(crate) fn read_only(&self) -> bool {
		match self {
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Info(_) => true,

			Expr::Block(block) => block.read_only(),
			Expr::Prefix {
				expr,
				..
			}
			| Expr::Postfix {
				expr,
				..
			} => expr.read_only(),
			Expr::Binary {
				left,
				right,
				..
			} => left.read_only() && right.read_only(),
			Expr::FunctionCall(function) => function.read_only(),
			Expr::Return(expr) => expr.read_only(),
			Expr::Throw(expr) => expr.read_only(),
			Expr::IfElse(s) => s.read_only(),
			Expr::Select(s) => s.read_only(),
			Expr::Let(s) => s.read_only(),
			Expr::Forach(s) => s.read_only(),
			Expr::Closure(s) => s.read_only(),
			Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_) => false,
		}
	}

	/// Checks whether all expression parts are static values
	pub(crate) fn is_static(&self) -> bool {
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

impl fmt::Display for Expr {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		todo!()
	}
}
