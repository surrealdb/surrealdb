use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{
	BinaryOperator, Block, Cast, Constant, FlowResult, Function, Future, Idiom, Literal, Mock,
	Model, Param, Table, UnaryOperator, Value,
	statements::{
		AlterStatement, CreateStatement, DefineStatement, DeleteStatement, ForeachStatement,
		IfelseStatement, InfoStatement, InsertStatement, RebuildStatement, RelateStatement,
		RemoveStatement, SelectStatement, UpdateStatement, UpsertStatement,
	},
};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Value")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Expr {
	Literal(Literal),

	Param(Param),
	Idiom(Idiom),
	Table(Table),
	Mock(Mock),
	Block(Box<Block>),
	Future(Box<Future>),
	Constant(Constant),
	Unary {
		op: UnaryOperator,
		expr: Box<Expr>,
	},
	Binary {
		left: Box<Expr>,
		op: BinaryOperator,
		right: Box<Expr>,
	},
	Model(Box<Model>),
	// TODO: Factor out the call from the function expression.
	Function(Box<Function>),

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
}

impl Expr {
	/// Check if this expression does only reads.
	pub(crate) fn readonly(&self) -> bool {
		todo!()
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
