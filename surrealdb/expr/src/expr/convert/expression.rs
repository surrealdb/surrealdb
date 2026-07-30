//! `sql` -> `expr` conversions for [`crate::sql::expression`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

#[cfg(feature = "gql")]
use crate::sql::Literal;
use crate::sql::expression::*;

impl From<Expr> for crate::expr::Expr {
	fn from(v: Expr) -> Self {
		match v {
			Expr::Literal(l) => crate::expr::Expr::Literal(l.into()),
			Expr::Param(p) => crate::expr::Expr::Param(p.into()),
			Expr::Idiom(i) => crate::expr::Expr::Idiom(i.into()),
			Expr::Table(t) => crate::expr::Expr::Table(t.into()),
			Expr::Mock(m) => crate::expr::Expr::Mock(m.into()),
			Expr::Block(b) => crate::expr::Expr::Block(Box::new((*b).into())),
			Expr::Constant(c) => crate::expr::Expr::Constant(c.into()),
			Expr::Prefix {
				op,
				expr,
			} => crate::expr::Expr::Prefix {
				op: op.into(),
				expr: Box::new((*expr).into()),
			},
			Expr::Postfix {
				op,
				expr,
			} => crate::expr::Expr::Postfix {
				op: op.into(),
				expr: Box::new((*expr).into()),
			},

			Expr::Binary {
				left,
				op,
				right,
			} => crate::expr::Expr::Binary {
				left: Box::new((*left).into()),
				op: op.into(),
				right: Box::new((*right).into()),
			},
			Expr::FunctionCall(f) => crate::expr::Expr::FunctionCall(Box::new((*f).into())),
			Expr::Closure(s) => crate::expr::Expr::Closure(Box::new((*s).into())),
			Expr::Break => crate::expr::Expr::Break,
			Expr::Continue => crate::expr::Expr::Continue,
			Expr::Return(e) => crate::expr::Expr::Return(Box::new((*e).into())),
			Expr::Throw(e) => crate::expr::Expr::Throw(Box::new((*e).into())),
			Expr::IfElse(s) => crate::expr::Expr::IfElse(Box::new((*s).into())),
			Expr::Select(s) => crate::expr::Expr::Select(Box::new((*s).into())),
			Expr::Create(s) => crate::expr::Expr::Create(Box::new((*s).into())),
			Expr::Update(s) => crate::expr::Expr::Update(Box::new((*s).into())),
			Expr::Delete(s) => crate::expr::Expr::Delete(Box::new((*s).into())),
			Expr::Relate(s) => crate::expr::Expr::Relate(Box::new((*s).into())),
			Expr::Insert(s) => crate::expr::Expr::Insert(Box::new((*s).into())),
			Expr::Define(s) => crate::expr::Expr::Define(Box::new((*s).into())),
			Expr::Remove(s) => crate::expr::Expr::Remove(Box::new((*s).into())),
			Expr::Rebuild(s) => crate::expr::Expr::Rebuild(Box::new((*s).into())),
			Expr::Upsert(s) => crate::expr::Expr::Upsert(Box::new((*s).into())),
			Expr::Alter(s) => crate::expr::Expr::Alter(Box::new((*s).into())),
			Expr::Info(s) => crate::expr::Expr::Info(Box::new((*s).into())),
			Expr::Foreach(s) => crate::expr::Expr::Foreach(Box::new((*s).into())),
			Expr::Let(s) => crate::expr::Expr::Let(Box::new((*s).into())),
			Expr::Sleep(s) => crate::expr::Expr::Sleep(Box::new((*s).into())),
			Expr::Explain {
				format,
				analyze,
				statement,
			} => crate::expr::Expr::Explain {
				format: format.into(),
				analyze,
				statement: Box::new((*statement).into()),
			},
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
			crate::expr::Expr::Block(b) => Expr::Block(Box::new((*b).into())),
			crate::expr::Expr::Constant(c) => Expr::Constant(c.into()),
			crate::expr::Expr::Prefix {
				op,
				expr,
			} => Expr::Prefix {
				op: op.into(),
				expr: Box::new((*expr).into()),
			},
			crate::expr::Expr::Postfix {
				expr,
				op,
			} => Expr::Postfix {
				expr: Box::new((*expr).into()),
				op: op.into(),
			},

			crate::expr::Expr::Binary {
				left,
				op,
				right,
			} => Expr::Binary {
				left: Box::new((*left).into()),
				op: op.into(),
				right: Box::new((*right).into()),
			},
			crate::expr::Expr::FunctionCall(f) => Expr::FunctionCall(Box::new((*f).into())),
			crate::expr::Expr::Closure(s) => Expr::Closure(Box::new((*s).into())),
			crate::expr::Expr::Break => Expr::Break,
			crate::expr::Expr::Continue => Expr::Continue,
			crate::expr::Expr::Return(e) => Expr::Return(Box::new((*e).into())),
			crate::expr::Expr::Throw(e) => Expr::Throw(Box::new((*e).into())),
			crate::expr::Expr::IfElse(s) => Expr::IfElse(Box::new((*s).into())),
			crate::expr::Expr::Select(s) => Expr::Select(Box::new((*s).into())),
			crate::expr::Expr::Create(s) => Expr::Create(Box::new((*s).into())),
			crate::expr::Expr::Update(s) => Expr::Update(Box::new((*s).into())),
			crate::expr::Expr::Delete(s) => Expr::Delete(Box::new((*s).into())),
			crate::expr::Expr::Relate(s) => Expr::Relate(Box::new((*s).into())),
			crate::expr::Expr::Insert(s) => Expr::Insert(Box::new((*s).into())),
			crate::expr::Expr::Define(s) => Expr::Define(Box::new((*s).into())),
			crate::expr::Expr::Remove(s) => Expr::Remove(Box::new((*s).into())),
			crate::expr::Expr::Rebuild(s) => Expr::Rebuild(Box::new((*s).into())),
			crate::expr::Expr::Upsert(s) => Expr::Upsert(Box::new((*s).into())),
			crate::expr::Expr::Alter(s) => Expr::Alter(Box::new((*s).into())),
			crate::expr::Expr::Info(s) => Expr::Info(Box::new((*s).into())),
			crate::expr::Expr::Foreach(s) => Expr::Foreach(Box::new((*s).into())),
			crate::expr::Expr::Let(s) => Expr::Let(Box::new((*s).into())),
			crate::expr::Expr::Sleep(s) => Expr::Sleep(Box::new((*s).into())),
			crate::expr::Expr::Explain {
				format,
				analyze,
				statement,
			} => Expr::Explain {
				format: format.into(),
				analyze,
				statement: Box::new((*statement).into()),
			},
			// `Expr::Match` is only constructed by the GQL lowering at top level and
			// never enters a `sql::Ast`, the catalog, or `Revisioned` serialization
			// (which serializes `Expr` as SurrealQL text). It has no SurrealQL
			// surface, so this conversion is unreachable by construction; emit a
			// loud-but-non-panicking placeholder rather than a `sql::Match`.
			#[cfg(feature = "gql")]
			crate::expr::Expr::Match(_) => {
				tracing::error!(
					"Expr::Match reached the sql::Expr conversion; it must never enter a \
					 sql::Ast, the catalog, or Revisioned serialization"
				);
				debug_assert!(false, "Expr::Match must not be converted to sql::Expr");
				Expr::Literal(Literal::None)
			}
		}
	}
}
