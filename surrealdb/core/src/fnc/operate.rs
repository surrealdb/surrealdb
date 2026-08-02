//! The two operators that consult an index.
//!
//! `@@` and `<|k|>` are answered by the full-text or KNN index built for the
//! record being iterated, so they need the query executor the context carries.
//! Every other operator answers from its operands and lives one crate down;
//! they are re-exported here so a caller sees one module.

use anyhow::Result;
use reblessive::tree::Stk;
pub use surrealdb_runtime::operate::*;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Expr;
use crate::idx::planner::executor::QueryExecutor;
use crate::val::{RecordId, Value};

enum ExecutorOption<'a> {
	PreMatch,
	None,
	Execute(&'a QueryExecutor, &'a RecordId),
}

fn get_executor_and_thing<'a>(
	ctx: &'a FrozenContext,
	doc: &'a CursorDoc,
) -> Option<(&'a QueryExecutor, &'a RecordId)> {
	if let Some(thg) = &doc.rid {
		if let Some(exe) = ctx.get_query_executor()
			&& exe.is_table(&thg.table)
		{
			return Some((exe, thg.as_ref()));
		}
		if let Some(pla) = ctx.get_query_planner()
			&& let Some(exe) = pla.get_query_executor(&thg.table)
		{
			return Some((exe, thg));
		}
	}
	None
}

fn get_executor_option<'a>(
	ctx: &'a FrozenContext,
	doc: Option<&'a CursorDoc>,
	exp: &'a Expr,
) -> ExecutorOption<'a> {
	if let Some(doc) = doc
		&& let Some((exe, thg)) = get_executor_and_thing(ctx, doc)
	{
		if let Some(ir) = &doc.ir
			&& exe.is_iterator_expression(ir.irf(), exp)
		{
			return ExecutorOption::PreMatch;
		}
		return ExecutorOption::Execute(exe, thg);
	}
	ExecutorOption::None
}

pub(crate) async fn matches(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	exp: &Expr,
	l: Value,
	r: Value,
) -> Result<Value> {
	let res = match get_executor_option(ctx, doc, exp) {
		ExecutorOption::PreMatch => true,
		ExecutorOption::None => false,
		ExecutorOption::Execute(exe, thg) => exe.matches(stk, ctx, opt, thg, exp, l, r).await?,
	};
	Ok(res.into())
}

pub(crate) async fn knn(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	exp: &Expr,
) -> Result<Value> {
	match get_executor_option(ctx, doc, exp) {
		ExecutorOption::PreMatch => Ok(Value::Bool(true)),
		ExecutorOption::None => Ok(Value::Bool(false)),
		ExecutorOption::Execute(exe, thg) => exe.knn(stk, ctx, opt, thg, doc, exp).await,
	}
}
