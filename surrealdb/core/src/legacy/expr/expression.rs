use std::ops::Bound;

use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::{NoWriteFrame, Options};
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::expr::expression::Expr;
use crate::expr::{
	BinaryOperator, ControlFlow, Error as ExprError, FlowResult, PostfixOperator, PrefixOperator,
};
use crate::fnc;
use crate::val::{Array, Range, Value};

/// Process this type returning a computed simple Value
pub(crate) async fn expr_compute(
	this: &Expr,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	let opt = opt.dive(1).map_err(anyhow::Error::new)?;

	// A stored expression that a *read* evaluates may not modify data: a SELECT
	// PERMISSIONS predicate runs with permission enforcement disabled
	// (GHSA-66r2-5gwj-gxm2), and a COMPUTED body runs under the definer's auth
	// on every read of the field. This catches writes reached directly, through
	// nested subqueries, or through function/closure bodies, on both the legacy
	// and streaming execution paths — the streaming engine plans no writes of
	// its own, so every one of them arrives here.
	//
	// This is the enforcement point for both rules. A definition-time check
	// cannot stand in for it: whether a called function writes may depend on
	// which branch its arguments select, so only reaching the statement settles
	// it. create/update/delete PERMISSIONS clauses are reached from a write and
	// carry no such frame, so they never land here.
	if let Some(frame) = opt.no_write
		&& matches!(
			this,
			Expr::Create(_)
				| Expr::Update(_)
				| Expr::Upsert(_)
				| Expr::Delete(_)
				| Expr::Relate(_)
				| Expr::Insert(_)
				| Expr::Define(_)
				| Expr::Remove(_)
				| Expr::Rebuild(_)
				| Expr::Alter(_)
		) {
		let err = match frame {
			NoWriteFrame::PermissionPredicate => {
				// Point the operator at the sanctioned mechanism for write side
				// effects driven by a read.
				warn!(
					"A SELECT PERMISSIONS clause attempted to modify data and was blocked \
					 (GHSA-66r2-5gwj-gxm2). Move audit-style side effects to a DEFINE EVENT."
				);
				ExecError::PermissionPredicateSideEffect
			}
			NoWriteFrame::ComputedField => ExecError::ComputedFieldSideEffect,
		};
		return Err(ControlFlow::Err(anyhow::Error::new(err)));
	}

	match this {
		Expr::Literal(literal) => {
			crate::legacy::literal_compute(literal, stk, ctx, &opt, doc).await
		}
		Expr::Param(param) => {
			crate::legacy::param_compute(param, stk, ctx, &opt, doc).await.map_err(ControlFlow::Err)
		}
		Expr::Idiom(idiom) => crate::legacy::idiom_compute(idiom, stk, ctx, &opt, doc).await,
		Expr::Table(ident) => Ok(Value::Table(ident.clone())),
		Expr::Mock(mock) => {
			// NOTE(value pr): This is a breaking change but makes the most sense without
			// having mock be part of the Value type.
			// Mock is mostly used within `CREATE |thing:20|` to create a bunch of entries
			// at one. Here it behaves similar to `CREATE
			// ([thing:1,thing:2,thing:3...])` so when we encounted mock outside of
			// create we return the array here instead.
			//

			let iter = mock.clone().into_iter();
			if iter
				.size_hint()
				.1
				.map(|x| {
					x.saturating_mul(std::mem::size_of::<Value>())
						> ctx.config.exec.generation_allocation_limit
				})
				.unwrap_or(true)
			{
				return Err(ControlFlow::Err(anyhow::Error::msg(
					"Mock range exceeds allocation limit",
				)));
			}
			let record_ids = iter.map(Value::RecordId).collect();
			Ok(Value::Array(Array(record_ids)))
		}
		Expr::Block(block) => crate::legacy::block_compute(block, stk, ctx, &opt, doc).await,
		Expr::Constant(constant) => Ok(constant.compute()),
		Expr::Prefix {
			op,
			expr,
		} => crate::legacy::expr_compute_prefix(stk, ctx, &opt, doc, op, expr).await,
		Expr::Postfix {
			expr,
			op,
		} => crate::legacy::expr_compute_postfix(stk, ctx, &opt, doc, expr, op).await,
		Expr::Binary {
			..
		} => crate::legacy::expr_compute_binary(stk, ctx, &opt, doc, this).await,
		Expr::FunctionCall(function_call) => {
			crate::legacy::function_call_compute(function_call, stk, ctx, &opt, doc).await
		}
		Expr::Closure(closure) => Ok(crate::legacy::closure_expr_compute(closure, ctx).await?),
		Expr::Break => Err(ControlFlow::Break),
		Expr::Continue => Err(ControlFlow::Continue),
		Expr::Return(output_statement) => {
			crate::legacy::output_statement_compute(output_statement, stk, ctx, &opt, doc).await
		}
		Expr::Throw(expr) => {
			let res = stk.run(|stk| crate::legacy::expr_compute(expr, stk, ctx, &opt, doc)).await?;
			Err(ControlFlow::Err(anyhow::Error::new(ExecError::Thrown(res.to_raw_string()))))
		}
		Expr::IfElse(ifelse_statement) => {
			crate::legacy::ifelse_statement_compute(ifelse_statement, stk, ctx, &opt, doc).await
		}
		Expr::Select(select_statement) => {
			crate::legacy::select_statement_compute(select_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Create(create_statement) => {
			crate::legacy::create_statement_compute(create_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Update(update_statement) => {
			crate::legacy::update_statement_compute(update_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Delete(delete_statement) => {
			crate::legacy::delete_statement_compute(delete_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Relate(relate_statement) => {
			crate::legacy::relate_statement_compute(relate_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Insert(insert_statement) => {
			crate::legacy::insert_statement_compute(insert_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Define(define_statement) => {
			crate::legacy::define_statement_compute(define_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Remove(remove_statement) => {
			crate::legacy::remove_statement_compute(remove_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Rebuild(rebuild_statement) => {
			crate::legacy::rebuild_statement_compute(rebuild_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Upsert(upsert_statement) => {
			crate::legacy::upsert_statement_compute(upsert_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Alter(alter_statement) => {
			crate::legacy::alter_statement_compute(alter_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Info(info_statement) => {
			crate::legacy::info_statement_compute(info_statement, stk, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Foreach(foreach_statement) => {
			crate::legacy::foreach_statement_compute(foreach_statement, stk, ctx, &opt, doc).await
		}
		Expr::Let(_) => Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidStatement(
			"LET statements can only appear at the top level of a query or inside a block \
				 expression"
				.to_string(),
		)))),
		Expr::Sleep(sleep_statement) => {
			crate::legacy::sleep_statement_compute(sleep_statement, ctx, &opt, doc)
				.await
				.map_err(ControlFlow::Err)
		}
		Expr::Explain {
			..
		} => Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidStatement(
			"EXPLAIN is only supported with the new execution model".to_string(),
		)))),
		Expr::Match(_) => Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidStatement(
			"GQL MATCH requires the streaming execution engine; it cannot run under the \
				 compute-only planner strategy"
				.to_string(),
		)))),
	}
}

pub(crate) async fn expr_compute_prefix(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	op: &PrefixOperator,
	expr: &Expr,
) -> FlowResult<Value> {
	let res = stk.run(|stk| crate::legacy::expr_compute(expr, stk, ctx, opt, doc)).await?;

	match op {
		PrefixOperator::Not => fnc::operate::not(res).map_err(ControlFlow::Err),
		PrefixOperator::Positive => Ok(res),
		PrefixOperator::Negate => fnc::operate::neg(res).map_err(ControlFlow::Err),
		PrefixOperator::Range => Ok(Value::Range(Box::new(Range {
			start: Bound::Unbounded,
			end: Bound::Excluded(res),
		}))),
		PrefixOperator::RangeInclusive => Ok(Value::Range(Box::new(Range {
			start: Bound::Unbounded,
			end: Bound::Included(res),
		}))),
		PrefixOperator::Cast(kind) => res
			.cast_to_kind(kind)
			.map_err(ExprError::from)
			.map_err(anyhow::Error::new)
			.map_err(ControlFlow::Err),
	}
}

pub(crate) async fn expr_compute_postfix(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
	op: &PostfixOperator,
) -> FlowResult<Value> {
	let res = stk.run(|stk| crate::legacy::expr_compute(expr, stk, ctx, opt, doc)).await?;
	match op {
		PostfixOperator::Range => Ok(Value::Range(Box::new(Range {
			start: Bound::Included(res),
			end: Bound::Unbounded,
		}))),
		PostfixOperator::RangeSkip => Ok(Value::Range(Box::new(Range {
			start: Bound::Excluded(res),
			end: Bound::Unbounded,
		}))),
		PostfixOperator::MethodCall(name, exprs) => {
			let mut args = Vec::new();

			for e in exprs.iter() {
				args.push(stk.run(|stk| crate::legacy::expr_compute(e, stk, ctx, opt, doc)).await?);
			}

			if let Value::Object(ref x) = res
				&& let Some(Value::Closure(x)) = x.get(name.as_str())
			{
				return crate::legacy::closure_invoke(x, stk, ctx, opt, doc, args)
					.await
					.map_err(ControlFlow::Err);
			};
			fnc::idiom(stk, ctx, opt, doc, res, name, args).await.map_err(ControlFlow::Err)
		}
		PostfixOperator::Call(exprs) => {
			let mut args = Vec::new();

			for e in exprs.iter() {
				args.push(stk.run(|stk| crate::legacy::expr_compute(e, stk, ctx, opt, doc)).await?);
			}

			if let Value::Closure(x) = res {
				crate::legacy::closure_invoke(&x, stk, ctx, opt, doc, args)
					.await
					.map_err(ControlFlow::Err)
			} else {
				Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidFunction {
					name: "ANONYMOUS".to_string(),
					message: format!("'{}' is not a function", res.kind_of()),
				})))
			}
		}
	}
}

pub(crate) async fn expr_compute_binary(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
) -> FlowResult<Value> {
	// NOTE: The structure here is somewhat convoluted, because knn needs to have
	// access to the expression itself instead of just the op and left/right
	// expressions we need to pass in the parent expression when encountering a
	// binary expression and then match again here. Ideally knn should be able to
	// be called more naturally.
	let Expr::Binary {
		left,
		op,
		right,
	} = expr
	else {
		unreachable!()
	};

	if let BinaryOperator::NearestNeighbor(_) = op {
		return fnc::operate::knn(stk, ctx, opt, doc, expr).await.map_err(ControlFlow::Err);
	}

	let left = stk.run(|stk| crate::legacy::expr_compute(left, stk, ctx, opt, doc)).await?;

	let res = match op {
		BinaryOperator::Subtract => fnc::operate::sub(
			left,
			stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Add => fnc::operate::add(
			left,
			stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Multiply => fnc::operate::mul(
			left,
			stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Divide => fnc::operate::div(
			left,
			stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Remainder => fnc::operate::rem(
			left,
			stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Power => fnc::operate::pow(
			left,
			stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Equal => fnc::operate::equal(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::ExactEqual => fnc::operate::exact(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::NotEqual => fnc::operate::not_equal(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::AllEqual => fnc::operate::all_equal(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::AnyEqual => fnc::operate::any_equal(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Or | BinaryOperator::TenaryCondition => {
			if left.is_truthy() {
				return Ok(left);
			}
			return stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await;
		}
		BinaryOperator::And => {
			if !left.is_truthy() {
				return Ok(left);
			}
			return stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await;
		}
		BinaryOperator::NullCoalescing => {
			if !left.is_nullish() {
				return Ok(left);
			}
			return stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await;
		}
		BinaryOperator::LessThan => fnc::operate::less_than(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::LessThanEqual => fnc::operate::less_than_or_equal(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::MoreThan => fnc::operate::more_than(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::MoreThanEqual => fnc::operate::more_than_or_equal(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Contain => fnc::operate::contain(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::NotContain => fnc::operate::not_contain(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::ContainAll => fnc::operate::contain_all(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::ContainAny => fnc::operate::contain_any(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::ContainNone => fnc::operate::contain_none(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Inside => fnc::operate::inside(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::NotInside => fnc::operate::not_inside(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::AllInside => fnc::operate::inside_all(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::AnyInside => fnc::operate::inside_any(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::NoneInside => fnc::operate::inside_none(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Outside => fnc::operate::outside(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Intersects => fnc::operate::intersects(
			&left,
			&stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?,
		),
		BinaryOperator::Matches(_) => {
			let right =
				stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?;
			fnc::operate::matches(stk, ctx, opt, doc, expr, left, right).await
		}
		BinaryOperator::NearestNeighbor(_) => unreachable!(),
		BinaryOperator::Range => {
			let right =
				stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?;
			Ok(Value::Range(Box::new(Range {
				start: Bound::Included(left),
				end: Bound::Excluded(right),
			})))
		}
		BinaryOperator::RangeInclusive => {
			let right =
				stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?;
			Ok(Value::Range(Box::new(Range {
				start: Bound::Included(left),
				end: Bound::Included(right),
			})))
		}
		BinaryOperator::RangeSkip => {
			let right =
				stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?;
			Ok(Value::Range(Box::new(Range {
				start: Bound::Excluded(left),
				end: Bound::Excluded(right),
			})))
		}
		BinaryOperator::RangeSkipInclusive => {
			let right =
				stk.run(|stk| crate::legacy::expr_compute(right, stk, ctx, opt, doc)).await?;
			Ok(Value::Range(Box::new(Range {
				start: Bound::Excluded(left),
				end: Bound::Included(right),
			})))
		}
	};

	res.map_err(ControlFlow::Err)
}
