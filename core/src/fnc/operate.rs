use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::sql::value::TryRem;
use crate::sql::value::{TryAdd, TryDiv, TryMul, TryNeg, TryPow, TrySub, Value};
use crate::sql::{Expression, Thing};
use reblessive::tree::Stk;

pub fn neg(a: Value) -> Result<Value, Error> {
	a.try_neg()
}

pub fn not(a: Value) -> Result<Value, Error> {
	super::not::not((a,))
}

pub fn or(a: Value, b: Value) -> Result<Value, Error> {
	Ok(match a.is_truthy() {
		true => a,
		false => b,
	})
}

pub fn and(a: Value, b: Value) -> Result<Value, Error> {
	Ok(match a.is_truthy() {
		true => b,
		false => a,
	})
}

pub fn tco(a: Value, b: Value) -> Result<Value, Error> {
	Ok(match a.is_truthy() {
		true => a,
		false => b,
	})
}

pub fn nco(a: Value, b: Value) -> Result<Value, Error> {
	Ok(match a.is_some() {
		true => a,
		false => b,
	})
}

pub fn add(a: Value, b: Value) -> Result<Value, Error> {
	a.try_add(b)
}

pub fn sub(a: Value, b: Value) -> Result<Value, Error> {
	a.try_sub(b)
}

pub fn mul(a: Value, b: Value) -> Result<Value, Error> {
	a.try_mul(b)
}

pub fn div(a: Value, b: Value) -> Result<Value, Error> {
	a.try_div(b)
}

pub fn rem(a: Value, b: Value) -> Result<Value, Error> {
	a.try_rem(b)
}

pub fn pow(a: Value, b: Value) -> Result<Value, Error> {
	a.try_pow(b)
}

pub fn exact(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(Value::from(a == b))
}

pub fn equal(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.equal(b).into())
}

pub fn not_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((!a.equal(b)).into())
}

pub fn all_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.all_equal(b).into())
}

pub fn any_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.any_equal(b).into())
}

pub fn like(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.fuzzy(b).into())
}

pub fn not_like(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((!a.fuzzy(b)).into())
}

pub fn all_like(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.all_fuzzy(b).into())
}

pub fn any_like(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.any_fuzzy(b).into())
}

pub fn less_than(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((a.is_none_or_null() || b.is_none_or_null() || a.lt(b)).into())
}

pub fn less_than_or_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((a.is_none_or_null() || b.is_none_or_null() || a.le(b)).into())
}

pub fn more_than(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((a.is_none_or_null() || b.is_none_or_null() || a.gt(b)).into())
}

pub fn more_than_or_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((a.is_none_or_null() || b.is_none_or_null() || a.ge(b)).into())
}

pub fn contain(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.contains(b).into())
}

pub fn not_contain(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((!a.contains(b)).into())
}

pub fn contain_all(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.contains_all(b).into())
}

pub fn contain_any(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.contains_any(b).into())
}

pub fn contain_none(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((!a.contains_any(b)).into())
}

pub fn inside(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(b.contains(a).into())
}

pub fn not_inside(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((!b.contains(a)).into())
}

pub fn inside_all(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(b.contains_all(a).into())
}

pub fn inside_any(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(b.contains_any(a).into())
}

pub fn inside_none(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((!b.contains_any(a)).into())
}

pub fn outside(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok((!a.intersects(b)).into())
}

pub fn intersects(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.intersects(b).into())
}

enum ExecutorOption<'a> {
	PreMatch,
	None,
	Execute(&'a QueryExecutor, &'a Thing),
}

fn get_executor_and_thing<'a>(
	ctx: &'a Context<'_>,
	doc: &'a CursorDoc,
) -> Option<(&'a QueryExecutor, &'a Thing)> {
	if let Some(thg) = doc.rid {
		if let Some(exe) = ctx.get_query_executor() {
			if exe.is_table(&thg.tb) {
				return Some((exe, thg));
			}
		}
		if let Some(pla) = ctx.get_query_planner() {
			if let Some(exe) = pla.get_query_executor(&thg.tb) {
				return Some((exe, thg));
			}
		}
	}
	None
}

fn get_executor_option<'a>(
	ctx: &'a Context<'_>,
	doc: Option<&'a CursorDoc<'_>>,
	exp: &'a Expression,
) -> ExecutorOption<'a> {
	if let Some(doc) = doc {
		if let Some((exe, thg)) = get_executor_and_thing(ctx, doc) {
			if let Some(ir) = doc.ir {
				if exe.is_iterator_expression(ir.irf(), exp) {
					return ExecutorOption::PreMatch;
				}
			}
			return ExecutorOption::Execute(exe, thg);
		}
	}
	ExecutorOption::None
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn matches(
	stk: &mut Stk,
	ctx: &Context<'_>,
	opt: &Options,
	txn: &Transaction,
	doc: Option<&CursorDoc<'_>>,
	exp: &Expression,
	l: Value,
	r: Value,
) -> Result<Value, Error> {
	let res = match get_executor_option(ctx, doc, exp) {
		ExecutorOption::PreMatch => true,
		ExecutorOption::None => false,
		ExecutorOption::Execute(exe, thg) => {
			exe.matches(stk, ctx, opt, txn, thg, exp, l, r).await?
		}
	};
	Ok(res.into())
}

pub(crate) async fn knn(
	stk: &mut Stk,
	ctx: &Context<'_>,
	opt: &Options,
	txn: &Transaction,
	doc: Option<&CursorDoc<'_>>,
	exp: &Expression,
) -> Result<Value, Error> {
	match get_executor_option(ctx, doc, exp) {
		ExecutorOption::PreMatch => Ok(Value::Bool(true)),
		ExecutorOption::None => Ok(Value::Bool(false)),
		ExecutorOption::Execute(exe, thg) => exe.knn(stk, ctx, opt, txn, thg, doc, exp).await,
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn or_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = or(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn or_false_one() {
		let one = Value::from(0);
		let two = Value::from(1);
		let res = or(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn or_false_two() {
		let one = Value::from(1);
		let two = Value::from(0);
		let res = or(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn and_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = and(one, two);
		let out = res.unwrap();
		assert_eq!("2", format!("{}", out));
	}

	#[test]
	fn and_false_one() {
		let one = Value::from(0);
		let two = Value::from(1);
		let res = and(one, two);
		let out = res.unwrap();
		assert_eq!("0", format!("{}", out));
	}

	#[test]
	fn and_false_two() {
		let one = Value::from(1);
		let two = Value::from(0);
		let res = and(one, two);
		let out = res.unwrap();
		assert_eq!("0", format!("{}", out));
	}

	#[test]
	fn tco_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = tco(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn tco_false_one() {
		let one = Value::from(0);
		let two = Value::from(1);
		let res = tco(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn tco_false_two() {
		let one = Value::from(1);
		let two = Value::from(0);
		let res = tco(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn nco_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = nco(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn nco_false_one() {
		let one = Value::None;
		let two = Value::from(1);
		let res = nco(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn nco_false_two() {
		let one = Value::from(1);
		let two = Value::None;
		let res = nco(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn add_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = add(one, two);
		let out = res.unwrap();
		assert_eq!("9", format!("{}", out));
	}

	#[test]
	fn sub_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = sub(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn mul_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = mul(one, two);
		let out = res.unwrap();
		assert_eq!("20", format!("{}", out));
	}

	#[test]
	fn div_int() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = div(one, two);
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn div_float() {
		let one = Value::from(5.0);
		let two = Value::from(4.0);
		let res = div(one, two);
		let out = res.unwrap();
		assert_eq!("1.25f", format!("{}", out));
	}
}
