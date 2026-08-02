//! SurrealQL's operators.
//!
//! Every operator here answers from its operands alone. The two that do
//! not — `@@` and `<|k|>`, which consult the index built for the record
//! being iterated — live one crate up, beside that index.

use anyhow::Result;
use surrealdb_expr::val::{TryAdd, TryDiv, TryMul, TryNeg, TryPow, TryRem, TrySub, Value};

pub fn neg(a: Value) -> Result<Value> {
	a.try_neg()
}

pub fn not(a: Value) -> Result<Value> {
	super::not::not((a,))
}

pub fn add(a: Value, b: Value) -> Result<Value> {
	a.try_add(b)
}

pub fn sub(a: Value, b: Value) -> Result<Value> {
	a.try_sub(b)
}

pub fn mul(a: Value, b: Value) -> Result<Value> {
	a.try_mul(b)
}

pub fn div(a: Value, b: Value) -> Result<Value> {
	Ok(a.try_div(b).unwrap_or(f64::NAN.into()))
}

pub fn rem(a: Value, b: Value) -> Result<Value> {
	a.try_rem(b)
}

pub fn pow(a: Value, b: Value) -> Result<Value> {
	a.try_pow(b)
}

pub fn exact(a: &Value, b: &Value) -> Result<Value> {
	Ok(Value::from(a == b))
}

pub fn equal(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.equal(b).into())
}

pub fn not_equal(a: &Value, b: &Value) -> Result<Value> {
	Ok((!a.equal(b)).into())
}

pub fn all_equal(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.all_equal(b).into())
}

pub fn any_equal(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.any_equal(b).into())
}

pub fn less_than(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.lt(b).into())
}

pub fn less_than_or_equal(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.le(b).into())
}

pub fn more_than(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.gt(b).into())
}

pub fn more_than_or_equal(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.ge(b).into())
}

pub fn contain(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.contains(b).into())
}

pub fn not_contain(a: &Value, b: &Value) -> Result<Value> {
	Ok((!a.contains(b)).into())
}

pub fn contain_all(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.contains_all(b).into())
}

pub fn contain_any(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.contains_any(b).into())
}

pub fn contain_none(a: &Value, b: &Value) -> Result<Value> {
	Ok((!a.contains_any(b)).into())
}

pub fn inside(a: &Value, b: &Value) -> Result<Value> {
	Ok(b.contains(a).into())
}

pub fn not_inside(a: &Value, b: &Value) -> Result<Value> {
	Ok((!b.contains(a)).into())
}

pub fn inside_all(a: &Value, b: &Value) -> Result<Value> {
	Ok(b.contains_all(a).into())
}

pub fn inside_any(a: &Value, b: &Value) -> Result<Value> {
	Ok(b.contains_any(a).into())
}

pub fn inside_none(a: &Value, b: &Value) -> Result<Value> {
	Ok((!b.contains_any(a)).into())
}

pub fn outside(a: &Value, b: &Value) -> Result<Value> {
	Ok((!a.intersects(b)).into())
}

pub fn intersects(a: &Value, b: &Value) -> Result<Value> {
	Ok(a.intersects(b).into())
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn add_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = add(one, two);
		let out = res.unwrap();
		assert_eq!(out, Value::from(9));
	}

	#[test]
	fn sub_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = sub(one, two);
		let out = res.unwrap();
		assert_eq!(out, Value::from(1));
	}

	#[test]
	fn mul_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = mul(one, two);
		let out = res.unwrap();
		assert_eq!(out, Value::from(20));
	}

	#[test]
	fn div_int() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = div(one, two);
		let out = res.unwrap();
		assert_eq!(out, Value::from(1));
	}

	#[test]
	fn div_float() {
		let one = Value::from(5.0);
		let two = Value::from(4.0);
		let res = div(one, two);
		let out = res.unwrap();
		assert_eq!(out, Value::from(1.25_f64));
	}
}
