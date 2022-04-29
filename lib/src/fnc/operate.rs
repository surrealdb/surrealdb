use crate::err::Error;
use crate::sql::value::Value;
use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Sub;

pub fn or(a: Value, b: Value) -> Result<Value, Error> {
	match a.is_truthy() {
		true => Ok(a),
		false => Ok(b),
	}
}

pub fn and(a: Value, b: Value) -> Result<Value, Error> {
	match a.is_truthy() {
		true => Ok(b),
		false => Ok(a),
	}
}

pub fn add(a: Value, b: Value) -> Result<Value, Error> {
	Ok(a.add(b))
}

pub fn sub(a: Value, b: Value) -> Result<Value, Error> {
	Ok(a.sub(b))
}

pub fn mul(a: Value, b: Value) -> Result<Value, Error> {
	Ok(a.mul(b))
}

pub fn div(a: Value, b: Value) -> Result<Value, Error> {
	Ok(a.div(b))
}

pub fn exact(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(Value::from(a == b))
}

pub fn equal(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.equal(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn not_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.equal(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn all_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.all_equal(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn any_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.any_equal(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn like(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.fuzzy(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn not_like(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.fuzzy(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn all_like(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.all_fuzzy(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn any_like(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.any_fuzzy(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn less_than(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.lt(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn less_than_or_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.le(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn more_than(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.gt(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn more_than_or_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.ge(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn contain(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.contains(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn not_contain(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.contains(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn contain_all(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.contains_all(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn contain_any(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.contains_any(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn contain_none(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.contains_any(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn inside(a: &Value, b: &Value) -> Result<Value, Error> {
	match b.contains(a) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn not_inside(a: &Value, b: &Value) -> Result<Value, Error> {
	match b.contains(a) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn inside_all(a: &Value, b: &Value) -> Result<Value, Error> {
	match b.contains_all(a) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn inside_any(a: &Value, b: &Value) -> Result<Value, Error> {
	match b.contains_any(a) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn inside_none(a: &Value, b: &Value) -> Result<Value, Error> {
	match b.contains_any(a) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn outside(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.intersects(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn intersects(a: &Value, b: &Value) -> Result<Value, Error> {
	match a.intersects(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
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
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn or_false_one() {
		let one = Value::from(0);
		let two = Value::from(1);
		let res = or(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn or_false_two() {
		let one = Value::from(1);
		let two = Value::from(0);
		let res = or(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn and_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("2", format!("{}", out));
	}

	#[test]
	fn and_false_one() {
		let one = Value::from(0);
		let two = Value::from(1);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("0", format!("{}", out));
	}

	#[test]
	fn and_false_two() {
		let one = Value::from(1);
		let two = Value::from(0);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("0", format!("{}", out));
	}

	#[test]
	fn add_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = add(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("9", format!("{}", out));
	}

	#[test]
	fn sub_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = sub(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn mul_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = mul(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("20", format!("{}", out));
	}

	#[test]
	fn div_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = div(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1.25", format!("{}", out));
	}
}
