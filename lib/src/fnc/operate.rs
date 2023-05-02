use crate::err::Error;
use crate::sql::value::TryAdd;
use crate::sql::value::TryDiv;
use crate::sql::value::TryMul;
use crate::sql::value::TryPow;
use crate::sql::value::TrySub;
use crate::sql::value::Value;

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
	Ok(a.lt(b).into())
}

pub fn less_than_or_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.le(b).into())
}

pub fn more_than(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.gt(b).into())
}

pub fn more_than_or_equal(a: &Value, b: &Value) -> Result<Value, Error> {
	Ok(a.ge(b).into())
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
	fn tco_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = tco(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn tco_false_one() {
		let one = Value::from(0);
		let two = Value::from(1);
		let res = tco(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn tco_false_two() {
		let one = Value::from(1);
		let two = Value::from(0);
		let res = tco(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn nco_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = nco(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn nco_false_one() {
		let one = Value::None;
		let two = Value::from(1);
		let res = nco(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn nco_false_two() {
		let one = Value::from(1);
		let two = Value::None;
		let res = nco(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
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
