use crate::err::Error;
use crate::sql::literal::Literal;
use crate::sql::value::Value;

pub fn or(a: Literal, b: Literal) -> Result<Literal, Error> {
	match a.as_bool() {
		true => Ok(a),
		false => Ok(b),
	}
}

pub fn and(a: Literal, b: Literal) -> Result<Literal, Error> {
	match a.as_bool() {
		true => match b.as_bool() {
			_ => Ok(b),
		},
		false => Ok(a),
	}
}

pub fn add(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	let a = a.as_number();
	let b = b.as_number();
	Ok(Literal::from(a + b))
}

pub fn sub(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	let a = a.as_number();
	let b = b.as_number();
	Ok(Literal::from(a - b))
}

pub fn mul(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	let a = a.as_number();
	let b = b.as_number();
	Ok(Literal::from(a * b))
}

pub fn div(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	let a = a.as_number();
	let b = b.as_number();
	Ok(Literal::from(a / b))
}

pub fn exact(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	Ok(Literal::from(a == b))
}

pub fn equal(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::None => Ok(Literal::from(b.is_none() == true)),
		Literal::Null => Ok(Literal::from(b.is_null() == true)),
		Literal::Void => Ok(Literal::from(b.is_void() == true)),
		Literal::True => Ok(Literal::from(b.is_true() == true)),
		Literal::False => Ok(Literal::from(b.is_false() == true)),
		Literal::Int(v) => Ok(Literal::from(v == &b.as_int())),
		Literal::Float(v) => Ok(Literal::from(v == &b.as_float())),
		Literal::Thing(v) => match b {
			Literal::Thing(w) => Ok(Literal::from(v == w)),
			_ => Ok(Literal::True),
		},
		Literal::Regex(v) => match b {
			Literal::Regex(w) => Ok(Literal::from(v == w)),
			Literal::Strand(w) => match v.value {
				Some(ref r) => Ok(Literal::from(r.is_match(w.value.as_str()) == true)),
				None => Ok(Literal::False),
			},
			_ => Ok(Literal::False),
		},
		Literal::Point(v) => match b {
			Literal::Point(w) => Ok(Literal::from(v == w)),
			_ => Ok(Literal::False),
		},
		Literal::Array(v) => match b {
			Literal::Array(w) => Ok(Literal::from(v == w)),
			_ => Ok(Literal::False),
		},
		Literal::Object(v) => match b {
			Literal::Object(w) => Ok(Literal::from(v == w)),
			_ => Ok(Literal::False),
		},
		Literal::Strand(v) => match b {
			Literal::Strand(w) => Ok(Literal::from(v == w)),
			Literal::Regex(w) => match w.value {
				Some(ref r) => Ok(Literal::from(r.is_match(v.value.as_str()) == true)),
				None => Ok(Literal::False),
			},
			_ => Ok(Literal::from(v == &b.as_strand())),
		},
		Literal::Number(v) => Ok(Literal::from(v == &b.as_number())),
		Literal::Polygon(v) => match b {
			Literal::Polygon(w) => Ok(Literal::from(v == w)),
			_ => Ok(Literal::False),
		},
		Literal::Duration(v) => match b {
			Literal::Duration(w) => Ok(Literal::from(v == w)),
			_ => Ok(Literal::False),
		},
		Literal::Datetime(v) => match b {
			Literal::Datetime(w) => Ok(Literal::from(v == w)),
			_ => Ok(Literal::False),
		},
		_ => unreachable!(),
	}
}

pub fn not_equal(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::None => Ok(Literal::from(b.is_none() != true)),
		Literal::Null => Ok(Literal::from(b.is_null() != true)),
		Literal::Void => Ok(Literal::from(b.is_void() != true)),
		Literal::True => Ok(Literal::from(b.is_true() != true)),
		Literal::False => Ok(Literal::from(b.is_false() != true)),
		Literal::Int(v) => Ok(Literal::from(v != &b.as_int())),
		Literal::Float(v) => Ok(Literal::from(v != &b.as_float())),
		Literal::Thing(v) => match b {
			Literal::Thing(w) => Ok(Literal::from(v != w)),
			_ => Ok(Literal::True),
		},
		Literal::Regex(v) => match b {
			Literal::Regex(w) => Ok(Literal::from(v != w)),
			Literal::Strand(w) => match v.value {
				Some(ref r) => Ok(Literal::from(r.is_match(w.value.as_str()) != true)),
				None => Ok(Literal::True),
			},
			_ => Ok(Literal::True),
		},
		Literal::Point(v) => match b {
			Literal::Point(w) => Ok(Literal::from(v != w)),
			_ => Ok(Literal::True),
		},
		Literal::Array(v) => match b {
			Literal::Array(w) => Ok(Literal::from(v != w)),
			_ => Ok(Literal::True),
		},
		Literal::Object(v) => match b {
			Literal::Object(w) => Ok(Literal::from(v != w)),
			_ => Ok(Literal::True),
		},
		Literal::Number(v) => match b {
			Literal::Number(w) => Ok(Literal::from(v != w)),
			_ => Ok(Literal::True),
		},
		Literal::Strand(v) => match b {
			Literal::Strand(w) => Ok(Literal::from(v != w)),
			Literal::Regex(w) => match w.value {
				Some(ref r) => Ok(Literal::from(r.is_match(v.value.as_str()) != true)),
				None => Ok(Literal::False),
			},
			_ => Ok(Literal::from(v != &b.as_strand())),
		},
		Literal::Polygon(v) => match b {
			Literal::Polygon(w) => Ok(Literal::from(v != w)),
			_ => Ok(Literal::True),
		},
		Literal::Duration(v) => match b {
			Literal::Duration(w) => Ok(Literal::from(v != w)),
			_ => Ok(Literal::True),
		},
		Literal::Datetime(v) => match b {
			Literal::Datetime(w) => Ok(Literal::from(v != w)),
			_ => Ok(Literal::True),
		},
		_ => unreachable!(),
	}
}

pub fn all_equal(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::Array(ref v) => match v.value.iter().all(|x| match x {
			Value::Literal(v) => equal(v, b).is_ok(),
			_ => unreachable!(),
		}) {
			true => Ok(Literal::True),
			false => Ok(Literal::False),
		},
		_ => equal(a, b),
	}
}

pub fn any_equal(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::Array(ref v) => match v.value.iter().any(|x| match x {
			Value::Literal(v) => equal(v, b).is_ok(),
			_ => unreachable!(),
		}) {
			true => Ok(Literal::True),
			false => Ok(Literal::False),
		},
		_ => equal(a, b),
	}
}

pub fn like(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	todo!()
}

pub fn not_like(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	todo!()
}

pub fn all_like(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	todo!()
}

pub fn any_like(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	todo!()
}

pub fn less_than(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a.lt(&b) {
		true => Ok(Literal::True),
		false => Ok(Literal::False),
	}
}

pub fn less_than_or_equal(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a.le(&b) {
		true => Ok(Literal::True),
		false => Ok(Literal::False),
	}
}

pub fn more_than(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a.gt(&b) {
		true => Ok(Literal::True),
		false => Ok(Literal::False),
	}
}

pub fn more_than_or_equal(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a.ge(&b) {
		true => Ok(Literal::True),
		false => Ok(Literal::False),
	}
}

pub fn contain(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn not_contain(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn contain_all(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn contain_some(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn contain_none(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match a {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn inside(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match b {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn not_inside(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match b {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn inside_all(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match b {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn inside_some(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match b {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn inside_none(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match b {
		Literal::Array(v) => todo!(),
		Literal::Strand(v) => todo!(),
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

pub fn intersects(a: &Literal, b: &Literal) -> Result<Literal, Error> {
	match b {
		Literal::Polygon(v) => todo!(),
		_ => Ok(Literal::False),
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn or_true() {
		let one = Literal::from(1);
		let two = Literal::from(2);
		let res = or(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn or_false_one() {
		let one = Literal::from(0);
		let two = Literal::from(1);
		let res = or(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn or_false_two() {
		let one = Literal::from(1);
		let two = Literal::from(0);
		let res = or(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn and_true() {
		let one = Literal::from(1);
		let two = Literal::from(2);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("2", format!("{}", out));
	}

	#[test]
	fn and_false_one() {
		let one = Literal::from(0);
		let two = Literal::from(1);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("0", format!("{}", out));
	}

	#[test]
	fn and_false_two() {
		let one = Literal::from(1);
		let two = Literal::from(0);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("0", format!("{}", out));
	}

	#[test]
	fn add_basic() {
		let one = Literal::from(5);
		let two = Literal::from(4);
		let res = add(&one, &two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("9", format!("{}", out));
	}

	#[test]
	fn sub_basic() {
		let one = Literal::from(5);
		let two = Literal::from(4);
		let res = sub(&one, &two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn mul_basic() {
		let one = Literal::from(5);
		let two = Literal::from(4);
		let res = mul(&one, &two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("20", format!("{}", out));
	}

	#[test]
	fn div_basic() {
		let one = Literal::from(5);
		let two = Literal::from(4);
		let res = div(&one, &two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1.25", format!("{}", out));
	}
}
