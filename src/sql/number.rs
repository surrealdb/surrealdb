use dec::Decimal;
use nom::number::complete::double;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Number {
	pub value: Decimal,
}

impl Default for Number {
	fn default() -> Self {
		Number { value: 0i32.into() }
	}
}

impl From<i32> for Number {
	fn from(i: i32) -> Self {
		Number { value: i.into() }
	}
}

impl From<i64> for Number {
	fn from(i: i64) -> Self {
		Number { value: i.into() }
	}
}

impl From<f32> for Number {
	fn from(f: f32) -> Self {
		Number {
			value: Decimal::from_str(&f.to_string()).unwrap(),
		}
	}
}

impl From<f64> for Number {
	fn from(f: f64) -> Self {
		Number {
			value: Decimal::from_str(&f.to_string()).unwrap(),
		}
	}
}

impl<'a> From<&'a str> for Number {
	fn from(s: &str) -> Self {
		Number {
			value: Decimal::from_str(s).unwrap(),
		}
	}
}

impl From<String> for Number {
	fn from(s: String) -> Self {
		Number {
			value: Decimal::from_str(&s).unwrap(),
		}
	}
}

impl fmt::Display for Number {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.value)
	}
}

pub fn number(i: &str) -> IResult<&str, Number> {
	let (i, v) = double(i)?;
	Ok((i, Number::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn number_integer() {
		let sql = "123";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("123", format!("{}", out));
		assert_eq!(out, Number::from(123));
	}

	#[test]
	fn number_integer_neg() {
		let sql = "-123";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("-123", format!("{}", out));
		assert_eq!(out, Number::from(-123));
	}

	#[test]
	fn number_decimal() {
		let sql = "123.45";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("123.45", format!("{}", out));
		assert_eq!(out, Number::from(123.45));
	}

	#[test]
	fn number_decimal_neg() {
		let sql = "-123.45";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("-123.45", format!("{}", out));
		assert_eq!(out, Number::from(-123.45));
	}

	#[test]
	fn number_scientific_lower() {
		let sql = "12345e-1";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("1234.5", format!("{}", out));
		assert_eq!(out, Number::from(1234.5));
	}

	#[test]
	fn number_scientific_lower_neg() {
		let sql = "-12345e-1";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("-1234.5", format!("{}", out));
		assert_eq!(out, Number::from(-1234.5));
	}

	#[test]
	fn number_scientific_upper() {
		let sql = "12345E-02";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("123.45", format!("{}", out));
		assert_eq!(out, Number::from(123.45));
	}

	#[test]
	fn number_scientific_upper_neg() {
		let sql = "-12345E-02";
		let res = number(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("-123.45", format!("{}", out));
		assert_eq!(out, Number::from(-123.45));
	}
}
