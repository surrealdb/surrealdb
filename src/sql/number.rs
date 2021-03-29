use dec::Decimal;
use nom::number::complete::double;
use nom::IResult;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops;
use std::str::FromStr;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize)]
pub struct Number {
	pub value: Decimal,
}

impl Default for Number {
	fn default() -> Self {
		Number {
			value: 0i32.into(),
		}
	}
}

impl From<i8> for Number {
	fn from(i: i8) -> Self {
		Number {
			value: i.into(),
		}
	}
}

impl From<i16> for Number {
	fn from(i: i16) -> Self {
		Number {
			value: i.into(),
		}
	}
}

impl From<i32> for Number {
	fn from(i: i32) -> Self {
		Number {
			value: i.into(),
		}
	}
}

impl From<i64> for Number {
	fn from(i: i64) -> Self {
		Number {
			value: i.into(),
		}
	}
}

impl From<u8> for Number {
	fn from(i: u8) -> Self {
		Number {
			value: i.into(),
		}
	}
}

impl From<u16> for Number {
	fn from(i: u16) -> Self {
		Number {
			value: i.into(),
		}
	}
}

impl From<u32> for Number {
	fn from(i: u32) -> Self {
		Number {
			value: i.into(),
		}
	}
}

impl From<u64> for Number {
	fn from(i: u64) -> Self {
		Number {
			value: i.into(),
		}
	}
}

impl From<f32> for Number {
	fn from(f: f32) -> Self {
		Number {
			value: Decimal::from_str(&f.to_string()).unwrap_or(Decimal::new(0, 0)),
		}
	}
}

impl From<f64> for Number {
	fn from(f: f64) -> Self {
		Number {
			value: Decimal::from_str(&f.to_string()).unwrap_or(Decimal::new(0, 0)),
		}
	}
}

impl<'a> From<&'a str> for Number {
	fn from(s: &str) -> Self {
		Number {
			value: Decimal::from_str(s).unwrap_or(Decimal::new(0, 0)),
		}
	}
}

impl From<String> for Number {
	fn from(s: String) -> Self {
		Number {
			value: Decimal::from_str(&s).unwrap_or(Decimal::new(0, 0)),
		}
	}
}

impl From<Decimal> for Number {
	fn from(v: Decimal) -> Self {
		Number {
			value: v,
		}
	}
}

impl fmt::Display for Number {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.value)
	}
}

impl Serialize for Number {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			serializer.serialize_some(&self.value)
		} else {
			let mut val = serializer.serialize_struct("Number", 1)?;
			val.serialize_field("value", &self.value)?;
			val.end()
		}
	}
}

impl ops::Add for Number {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		Number::from(self.value + other.value)
	}
}

impl ops::Sub for Number {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		Number::from(self.value - other.value)
	}
}

impl ops::Mul for Number {
	type Output = Self;
	fn mul(self, other: Self) -> Self {
		Number::from(self.value * other.value)
	}
}

impl ops::Div for Number {
	type Output = Self;
	fn div(self, other: Self) -> Self {
		Number::from(self.value / other.value)
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
