use super::super::{ending::number as ending, IResult, ParseError};
use crate::sql::Number;
use nom::{
	branch::alt,
	bytes::complete::tag,
	character::complete::i64,
	combinator::{opt, value},
	number::complete::recognize_float,
	Err,
};
use rust_decimal::Decimal;
use std::str::FromStr;

fn not_nan(i: &str) -> IResult<&str, Number> {
	let (i, v) = match recognize_float(i) {
		Ok(x) => x,
		Err(Err::Failure(x)) | Err(Err::Error(x)) => return Err(Err::Error(x)),
		Err(x) => return Err(x),
	};
	let (i, suffix) = suffix(i)?;
	let (i, _) = ending(i)?;
	let number = match suffix {
		Suffix::None => {
			// Manually check for int or float for better parsing errors
			if v.contains(['e', 'E', '.']) {
				let float = f64::from_str(v)
					.map_err(|e| ParseError::ParseFloat {
						tried: v,
						error: e,
					})
					.map_err(Err::Failure)?;
				Number::from(float)
			} else {
				let int = i64::from_str(v)
					.map_err(|e| ParseError::ParseInt {
						tried: v,
						error: e,
					})
					.map_err(Err::Failure)?;
				Number::from(int)
			}
		}
		Suffix::Float => {
			let float = f64::from_str(v)
				.map_err(|e| ParseError::ParseFloat {
					tried: v,
					error: e,
				})
				.map_err(Err::Failure)?;
			Number::from(float)
		}
		Suffix::Decimal => Number::from(
			Decimal::from_str(v)
				.map_err(|e| ParseError::ParseDecimal {
					tried: v,
					error: e,
				})
				.map_err(Err::Failure)?,
		),
	};
	Ok((i, number))
}

pub fn number(i: &str) -> IResult<&str, Number> {
	alt((value(Number::NAN, tag("NaN")), not_nan))(i)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Suffix {
	None,
	Float,
	Decimal,
}

fn suffix(i: &str) -> IResult<&str, Suffix> {
	let (i, opt_suffix) =
		opt(alt((value(Suffix::Float, tag("f")), value(Suffix::Decimal, tag("dec")))))(i)?;
	Ok((i, opt_suffix.unwrap_or(Suffix::None)))
}

pub fn integer(i: &str) -> IResult<&str, i64> {
	let (i, v) = i64(i)?;
	let (i, _) = ending(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;
	use std::{cmp::Ordering, ops::Div};

	#[test]
	fn number_nan() {
		let sql = "NaN";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!("NaN", format!("{}", out));
	}

	#[test]
	fn number_int() {
		let sql = "123";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!("123", format!("{}", out));
		assert_eq!(out, Number::Int(123));
	}

	#[test]
	fn number_int_neg() {
		let sql = "-123";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!("-123", format!("{}", out));
		assert_eq!(out, Number::Int(-123));
	}

	#[test]
	fn number_float() {
		let sql = "123.45f";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
		assert_eq!(out, Number::Float(123.45));
	}

	#[test]
	fn number_float_neg() {
		let sql = "-123.45f";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
		assert_eq!(out, Number::Float(-123.45));
	}

	#[test]
	fn number_scientific_lower() {
		let sql = "12345e-1";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!("1234.5f", format!("{}", out));
		assert_eq!(out, Number::Float(1234.5));
	}

	#[test]
	fn number_scientific_lower_neg() {
		let sql = "-12345e-1";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!("-1234.5f", format!("{}", out));
		assert_eq!(out, Number::Float(-1234.5));
	}

	#[test]
	fn number_scientific_upper() {
		let sql = "12345E-02";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!("123.45f", format!("{}", out));
		assert_eq!(out, Number::Float(123.45));
	}

	#[test]
	fn number_scientific_upper_neg() {
		let sql = "-12345E-02";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!("-123.45f", format!("{}", out));
		assert_eq!(out, Number::Float(-123.45));
	}

	#[test]
	fn number_float_keeps_precision() {
		let sql = "13.571938471938472f";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn number_decimal_keeps_precision() {
		let sql = "0.0000000000000000000000000321dec";
		let res = number(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn number_div_int() {
		let res = Number::Int(3).div(Number::Int(2));
		assert_eq!(res, Number::Int(1));
	}

	#[test]
	fn number_pow_int() {
		let res = Number::Int(3).pow(Number::Int(4));
		assert_eq!(res, Number::Int(81));
	}

	#[test]
	fn number_pow_int_negative() {
		let res = Number::Int(4).pow(Number::Float(-0.5));
		assert_eq!(res, Number::Float(0.5));
	}

	#[test]
	fn number_pow_float() {
		let res = Number::Float(2.5).pow(Number::Int(2));
		assert_eq!(res, Number::Float(6.25));
	}

	#[test]
	fn number_pow_float_negative() {
		let res = Number::Int(4).pow(Number::Float(-0.5));
		assert_eq!(res, Number::Float(0.5));
	}

	#[test]
	fn number_pow_decimal_one() {
		let res = Number::try_from("13.5719384719384719385639856394139476937756394756")
			.unwrap()
			.pow(Number::Int(1));
		assert_eq!(
			res,
			Number::try_from("13.5719384719384719385639856394139476937756394756").unwrap()
		);
	}

	#[test]
	fn number_pow_decimal_two() {
		let res = Number::try_from("13.5719384719384719385639856394139476937756394756")
			.unwrap()
			.pow(Number::Int(2));
		assert_eq!(
			res,
			Number::try_from("184.19751388608358465578173996877942643463869043732548087725588482334195240945031617770904299536").unwrap()
		);
	}

	#[test]
	fn ord() {
		fn assert_cmp(a: &Number, b: &Number, ord: Ordering) {
			assert_eq!(a.cmp(b), ord, "{a} {ord:?} {b}");
			assert_eq!(a == b, ord.is_eq(), "{a} {ord:?} {b}");
		}

		let nz = -0.0f64;
		let z = 0.0f64;
		assert_ne!(nz.to_bits(), z.to_bits());
		let nzp = permutations(nz);
		let zp = permutations(z);
		for nzp in nzp.iter() {
			for zp in zp.iter() {
				assert_cmp(nzp, zp, Ordering::Equal);
			}
		}

		let negative_nan = f64::from_bits(18444492273895866368);

		let ordering = &[
			negative_nan,
			f64::NEG_INFINITY,
			-10.0,
			-1.0,
			-f64::MIN_POSITIVE,
			0.0,
			f64::MIN_POSITIVE,
			1.0,
			10.0,
			f64::INFINITY,
			f64::NAN,
		];

		fn permutations(n: f64) -> Vec<Number> {
			let mut ret = Vec::new();
			ret.push(Number::Float(n));
			if n.is_finite() && (n == 0.0 || n.abs() > f64::EPSILON) {
				ret.push(Number::Decimal(n.try_into().unwrap()));
				ret.push(Number::Int(n as i64));
			}
			ret
		}

		for (ai, a) in ordering.iter().enumerate() {
			let ap = permutations(*a);
			for (bi, b) in ordering.iter().enumerate() {
				let bp = permutations(*b);
				let correct = ai.cmp(&bi);

				for a in &ap {
					for b in &bp {
						assert_cmp(a, b, correct);
					}
				}
			}
		}
	}
}
