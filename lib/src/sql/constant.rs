use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::value::Value;
use crate::sql::Datetime;
use chrono::TimeZone;
use chrono::Utc;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Constant";

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[serde(rename = "$surrealdb::private::sql::Constant")]
pub enum Constant {
	MathE,
	MathFrac1Pi,
	MathFrac1Sqrt2,
	MathFrac2Pi,
	MathFrac2SqrtPi,
	MathFracPi2,
	MathFracPi3,
	MathFracPi4,
	MathFracPi6,
	MathFracPi8,
	MathInf,
	MathLn10,
	MathLn2,
	MathLog102,
	MathLog10E,
	MathLog210,
	MathLog2E,
	MathPi,
	MathSqrt2,
	MathTau,
	TimeEpoch,
	// Add new variants here
}

/// A type of constant that may be converted to a value or JSON.
pub(crate) enum ConstantValue {
	Float(f64),
	Datetime(Datetime),
}

impl Constant {
	pub(crate) fn value(&self) -> ConstantValue {
		use std::f64::consts as f64c;
		match self {
			Self::MathE => ConstantValue::Float(f64c::E),
			Self::MathFrac1Pi => ConstantValue::Float(f64c::FRAC_1_PI),
			Self::MathFrac1Sqrt2 => ConstantValue::Float(f64c::FRAC_1_SQRT_2),
			Self::MathFrac2Pi => ConstantValue::Float(f64c::FRAC_2_PI),
			Self::MathFrac2SqrtPi => ConstantValue::Float(f64c::FRAC_2_SQRT_PI),
			Self::MathFracPi2 => ConstantValue::Float(f64c::FRAC_PI_2),
			Self::MathFracPi3 => ConstantValue::Float(f64c::FRAC_PI_3),
			Self::MathFracPi4 => ConstantValue::Float(f64c::FRAC_PI_4),
			Self::MathFracPi6 => ConstantValue::Float(f64c::FRAC_PI_6),
			Self::MathFracPi8 => ConstantValue::Float(f64c::FRAC_PI_8),
			Self::MathInf => ConstantValue::Float(f64::INFINITY),
			Self::MathLn10 => ConstantValue::Float(f64c::LN_10),
			Self::MathLn2 => ConstantValue::Float(f64c::LN_2),
			Self::MathLog102 => ConstantValue::Float(f64c::LOG10_2),
			Self::MathLog10E => ConstantValue::Float(f64c::LOG10_E),
			Self::MathLog210 => ConstantValue::Float(f64c::LOG2_10),
			Self::MathLog2E => ConstantValue::Float(f64c::LOG2_E),
			Self::MathPi => ConstantValue::Float(f64c::PI),
			Self::MathSqrt2 => ConstantValue::Float(f64c::SQRT_2),
			Self::MathTau => ConstantValue::Float(f64c::TAU),
			Self::TimeEpoch => ConstantValue::Datetime(Datetime(Utc.timestamp_nanos(0))),
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, _ctx: &Context<'_>, _opt: &Options) -> Result<Value, Error> {
		Ok(match self.value() {
			ConstantValue::Datetime(d) => d.into(),
			ConstantValue::Float(f) => f.into(),
		})
	}
}

impl fmt::Display for Constant {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::MathE => "math::E",
			Self::MathFrac1Pi => "math::FRAC_1_PI",
			Self::MathFrac1Sqrt2 => "math::FRAC_1_SQRT_2",
			Self::MathFrac2Pi => "math::FRAC_2_PI",
			Self::MathFrac2SqrtPi => "math::FRAC_2_SQRT_PI",
			Self::MathFracPi2 => "math::FRAC_PI_2",
			Self::MathFracPi3 => "math::FRAC_PI_3",
			Self::MathFracPi4 => "math::FRAC_PI_4",
			Self::MathFracPi6 => "math::FRAC_PI_6",
			Self::MathFracPi8 => "math::FRAC_PI_8",
			Self::MathInf => "math::INF",
			Self::MathLn10 => "math::LN_10",
			Self::MathLn2 => "math::LN_2",
			Self::MathLog102 => "math::LOG10_2",
			Self::MathLog10E => "math::LOG10_E",
			Self::MathLog210 => "math::LOG2_10",
			Self::MathLog2E => "math::LOG2_E",
			Self::MathPi => "math::PI",
			Self::MathSqrt2 => "math::SQRT_2",
			Self::MathTau => "math::TAU",
			Self::TimeEpoch => "time::EPOCH",
		})
	}
}

pub fn constant(i: &str) -> IResult<&str, Constant> {
	alt((constant_math, constant_time))(i)
}

fn constant_math(i: &str) -> IResult<&str, Constant> {
	preceded(
		tag_no_case("math::"),
		alt((
			map(tag_no_case("E"), |_| Constant::MathE),
			map(tag_no_case("FRAC_1_PI"), |_| Constant::MathFrac1Pi),
			map(tag_no_case("FRAC_1_SQRT_2"), |_| Constant::MathFrac1Sqrt2),
			map(tag_no_case("FRAC_2_PI"), |_| Constant::MathFrac2Pi),
			map(tag_no_case("FRAC_2_SQRT_PI"), |_| Constant::MathFrac2SqrtPi),
			map(tag_no_case("FRAC_PI_2"), |_| Constant::MathFracPi2),
			map(tag_no_case("FRAC_PI_3"), |_| Constant::MathFracPi3),
			map(tag_no_case("FRAC_PI_4"), |_| Constant::MathFracPi4),
			map(tag_no_case("FRAC_PI_6"), |_| Constant::MathFracPi6),
			map(tag_no_case("FRAC_PI_8"), |_| Constant::MathFracPi8),
			map(tag_no_case("INF"), |_| Constant::MathInf),
			map(tag_no_case("LN_10"), |_| Constant::MathLn10),
			map(tag_no_case("LN_2"), |_| Constant::MathLn2),
			map(tag_no_case("LOG10_2"), |_| Constant::MathLog102),
			map(tag_no_case("LOG10_E"), |_| Constant::MathLog10E),
			map(tag_no_case("LOG2_10"), |_| Constant::MathLog210),
			map(tag_no_case("LOG2_E"), |_| Constant::MathLog2E),
			map(tag_no_case("PI"), |_| Constant::MathPi),
			map(tag_no_case("SQRT_2"), |_| Constant::MathSqrt2),
			map(tag_no_case("TAU"), |_| Constant::MathTau),
		)),
	)(i)
}

fn constant_time(i: &str) -> IResult<&str, Constant> {
	preceded(tag_no_case("time::"), alt((map(tag_no_case("EPOCH"), |_| Constant::TimeEpoch),)))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn constant_lowercase() {
		let sql = "math::pi";
		let res = constant(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Constant::MathPi);
	}

	#[test]
	fn constant_uppercase() {
		let sql = "MATH::PI";
		let res = constant(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Constant::MathPi);
	}

	#[test]
	fn constant_mixedcase() {
		let sql = "math::PI";
		let res = constant(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Constant::MathPi);
	}
}
