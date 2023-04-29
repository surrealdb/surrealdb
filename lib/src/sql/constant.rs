use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::value::Value;
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
	MathLn10,
	MathLn2,
	MathLog102,
	MathLog10E,
	MathLog210,
	MathLog2E,
	MathPi,
	MathSqrt2,
	MathTau,
	// Add new variants here
}

impl Constant {
	pub(crate) fn as_f64(&self) -> f64 {
		match self {
			Self::MathE => std::f64::consts::E,
			Self::MathFrac1Pi => std::f64::consts::FRAC_1_PI,
			Self::MathFrac1Sqrt2 => std::f64::consts::FRAC_1_SQRT_2,
			Self::MathFrac2Pi => std::f64::consts::FRAC_2_PI,
			Self::MathFrac2SqrtPi => std::f64::consts::FRAC_2_SQRT_PI,
			Self::MathFracPi2 => std::f64::consts::FRAC_PI_2,
			Self::MathFracPi3 => std::f64::consts::FRAC_PI_3,
			Self::MathFracPi4 => std::f64::consts::FRAC_PI_4,
			Self::MathFracPi6 => std::f64::consts::FRAC_PI_6,
			Self::MathFracPi8 => std::f64::consts::FRAC_PI_8,
			Self::MathLn10 => std::f64::consts::LN_10,
			Self::MathLn2 => std::f64::consts::LN_2,
			Self::MathLog102 => std::f64::consts::LOG10_2,
			Self::MathLog10E => std::f64::consts::LOG10_E,
			Self::MathLog210 => std::f64::consts::LOG2_10,
			Self::MathLog2E => std::f64::consts::LOG2_E,
			Self::MathPi => std::f64::consts::PI,
			Self::MathSqrt2 => std::f64::consts::SQRT_2,
			Self::MathTau => std::f64::consts::TAU,
		}
	}

	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		Ok(self.as_f64().into())
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
			Self::MathLn10 => "math::LN_10",
			Self::MathLn2 => "math::LN_2",
			Self::MathLog102 => "math::LOG10_2",
			Self::MathLog10E => "math::LOG10_E",
			Self::MathLog210 => "math::LOG2_10",
			Self::MathLog2E => "math::LOG2_E",
			Self::MathPi => "math::PI",
			Self::MathSqrt2 => "math::SQRT_2",
			Self::MathTau => "math::TAU",
		})
	}
}

pub fn constant(i: &str) -> IResult<&str, Constant> {
	alt((constant_math,))(i)
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
