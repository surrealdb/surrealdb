use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::serde::is_internal_serialization;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Deserialize, Store)]
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
}

impl Constant {
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		Ok(match self {
			Constant::MathE => std::f64::consts::E.into(),
			Constant::MathFrac1Pi => std::f64::consts::FRAC_1_PI.into(),
			Constant::MathFrac1Sqrt2 => std::f64::consts::FRAC_1_SQRT_2.into(),
			Constant::MathFrac2Pi => std::f64::consts::FRAC_2_PI.into(),
			Constant::MathFrac2SqrtPi => std::f64::consts::FRAC_2_SQRT_PI.into(),
			Constant::MathFracPi2 => std::f64::consts::FRAC_PI_2.into(),
			Constant::MathFracPi3 => std::f64::consts::FRAC_PI_3.into(),
			Constant::MathFracPi4 => std::f64::consts::FRAC_PI_4.into(),
			Constant::MathFracPi6 => std::f64::consts::FRAC_PI_6.into(),
			Constant::MathFracPi8 => std::f64::consts::FRAC_PI_8.into(),
			Constant::MathLn10 => std::f64::consts::LN_10.into(),
			Constant::MathLn2 => std::f64::consts::LN_2.into(),
			Constant::MathLog102 => std::f64::consts::LOG10_2.into(),
			Constant::MathLog10E => std::f64::consts::LOG10_E.into(),
			Constant::MathLog210 => std::f64::consts::LOG2_10.into(),
			Constant::MathLog2E => std::f64::consts::LOG2_E.into(),
			Constant::MathPi => std::f64::consts::PI.into(),
			Constant::MathSqrt2 => std::f64::consts::SQRT_2.into(),
			Constant::MathTau => std::f64::consts::TAU.into(),
		})
	}
}

impl fmt::Display for Constant {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Constant::MathE => "math::E",
			Constant::MathFrac1Pi => "math::FRAC_1_PI",
			Constant::MathFrac1Sqrt2 => "math::FRAC_1_SQRT_2",
			Constant::MathFrac2Pi => "math::FRAC_2_PI",
			Constant::MathFrac2SqrtPi => "math::FRAC_2_SQRT_PI",
			Constant::MathFracPi2 => "math::FRAC_PI_2",
			Constant::MathFracPi3 => "math::FRAC_PI_3",
			Constant::MathFracPi4 => "math::FRAC_PI_4",
			Constant::MathFracPi6 => "math::FRAC_PI_6",
			Constant::MathFracPi8 => "math::FRAC_PI_8",
			Constant::MathLn10 => "math::LN_10",
			Constant::MathLn2 => "math::LN_2",
			Constant::MathLog102 => "math::LOG10_2",
			Constant::MathLog10E => "math::LOG10_E",
			Constant::MathLog210 => "math::LOG2_10",
			Constant::MathLog2E => "math::LOG2_E",
			Constant::MathPi => "math::PI",
			Constant::MathSqrt2 => "math::SQRT_2",
			Constant::MathTau => "math::TAU",
		})
	}
}

#[rustfmt::skip]
impl Serialize for Constant {
	fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			match self {
				Constant::MathE => s.serialize_unit_variant("Constant", 0, "MathE"),
				Constant::MathFrac1Pi => s.serialize_unit_variant("Constant", 1, "MathFrac1Pi"),
				Constant::MathFrac1Sqrt2 => s.serialize_unit_variant("Constant", 2, "MathFrac1Sqrt2"),
				Constant::MathFrac2Pi => s.serialize_unit_variant("Constant", 3, "MathFrac2Pi"),
				Constant::MathFrac2SqrtPi => s.serialize_unit_variant("Constant", 4, "MathFrac2SqrtPi"),
				Constant::MathFracPi2 => s.serialize_unit_variant("Constant", 5, "MathFracPi2"),
				Constant::MathFracPi3 => s.serialize_unit_variant("Constant", 6, "MathFracPi3"),
				Constant::MathFracPi4 => s.serialize_unit_variant("Constant", 7, "MathFracPi4"),
				Constant::MathFracPi6 => s.serialize_unit_variant("Constant", 8, "MathFracPi6"),
				Constant::MathFracPi8 => s.serialize_unit_variant("Constant", 9, "MathFracPi8"),
				Constant::MathLn10 => s.serialize_unit_variant("Constant", 10, "MathLn10"),
				Constant::MathLn2 => s.serialize_unit_variant("Constant", 11, "MathLn2"),
				Constant::MathLog102 => s.serialize_unit_variant("Constant", 12, "MathLog102"),
				Constant::MathLog10E => s.serialize_unit_variant("Constant", 13, "MathLog10E"),
				Constant::MathLog210 => s.serialize_unit_variant("Constant", 14, "MathLog210"),
				Constant::MathLog2E => s.serialize_unit_variant("Constant", 15, "MathLog2E"),
				Constant::MathPi => s.serialize_unit_variant("Constant", 16, "MathPi"),
				Constant::MathSqrt2 => s.serialize_unit_variant("Constant", 17, "MathSqrt2"),
				Constant::MathTau => s.serialize_unit_variant("Constant", 18, "MathTau"),
			}
		} else {
			match self {
				Constant::MathE => s.serialize_f64(std::f64::consts::E),
				Constant::MathFrac1Pi => s.serialize_f64(std::f64::consts::FRAC_1_PI),
				Constant::MathFrac1Sqrt2 => s.serialize_f64(std::f64::consts::FRAC_1_SQRT_2),
				Constant::MathFrac2Pi => s.serialize_f64(std::f64::consts::FRAC_2_PI),
				Constant::MathFrac2SqrtPi => s.serialize_f64(std::f64::consts::FRAC_2_SQRT_PI),
				Constant::MathFracPi2 => s.serialize_f64(std::f64::consts::FRAC_PI_2),
				Constant::MathFracPi3 => s.serialize_f64(std::f64::consts::FRAC_PI_3),
				Constant::MathFracPi4 => s.serialize_f64(std::f64::consts::FRAC_PI_4),
				Constant::MathFracPi6 => s.serialize_f64(std::f64::consts::FRAC_PI_6),
				Constant::MathFracPi8 => s.serialize_f64(std::f64::consts::FRAC_PI_8),
				Constant::MathLn10 => s.serialize_f64(std::f64::consts::LN_10),
				Constant::MathLn2 => s.serialize_f64(std::f64::consts::LN_2),
				Constant::MathLog102 => s.serialize_f64(std::f64::consts::LOG10_2),
				Constant::MathLog10E => s.serialize_f64(std::f64::consts::LOG10_E),
				Constant::MathLog210 => s.serialize_f64(std::f64::consts::LOG2_10),
				Constant::MathLog2E => s.serialize_f64(std::f64::consts::LOG2_E),
				Constant::MathPi => s.serialize_f64(std::f64::consts::PI),
				Constant::MathSqrt2 => s.serialize_f64(std::f64::consts::SQRT_2),
				Constant::MathTau => s.serialize_f64(std::f64::consts::TAU),
			}
		}
	}
}

pub fn constant(i: &str) -> IResult<&str, Constant> {
	alt((constant_math,))(i)
}

fn constant_math(i: &str) -> IResult<&str, Constant> {
	alt((
		map(tag_no_case("math::E"), |_| Constant::MathE),
		map(tag_no_case("math::FRAC_1_PI"), |_| Constant::MathFrac1Pi),
		map(tag_no_case("math::FRAC_1_SQRT_2"), |_| Constant::MathFrac1Sqrt2),
		map(tag_no_case("math::FRAC_2_PI"), |_| Constant::MathFrac2Pi),
		map(tag_no_case("math::FRAC_2_SQRT_PI"), |_| Constant::MathFrac2SqrtPi),
		map(tag_no_case("math::FRAC_PI_2"), |_| Constant::MathFracPi2),
		map(tag_no_case("math::FRAC_PI_3"), |_| Constant::MathFracPi3),
		map(tag_no_case("math::FRAC_PI_4"), |_| Constant::MathFracPi4),
		map(tag_no_case("math::FRAC_PI_6"), |_| Constant::MathFracPi6),
		map(tag_no_case("math::FRAC_PI_8"), |_| Constant::MathFracPi8),
		map(tag_no_case("math::LN_10"), |_| Constant::MathLn10),
		map(tag_no_case("math::LN_2"), |_| Constant::MathLn2),
		map(tag_no_case("math::LOG10_2"), |_| Constant::MathLog102),
		map(tag_no_case("math::LOG10_E"), |_| Constant::MathLog10E),
		map(tag_no_case("math::LOG2_10"), |_| Constant::MathLog210),
		map(tag_no_case("math::LOG2_E"), |_| Constant::MathLog2E),
		map(tag_no_case("math::PI"), |_| Constant::MathPi),
		map(tag_no_case("math::SQRT_2"), |_| Constant::MathSqrt2),
		map(tag_no_case("math::TAU"), |_| Constant::MathTau),
	))(i)
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
