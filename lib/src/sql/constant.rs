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

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Constant";

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Deserialize, Store, Hash)]
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
			Self::MathE => std::f64::consts::E.into(),
			Self::MathFrac1Pi => std::f64::consts::FRAC_1_PI.into(),
			Self::MathFrac1Sqrt2 => std::f64::consts::FRAC_1_SQRT_2.into(),
			Self::MathFrac2Pi => std::f64::consts::FRAC_2_PI.into(),
			Self::MathFrac2SqrtPi => std::f64::consts::FRAC_2_SQRT_PI.into(),
			Self::MathFracPi2 => std::f64::consts::FRAC_PI_2.into(),
			Self::MathFracPi3 => std::f64::consts::FRAC_PI_3.into(),
			Self::MathFracPi4 => std::f64::consts::FRAC_PI_4.into(),
			Self::MathFracPi6 => std::f64::consts::FRAC_PI_6.into(),
			Self::MathFracPi8 => std::f64::consts::FRAC_PI_8.into(),
			Self::MathLn10 => std::f64::consts::LN_10.into(),
			Self::MathLn2 => std::f64::consts::LN_2.into(),
			Self::MathLog102 => std::f64::consts::LOG10_2.into(),
			Self::MathLog10E => std::f64::consts::LOG10_E.into(),
			Self::MathLog210 => std::f64::consts::LOG2_10.into(),
			Self::MathLog2E => std::f64::consts::LOG2_E.into(),
			Self::MathPi => std::f64::consts::PI.into(),
			Self::MathSqrt2 => std::f64::consts::SQRT_2.into(),
			Self::MathTau => std::f64::consts::TAU.into(),
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

#[rustfmt::skip]
impl Serialize for Constant {
	fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			match self {
				Self::MathE => s.serialize_unit_variant(TOKEN, 0, "MathE"),
				Self::MathFrac1Pi => s.serialize_unit_variant(TOKEN, 1, "MathFrac1Pi"),
				Self::MathFrac1Sqrt2 => s.serialize_unit_variant(TOKEN, 2, "MathFrac1Sqrt2"),
				Self::MathFrac2Pi => s.serialize_unit_variant(TOKEN, 3, "MathFrac2Pi"),
				Self::MathFrac2SqrtPi => s.serialize_unit_variant(TOKEN, 4, "MathFrac2SqrtPi"),
				Self::MathFracPi2 => s.serialize_unit_variant(TOKEN, 5, "MathFracPi2"),
				Self::MathFracPi3 => s.serialize_unit_variant(TOKEN, 6, "MathFracPi3"),
				Self::MathFracPi4 => s.serialize_unit_variant(TOKEN, 7, "MathFracPi4"),
				Self::MathFracPi6 => s.serialize_unit_variant(TOKEN, 8, "MathFracPi6"),
				Self::MathFracPi8 => s.serialize_unit_variant(TOKEN, 9, "MathFracPi8"),
				Self::MathLn10 => s.serialize_unit_variant(TOKEN, 10, "MathLn10"),
				Self::MathLn2 => s.serialize_unit_variant(TOKEN, 11, "MathLn2"),
				Self::MathLog102 => s.serialize_unit_variant(TOKEN, 12, "MathLog102"),
				Self::MathLog10E => s.serialize_unit_variant(TOKEN, 13, "MathLog10E"),
				Self::MathLog210 => s.serialize_unit_variant(TOKEN, 14, "MathLog210"),
				Self::MathLog2E => s.serialize_unit_variant(TOKEN, 15, "MathLog2E"),
				Self::MathPi => s.serialize_unit_variant(TOKEN, 16, "MathPi"),
				Self::MathSqrt2 => s.serialize_unit_variant(TOKEN, 17, "MathSqrt2"),
				Self::MathTau => s.serialize_unit_variant(TOKEN, 18, "MathTau"),
			}
		} else {
			match self {
				Self::MathE => s.serialize_f64(std::f64::consts::E),
				Self::MathFrac1Pi => s.serialize_f64(std::f64::consts::FRAC_1_PI),
				Self::MathFrac1Sqrt2 => s.serialize_f64(std::f64::consts::FRAC_1_SQRT_2),
				Self::MathFrac2Pi => s.serialize_f64(std::f64::consts::FRAC_2_PI),
				Self::MathFrac2SqrtPi => s.serialize_f64(std::f64::consts::FRAC_2_SQRT_PI),
				Self::MathFracPi2 => s.serialize_f64(std::f64::consts::FRAC_PI_2),
				Self::MathFracPi3 => s.serialize_f64(std::f64::consts::FRAC_PI_3),
				Self::MathFracPi4 => s.serialize_f64(std::f64::consts::FRAC_PI_4),
				Self::MathFracPi6 => s.serialize_f64(std::f64::consts::FRAC_PI_6),
				Self::MathFracPi8 => s.serialize_f64(std::f64::consts::FRAC_PI_8),
				Self::MathLn10 => s.serialize_f64(std::f64::consts::LN_10),
				Self::MathLn2 => s.serialize_f64(std::f64::consts::LN_2),
				Self::MathLog102 => s.serialize_f64(std::f64::consts::LOG10_2),
				Self::MathLog10E => s.serialize_f64(std::f64::consts::LOG10_E),
				Self::MathLog210 => s.serialize_f64(std::f64::consts::LOG2_10),
				Self::MathLog2E => s.serialize_f64(std::f64::consts::LOG2_E),
				Self::MathPi => s.serialize_f64(std::f64::consts::PI),
				Self::MathSqrt2 => s.serialize_f64(std::f64::consts::SQRT_2),
				Self::MathTau => s.serialize_f64(std::f64::consts::TAU),
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
