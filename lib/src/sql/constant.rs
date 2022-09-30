use crate::sql::error::IResult;
use crate::sql::value::Value;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;

pub fn constant(i: &str) -> IResult<&str, Value> {
	alt((constant_math,))(i)
}

fn constant_math(i: &str) -> IResult<&str, Value> {
	alt((
		map(tag_no_case("math::E"), |_| std::f64::consts::E.into()),
		map(tag_no_case("math::FRAC_1_PI"), |_| std::f64::consts::FRAC_1_PI.into()),
		map(tag_no_case("math::FRAC_1_SQRT_2"), |_| std::f64::consts::FRAC_1_SQRT_2.into()),
		map(tag_no_case("math::FRAC_2_PI"), |_| std::f64::consts::FRAC_2_PI.into()),
		map(tag_no_case("math::FRAC_2_SQRT_PI"), |_| std::f64::consts::FRAC_2_SQRT_PI.into()),
		map(tag_no_case("math::FRAC_PI_2"), |_| std::f64::consts::FRAC_PI_2.into()),
		map(tag_no_case("math::FRAC_PI_3"), |_| std::f64::consts::FRAC_PI_3.into()),
		map(tag_no_case("math::FRAC_PI_4"), |_| std::f64::consts::FRAC_PI_4.into()),
		map(tag_no_case("math::FRAC_PI_6"), |_| std::f64::consts::FRAC_PI_6.into()),
		map(tag_no_case("math::FRAC_PI_8"), |_| std::f64::consts::FRAC_PI_8.into()),
		map(tag_no_case("math::LN_10"), |_| std::f64::consts::LN_10.into()),
		map(tag_no_case("math::LN_2"), |_| std::f64::consts::LN_2.into()),
		map(tag_no_case("math::LOG10_2"), |_| std::f64::consts::LOG10_2.into()),
		map(tag_no_case("math::LOG10_E"), |_| std::f64::consts::LOG10_E.into()),
		map(tag_no_case("math::LOG2_10"), |_| std::f64::consts::LOG2_10.into()),
		map(tag_no_case("math::LOG2_E"), |_| std::f64::consts::LOG2_E.into()),
		map(tag_no_case("math::PI"), |_| std::f64::consts::PI.into()),
		map(tag_no_case("math::SQRT_2"), |_| std::f64::consts::SQRT_2.into()),
		map(tag_no_case("math::TAU"), |_| std::f64::consts::TAU.into()),
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
		assert_eq!(out, Value::from(std::f64::consts::PI));
	}

	#[test]
	fn constant_uppercase() {
		let sql = "MATH::PI";
		let res = constant(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Value::from(std::f64::consts::PI));
	}

	#[test]
	fn constant_mixedcase() {
		let sql = "math::PI";
		let res = constant(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Value::from(std::f64::consts::PI));
	}
}
