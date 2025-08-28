use std::fmt;

use anyhow::Result;
use chrono::{TimeZone, Utc};

use crate::val::{Datetime, Duration, Value};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
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
	MathNegInf,
	MathPi,
	MathSqrt2,
	MathTau,
	TimeEpoch,
	TimeMin,
	TimeMax,
	DurationMax,
	// Add new variants here
}

/// A type of constant that may be converted to a value or JSON.
pub(crate) enum ConstantValue {
	Float(f64),
	Datetime(Datetime),
	Duration(Duration),
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
			Self::MathNegInf => ConstantValue::Float(f64::NEG_INFINITY),
			Self::MathPi => ConstantValue::Float(f64c::PI),
			Self::MathSqrt2 => ConstantValue::Float(f64c::SQRT_2),
			Self::MathTau => ConstantValue::Float(f64c::TAU),
			Self::TimeEpoch => ConstantValue::Datetime(Datetime(Utc.timestamp_nanos(0))),
			Self::TimeMin => ConstantValue::Datetime(Datetime::MIN_UTC),
			Self::TimeMax => ConstantValue::Datetime(Datetime::MAX_UTC),
			Self::DurationMax => ConstantValue::Duration(Duration::MAX),
		}
	}
	/// Process this type returning a computed simple Value
	pub fn compute(&self) -> Result<Value> {
		Ok(match self.value() {
			ConstantValue::Datetime(d) => d.into(),
			ConstantValue::Float(f) => f.into(),
			ConstantValue::Duration(d) => d.into(),
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
			Self::MathNegInf => "math::NEG_INF",
			Self::MathPi => "math::PI",
			Self::MathSqrt2 => "math::SQRT_2",
			Self::MathTau => "math::TAU",
			Self::TimeEpoch => "time::EPOCH",
			Self::TimeMin => "time::MINIMUM",
			Self::TimeMax => "time::MAXIMUM",
			Self::DurationMax => "duration::MAX",
		})
	}
}
