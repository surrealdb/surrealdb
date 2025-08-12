use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<Constant> for crate::expr::Constant {
	fn from(value: Constant) -> Self {
		match value {
			Constant::MathE => Self::MathE,
			Constant::MathFrac1Pi => Self::MathFrac1Pi,
			Constant::MathFrac1Sqrt2 => Self::MathFrac1Sqrt2,
			Constant::MathFrac2Pi => Self::MathFrac2Pi,
			Constant::MathFrac2SqrtPi => Self::MathFrac2SqrtPi,
			Constant::MathFracPi2 => Self::MathFracPi2,
			Constant::MathFracPi3 => Self::MathFracPi3,
			Constant::MathFracPi4 => Self::MathFracPi4,
			Constant::MathFracPi6 => Self::MathFracPi6,
			Constant::MathFracPi8 => Self::MathFracPi8,
			Constant::MathInf => Self::MathInf,
			Constant::MathLn10 => Self::MathLn10,
			Constant::MathLn2 => Self::MathLn2,
			Constant::MathLog102 => Self::MathLog102,
			Constant::MathLog10E => Self::MathLog10E,
			Constant::MathLog210 => Self::MathLog210,
			Constant::MathLog2E => Self::MathLog2E,
			Constant::MathNegInf => Self::MathNegInf,
			Constant::MathPi => Self::MathPi,
			Constant::MathSqrt2 => Self::MathSqrt2,
			Constant::MathTau => Self::MathTau,
			Constant::TimeEpoch => Self::TimeEpoch,
			Constant::TimeMin => Self::TimeMin,
			Constant::TimeMax => Self::TimeMax,
			Constant::DurationMax => Self::DurationMax,
		}
	}
}

impl From<crate::expr::Constant> for Constant {
	fn from(value: crate::expr::Constant) -> Self {
		match value {
			crate::expr::Constant::MathE => Self::MathE,
			crate::expr::Constant::MathFrac1Pi => Self::MathFrac1Pi,
			crate::expr::Constant::MathFrac1Sqrt2 => Self::MathFrac1Sqrt2,
			crate::expr::Constant::MathFrac2Pi => Self::MathFrac2Pi,
			crate::expr::Constant::MathFrac2SqrtPi => Self::MathFrac2SqrtPi,
			crate::expr::Constant::MathFracPi2 => Self::MathFracPi2,
			crate::expr::Constant::MathFracPi3 => Self::MathFracPi3,
			crate::expr::Constant::MathFracPi4 => Self::MathFracPi4,
			crate::expr::Constant::MathFracPi6 => Self::MathFracPi6,
			crate::expr::Constant::MathFracPi8 => Self::MathFracPi8,
			crate::expr::Constant::MathInf => Self::MathInf,
			crate::expr::Constant::MathLn10 => Self::MathLn10,
			crate::expr::Constant::MathLn2 => Self::MathLn2,
			crate::expr::Constant::MathLog102 => Self::MathLog102,
			crate::expr::Constant::MathLog10E => Self::MathLog10E,
			crate::expr::Constant::MathLog210 => Self::MathLog210,
			crate::expr::Constant::MathLog2E => Self::MathLog2E,
			crate::expr::Constant::MathNegInf => Self::MathNegInf,
			crate::expr::Constant::MathPi => Self::MathPi,
			crate::expr::Constant::MathSqrt2 => Self::MathSqrt2,
			crate::expr::Constant::MathTau => Self::MathTau,
			crate::expr::Constant::TimeEpoch => Self::TimeEpoch,
			crate::expr::Constant::TimeMin => Self::TimeMin,
			crate::expr::Constant::TimeMax => Self::TimeMax,
			crate::expr::Constant::DurationMax => Self::DurationMax,
		}
	}
}
