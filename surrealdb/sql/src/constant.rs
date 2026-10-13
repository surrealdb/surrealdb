use surrealdb_types::{SqlFormat, ToSql};

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
	MathInfinity,
	MathLn10,
	MathLn2,
	MathLog102,
	MathLog10E,
	MathLog210,
	MathLog2E,
	MathNegInfinity,
	MathPi,
	MathSqrt2,
	MathTau,
	TimeEpoch,
	TimeMin,
	TimeMax,
	DurationMax,
	// Add new variants here
}

impl ToSql for Constant {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(match self {
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
			Self::MathInfinity => "math::INFINITY",
			Self::MathLn10 => "math::LN_10",
			Self::MathLn2 => "math::LN_2",
			Self::MathLog102 => "math::LOG10_2",
			Self::MathLog10E => "math::LOG10_E",
			Self::MathLog210 => "math::LOG2_10",
			Self::MathLog2E => "math::LOG2_E",
			Self::MathNegInfinity => "math::NEG_INFINITY",
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
