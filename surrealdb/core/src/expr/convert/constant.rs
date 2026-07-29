//! `sql` -> `expr` conversions for [`crate::sql::constant`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::constant::*;

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
			Constant::MathInfinity => Self::MathInfinity,
			Constant::MathLn10 => Self::MathLn10,
			Constant::MathLn2 => Self::MathLn2,
			Constant::MathLog102 => Self::MathLog102,
			Constant::MathLog10E => Self::MathLog10E,
			Constant::MathLog210 => Self::MathLog210,
			Constant::MathLog2E => Self::MathLog2E,
			Constant::MathNegInfinity => Self::MathNegInfinity,
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
			crate::expr::Constant::MathInfinity => Self::MathInfinity,
			crate::expr::Constant::MathLn10 => Self::MathLn10,
			crate::expr::Constant::MathLn2 => Self::MathLn2,
			crate::expr::Constant::MathLog102 => Self::MathLog102,
			crate::expr::Constant::MathLog10E => Self::MathLog10E,
			crate::expr::Constant::MathLog210 => Self::MathLog210,
			crate::expr::Constant::MathLog2E => Self::MathLog2E,
			crate::expr::Constant::MathNegInfinity => Self::MathNegInfinity,
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
