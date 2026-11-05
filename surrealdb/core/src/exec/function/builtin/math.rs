//! Math functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

// Single argument math functions
define_pure_function!(MathAbs, "math::abs", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::abs);
define_pure_function!(MathAcos, "math::acos", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::acos);
define_pure_function!(MathAcot, "math::acot", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::acot);
define_pure_function!(MathAsin, "math::asin", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::asin);
define_pure_function!(MathAtan, "math::atan", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::atan);
define_pure_function!(MathCeil, "math::ceil", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Number) -> Number, crate::fnc::math::ceil);
define_pure_function!(MathCos, "math::cos", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::cos);
define_pure_function!(MathCot, "math::cot", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::cot);
define_pure_function!(MathDeg2rad, "math::deg2rad", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::deg2rad);
define_pure_function!(MathFloor, "math::floor", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Number) -> Number, crate::fnc::math::floor);
define_pure_function!(MathLn, "math::ln", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::ln);
define_pure_function!(MathLog10, "math::log10", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::log10);
define_pure_function!(MathLog2, "math::log2", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::log2);
define_pure_function!(MathRad2deg, "math::rad2deg", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::rad2deg);
define_pure_function!(MathRound, "math::round", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Number) -> Number, crate::fnc::math::round);
define_pure_function!(MathSign, "math::sign", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Number) -> Number, crate::fnc::math::sign);
define_pure_function!(MathSin, "math::sin", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::sin);
define_pure_function!(MathSqrt, "math::sqrt", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::sqrt);
define_pure_function!(MathTan, "math::tan", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number) -> Number, crate::fnc::math::tan);

// Two argument math functions
define_pure_function!(MathBottom, "math::bottom", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::bottom_walk, (array: Any, count: Int) -> Any, crate::fnc::math::bottom);
define_pure_function!(MathFixed, "math::fixed", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number, precision: Int) -> Number, crate::fnc::math::fixed);
define_pure_function!(MathLog, "math::log", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number, base: Number) -> Number, crate::fnc::math::log);
define_pure_function!(MathPow, "math::pow", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (base: Number, exponent: Number) -> Number, crate::fnc::math::pow);
define_pure_function!(MathTop, "math::top", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::top_walk, (array: Any, count: Int) -> Any, crate::fnc::math::top);

// Three argument math functions
define_pure_function!(MathClamp, "math::clamp", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Number, min: Number, max: Number) -> Number, crate::fnc::math::clamp);
define_pure_function!(MathLerp, "math::lerp", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Number, b: Number, t: Number) -> Number, crate::fnc::math::lerp);
define_pure_function!(MathLerpangle, "math::lerpangle", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Number, b: Number, t: Number) -> Number, crate::fnc::math::lerpangle);

// Array aggregate math functions (operate on array of numbers)
define_pure_function!(MathInterquartile, "math::interquartile", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::interquartile_walk, (array: Any) -> Number, crate::fnc::math::interquartile);
define_pure_function!(MathMax, "math::max", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::NumericAggregate(0), metered: crate::fnc::math::max_walk, (array: Any) -> Number, crate::fnc::math::max);
define_pure_function!(MathMean, "math::mean", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::mean_walk, (array: Any) -> Number, crate::fnc::math::mean);
define_pure_function!(MathMedian, "math::median", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::median_walk, (array: Any) -> Number, crate::fnc::math::median);
define_pure_function!(MathMidhinge, "math::midhinge", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::midhinge_walk, (array: Any) -> Number, crate::fnc::math::midhinge);
define_pure_function!(MathMin, "math::min", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::NumericAggregate(0), metered: crate::fnc::math::min_walk, (array: Any) -> Number, crate::fnc::math::min);
define_pure_function!(MathMode, "math::mode", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::NumericAggregate(0), metered: crate::fnc::math::mode_walk, (array: Any) -> Number, crate::fnc::math::mode);
define_pure_function!(MathProduct, "math::product", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::NumericAggregate(0), metered: crate::fnc::math::product_walk, (array: Any) -> Number, crate::fnc::math::product);
define_pure_function!(MathSpread, "math::spread", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::spread_walk, (array: Any) -> Number, crate::fnc::math::spread);
define_pure_function!(MathStddev, "math::stddev", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::stddev_walk, (array: Any) -> Number, crate::fnc::math::stddev);
define_pure_function!(MathSum, "math::sum", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::NumericAggregate(0), metered: crate::fnc::math::sum_walk, (array: Any) -> Number, crate::fnc::math::sum);
define_pure_function!(MathTrimean, "math::trimean", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::trimean_walk, (array: Any) -> Number, crate::fnc::math::trimean);
define_pure_function!(MathVariance, "math::variance", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::variance_walk, (array: Any) -> Number, crate::fnc::math::variance);

// Two argument array aggregate functions
define_pure_function!(MathNearestrank, "math::nearestrank", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::nearestrank_walk, (array: Any, percentile: Number) -> Number, crate::fnc::math::nearestrank);
define_pure_function!(MathPercentile, "math::percentile", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::math::percentile_walk, (array: Any, percentile: Number) -> Number, crate::fnc::math::percentile);

/// The `math::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&MathAbs,
	&MathAcos,
	&MathAcot,
	&MathAsin,
	&MathAtan,
	&MathBottom,
	&MathCeil,
	&MathClamp,
	&MathCos,
	&MathCot,
	&MathDeg2rad,
	&MathFixed,
	&MathFloor,
	&MathInterquartile,
	&MathLerp,
	&MathLerpangle,
	&MathLn,
	&MathLog,
	&MathLog10,
	&MathLog2,
	&MathMax,
	&MathMean,
	&MathMedian,
	&MathMidhinge,
	&MathMin,
	&MathMode,
	&MathNearestrank,
	&MathPercentile,
	&MathPow,
	&MathProduct,
	&MathRad2deg,
	&MathRound,
	&MathSign,
	&MathSin,
	&MathSpread,
	&MathSqrt,
	&MathStddev,
	&MathSum,
	&MathTan,
	&MathTop,
	&MathTrimean,
	&MathVariance,
];
