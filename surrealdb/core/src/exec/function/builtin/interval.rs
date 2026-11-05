//! Interval functions

use crate::define_pure_function;
use crate::exec::function::{ScalarFunction, Signature};
use crate::expr::Kind;

// Construction
crate::__define_pure_scalar!(
	IntervalMake,
	"interval::make",
	effects: EffectSet::PURE,
	fallibility: Fallibility::VALUE,
	sig: Signature::new()
		.optional("years", Kind::Number)
		.optional("months", Kind::Number)
		.optional("weeks", Kind::Number)
		.optional("days", Kind::Number)
		.optional("hours", Kind::Number)
		.optional("mins", Kind::Number)
		.optional("secs", Kind::Number)
		.returns(Kind::Interval),
	metered: [],
	crate::fnc::interval::make
);

// Field extractors
define_pure_function!(IntervalYears, "interval::years", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Interval) -> Int, crate::fnc::interval::years);
define_pure_function!(IntervalMonths, "interval::months", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Interval) -> Int, crate::fnc::interval::months);
define_pure_function!(IntervalDays, "interval::days", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Interval) -> Int, crate::fnc::interval::days);
define_pure_function!(IntervalHours, "interval::hours", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Interval) -> Int, crate::fnc::interval::hours);
define_pure_function!(IntervalMins, "interval::mins", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Interval) -> Int, crate::fnc::interval::mins);
define_pure_function!(IntervalSecs, "interval::secs", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Interval) -> Decimal, crate::fnc::interval::secs);
define_pure_function!(IntervalMicros, "interval::micros", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Interval) -> Int, crate::fnc::interval::micros);

// Normalisation
define_pure_function!(IntervalJustifyHours, "interval::justify_hours", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Interval) -> Interval, crate::fnc::interval::justify_hours);
define_pure_function!(IntervalJustifyDays, "interval::justify_days", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Interval) -> Interval, crate::fnc::interval::justify_days);
define_pure_function!(IntervalJustifyInterval, "interval::justify_interval", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Interval) -> Interval, crate::fnc::interval::justify_interval);

/// The `interval::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&IntervalDays,
	&IntervalHours,
	&IntervalJustifyDays,
	&IntervalJustifyHours,
	&IntervalJustifyInterval,
	&IntervalMake,
	&IntervalMicros,
	&IntervalMins,
	&IntervalMonths,
	&IntervalSecs,
	&IntervalYears,
];
