//! Duration functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

// Duration component extractors
define_pure_function!(DurationDays, "duration::days", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Duration) -> Int, crate::fnc::duration::days);
define_pure_function!(DurationHours, "duration::hours", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Duration) -> Int, crate::fnc::duration::hours);
define_pure_function!(DurationMicros, "duration::micros", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Duration) -> Int, crate::fnc::duration::micros);
define_pure_function!(DurationMillis, "duration::millis", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Duration) -> Int, crate::fnc::duration::millis);
define_pure_function!(DurationMins, "duration::mins", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Duration) -> Int, crate::fnc::duration::mins);
define_pure_function!(DurationNanos, "duration::nanos", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Duration) -> Int, crate::fnc::duration::nanos);
define_pure_function!(DurationSecs, "duration::secs", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Duration) -> Int, crate::fnc::duration::secs);
define_pure_function!(DurationWeeks, "duration::weeks", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Duration) -> Int, crate::fnc::duration::weeks);
define_pure_function!(DurationYears, "duration::years", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Duration) -> Int, crate::fnc::duration::years);

// Duration constructors
define_pure_function!(DurationFromDays, "duration::from_days", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Duration, crate::fnc::duration::from::days);
define_pure_function!(DurationFromHours, "duration::from_hours", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Duration, crate::fnc::duration::from::hours);
define_pure_function!(DurationFromMicros, "duration::from_micros", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Duration, crate::fnc::duration::from::micros);
define_pure_function!(DurationFromMillis, "duration::from_millis", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Duration, crate::fnc::duration::from::millis);
define_pure_function!(DurationFromMins, "duration::from_mins", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Duration, crate::fnc::duration::from::mins);
define_pure_function!(DurationFromNanos, "duration::from_nanos", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Duration, crate::fnc::duration::from::nanos);
define_pure_function!(DurationFromSecs, "duration::from_secs", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Duration, crate::fnc::duration::from::secs);
define_pure_function!(DurationFromWeeks, "duration::from_weeks", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Duration, crate::fnc::duration::from::weeks);

/// The `duration::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&DurationDays,
	&DurationFromDays,
	&DurationFromHours,
	&DurationFromMicros,
	&DurationFromMillis,
	&DurationFromMins,
	&DurationFromNanos,
	&DurationFromSecs,
	&DurationFromWeeks,
	&DurationHours,
	&DurationMicros,
	&DurationMillis,
	&DurationMins,
	&DurationNanos,
	&DurationSecs,
	&DurationWeeks,
	&DurationYears,
];
