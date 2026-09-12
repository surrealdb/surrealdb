//! Time functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// No argument functions
//
// `time::now` reads the clock, which `EffectSet::CLOCK` declares:
// repeated calls disagree, whatever the arguments. The planner's literal folder
// still folds it once per statement, but through an explicit case of its own
// rather than through the determinism flag.
define_pure_function!(TimeNow, "time::now", effects: EffectSet::CLOCK, () -> Datetime, crate::fnc::time::now);

// Optional datetime argument functions (default to the current time)
//
// The default is what makes these clock readings, so they declare
// `EffectSet::CLOCK` too. The effect is a property of the name, not of a
// call: an argument that evaluates to `NONE` takes the default just as an
// omitted one does, and whether an expression can be `NONE` is not decidable
// where the rules ask. Given the argument they are pure, and narrowing the flag
// to calls whose argument cannot be `NONE` needs that argument's kind.
define_pure_function!(TimeDay, "time::day", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::day);
define_pure_function!(TimeHour, "time::hour", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::hour);
define_pure_function!(TimeMicros, "time::micros", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::micros);
define_pure_function!(TimeMillis, "time::millis", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::millis);
define_pure_function!(TimeMinute, "time::minute", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::minute);
define_pure_function!(TimeMonth, "time::month", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::month);
define_pure_function!(TimeNano, "time::nano", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::nano);
define_pure_function!(TimeSecond, "time::second", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::second);
// Reads the host's offset at the moment of the call, which a daylight-saving
// transition moves, so it reaches past the transaction's snapshot like the rest.
define_pure_function!(TimeTimezone, "time::timezone", effects: EffectSet::CLOCK, () -> String, crate::fnc::time::timezone);
define_pure_function!(TimeUnix, "time::unix", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::unix);
define_pure_function!(TimeWday, "time::wday", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::wday);
define_pure_function!(TimeWeek, "time::week", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::week);
define_pure_function!(TimeYday, "time::yday", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::yday);
define_pure_function!(TimeYear, "time::year", effects: EffectSet::CLOCK, (?value: Datetime) -> Int, crate::fnc::time::year);

// Two argument time functions
define_pure_function!(TimeCeil, "time::ceil", effects: EffectSet::PURE, (value: Datetime, duration: Duration) -> Datetime, crate::fnc::time::ceil);
define_pure_function!(TimeFloor, "time::floor", effects: EffectSet::PURE, (value: Datetime, duration: Duration) -> Datetime, crate::fnc::time::floor);
define_pure_function!(TimeFormat, "time::format", effects: EffectSet::PURE, (value: Datetime, format: String) -> String, crate::fnc::time::format);
define_pure_function!(TimeGroup, "time::group", effects: EffectSet::PURE, (value: Datetime, group: String) -> Datetime, crate::fnc::time::group);
define_pure_function!(TimeRound, "time::round", effects: EffectSet::PURE, (value: Datetime, duration: Duration) -> Datetime, crate::fnc::time::round);
define_pure_function!(TimeSetYear, "time::set_year", effects: EffectSet::PURE, (dt: Datetime, year: Number) -> Datetime, crate::fnc::time::set_year);
define_pure_function!(TimeSetMonth, "time::set_month", effects: EffectSet::PURE, (dt: Datetime, month: Number) -> Datetime, crate::fnc::time::set_month);
define_pure_function!(TimeSetDay, "time::set_day", effects: EffectSet::PURE, (dt: Datetime, day: Number) -> Datetime, crate::fnc::time::set_day);
define_pure_function!(TimeSetHour, "time::set_hour", effects: EffectSet::PURE, (dt: Datetime, hour: Number) -> Datetime, crate::fnc::time::set_hour);
define_pure_function!(TimeSetMinute, "time::set_minute", effects: EffectSet::PURE, (dt: Datetime, minute: Number) -> Datetime, crate::fnc::time::set_minute);
define_pure_function!(TimeSetSecond, "time::set_second", effects: EffectSet::PURE, (dt: Datetime, second: Number) -> Datetime, crate::fnc::time::set_second);
define_pure_function!(TimeSetNanosecond, "time::set_nanosecond", effects: EffectSet::PURE, (dt: Datetime, nanos: Number) -> Datetime, crate::fnc::time::set_nanosecond);

// Array argument functions
define_pure_function!(TimeMax, "time::max", effects: EffectSet::PURE, (array: Any) -> Datetime, crate::fnc::time::max);
define_pure_function!(TimeMin, "time::min", effects: EffectSet::PURE, (array: Any) -> Datetime, crate::fnc::time::min);

// Time from:: constructors
define_pure_function!(TimeFromMicros, "time::from_micros", effects: EffectSet::PURE, (value: Int) -> Datetime, crate::fnc::time::from::micros);
define_pure_function!(TimeFromMillis, "time::from_millis", effects: EffectSet::PURE, (value: Int) -> Datetime, crate::fnc::time::from::millis);
define_pure_function!(TimeFromNanos, "time::from_nanos", effects: EffectSet::PURE, (value: Int) -> Datetime, crate::fnc::time::from::nanos);
define_pure_function!(TimeFromSecs, "time::from_secs", effects: EffectSet::PURE, (value: Int) -> Datetime, crate::fnc::time::from::secs);
define_pure_function!(TimeFromUlid, "time::from_ulid", effects: EffectSet::PURE, (value: String) -> Datetime, crate::fnc::time::from::ulid);
define_pure_function!(TimeFromUnix, "time::from_unix", effects: EffectSet::PURE, (value: Int) -> Datetime, crate::fnc::time::from::unix);
define_pure_function!(TimeFromUuid, "time::from_uuid", effects: EffectSet::PURE, (value: Uuid) -> Datetime, crate::fnc::time::from::uuid);

// Time is:: functions
define_pure_function!(TimeIsLeapYear, "time::is_leap_year", effects: EffectSet::CLOCK, (?value: Datetime) -> Bool, crate::fnc::time::is::leap_year);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		TimeCeil,
		TimeDay,
		TimeFloor,
		TimeFormat,
		TimeFromMicros,
		TimeFromMillis,
		TimeFromNanos,
		TimeFromSecs,
		TimeFromUlid,
		TimeFromUnix,
		TimeFromUuid,
		TimeGroup,
		TimeHour,
		TimeIsLeapYear,
		TimeMax,
		TimeMicros,
		TimeMillis,
		TimeMin,
		TimeMinute,
		TimeMonth,
		TimeNano,
		TimeNow,
		TimeRound,
		TimeSecond,
		TimeTimezone,
		TimeUnix,
		TimeWday,
		TimeWeek,
		TimeYday,
		TimeYear,
		TimeSetYear,
		TimeSetMonth,
		TimeSetDay,
		TimeSetHour,
		TimeSetMinute,
		TimeSetSecond,
		TimeSetNanosecond
	);
}
