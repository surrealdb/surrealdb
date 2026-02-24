//! Time functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// No argument functions
define_pure_function!(TimeNow, "time::now", () -> Datetime, crate::fnc::time::now);

// Single datetime argument functions
define_pure_function!(TimeDay, "time::day", (value: Datetime) -> Int, crate::fnc::time::day);
define_pure_function!(TimeHour, "time::hour", (value: Datetime) -> Int, crate::fnc::time::hour);
define_pure_function!(TimeMicros, "time::micros", (value: Datetime) -> Int, crate::fnc::time::micros);
define_pure_function!(TimeMillis, "time::millis", (value: Datetime) -> Int, crate::fnc::time::millis);
define_pure_function!(TimeMinute, "time::minute", (value: Datetime) -> Int, crate::fnc::time::minute);
define_pure_function!(TimeMonth, "time::month", (value: Datetime) -> Int, crate::fnc::time::month);
define_pure_function!(TimeNano, "time::nano", (value: Datetime) -> Int, crate::fnc::time::nano);
define_pure_function!(TimeSecond, "time::second", (value: Datetime) -> Int, crate::fnc::time::second);
define_pure_function!(TimeTimezone, "time::timezone", (value: Datetime) -> String, crate::fnc::time::timezone);
define_pure_function!(TimeUnix, "time::unix", (value: Datetime) -> Int, crate::fnc::time::unix);
define_pure_function!(TimeWday, "time::wday", (value: Datetime) -> Int, crate::fnc::time::wday);
define_pure_function!(TimeWeek, "time::week", (value: Datetime) -> Int, crate::fnc::time::week);
define_pure_function!(TimeYday, "time::yday", (value: Datetime) -> Int, crate::fnc::time::yday);
define_pure_function!(TimeYear, "time::year", (value: Datetime) -> Int, crate::fnc::time::year);

// Two argument time functions
define_pure_function!(TimeCeil, "time::ceil", (value: Datetime, duration: Duration) -> Datetime, crate::fnc::time::ceil);
define_pure_function!(TimeFloor, "time::floor", (value: Datetime, duration: Duration) -> Datetime, crate::fnc::time::floor);
define_pure_function!(TimeFormat, "time::format", (value: Datetime, format: String) -> String, crate::fnc::time::format);
define_pure_function!(TimeGroup, "time::group", (value: Datetime, group: String) -> Datetime, crate::fnc::time::group);
define_pure_function!(TimeRound, "time::round", (value: Datetime, duration: Duration) -> Datetime, crate::fnc::time::round);

// Array argument functions
define_pure_function!(TimeMax, "time::max", (array: Any) -> Datetime, crate::fnc::time::max);
define_pure_function!(TimeMin, "time::min", (array: Any) -> Datetime, crate::fnc::time::min);

// Time from:: constructors
define_pure_function!(TimeFromMicros, "time::from_micros", (value: Int) -> Datetime, crate::fnc::time::from::micros);
define_pure_function!(TimeFromMillis, "time::from_millis", (value: Int) -> Datetime, crate::fnc::time::from::millis);
define_pure_function!(TimeFromNanos, "time::from_nanos", (value: Int) -> Datetime, crate::fnc::time::from::nanos);
define_pure_function!(TimeFromSecs, "time::from_secs", (value: Int) -> Datetime, crate::fnc::time::from::secs);
define_pure_function!(TimeFromUlid, "time::from_ulid", (value: String) -> Datetime, crate::fnc::time::from::ulid);
define_pure_function!(TimeFromUnix, "time::from_unix", (value: Int) -> Datetime, crate::fnc::time::from::unix);
define_pure_function!(TimeFromUuid, "time::from_uuid", (value: Uuid) -> Datetime, crate::fnc::time::from::uuid);

// Time is:: functions
define_pure_function!(TimeIsLeapYear, "time::is_leap_year", (value: Datetime) -> Bool, crate::fnc::time::is::leap_year);

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
	);
}
