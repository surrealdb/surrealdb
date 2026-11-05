//! Time functions
//!
//! The CLOCK builtins (`EffectSet::CLOCK`) read one timestamp per statement,
//! through [`statement_clock`]: repeated reads inside a statement agree,
//! whatever the row or nested frame they run in, and no builtin folds or
//! caches the value itself. A top-level statement takes its instant when the
//! executor starts it, and every statement of a block, of the body of a `FOR`
//! or an `IF`, and of a function takes one when it starts, so the clock
//! advances from one statement of a list to the next. They are
//! context-reading functions, so the executor calls them through
//! `invoke_async` with the `EvalContext`, and their synchronous `invoke` is
//! an error.

use anyhow::Result;
use chrono::{Local, TimeZone};

use crate::define_pure_function;
use crate::exec::EvalContext;
use crate::exec::function::{ScalarFunction, Signature};
use crate::expr::Kind;
use crate::fnc::args::Optional;
use crate::val::{Datetime, Value};

/// The timestamp every CLOCK builtin reads as the current time: that of the
/// statement `ctx` runs in, taken when it started, so every read in one
/// statement agrees. A `sleep` inside the statement does not advance it.
///
/// A context that carries no statement timestamp answers the wall clock at the
/// call.
pub(crate) fn statement_clock(ctx: &EvalContext<'_>) -> Datetime {
	match ctx.exec_ctx.ctx().statement_clock() {
		Some(clock) => *clock,
		None => Datetime::now(),
	}
}

/// `time::now`: the statement's instant.
fn now_at(clock: Datetime) -> Result<Value> {
	Ok(clock.into())
}

/// `time::timezone`: the host's UTC offset at the statement's instant, so a
/// daylight-saving change inside one statement does not change the answer.
pub(crate) fn timezone_at(clock: Datetime) -> Result<Value> {
	Ok(Local.offset_from_utc_datetime(&clock.naive_utc()).to_string().into())
}

/// Define a CLOCK builtin as a context-reading scalar function.
///
/// Two shapes cover the family. `() -> Ret, f` calls `f(statement_clock(ctx))`.
/// `(?name: Datetime) -> Ret, f` decodes the optional datetime exactly as
/// `define_pure_function!` does, substitutes `statement_clock(ctx)` when it is
/// omitted, and calls the `fnc::time` function `f` with it, so a given
/// argument is read as itself. Both declare `EffectSet::CLOCK`, report
/// `is_pure() == false` so the executor passes the context, and refuse the
/// context-free `invoke`.
macro_rules! define_clock_function {
	// No argument: () -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		fallibility: $fallibility:expr,
		() -> $ret:ident,
		$impl_path:path
	) => {
		#[allow(non_upper_case_globals)]
		pub const $struct_name: $crate::exec::function::BuiltinScalarFn =
			$crate::exec::function::BuiltinScalarFn {
				name: $func_name,
				effects: $crate::expr::logical::EffectSet::CLOCK,
				fallibility: {
					#[allow(unused_imports)]
					use $crate::exec::ir::iface::Fallibility;
					$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
				},
				closure_args: &[],
				signature: || {
					$crate::exec::function::Signature::new().returns($crate::expr::Kind::$ret)
				},
				exec: $crate::exec::function::ScalarExec::Context(|ctx, args| {
					$crate::exec::function::check_arity($func_name, &args, 0, Some(0))?;
					$impl_path(statement_clock(ctx))
				}),
			};
	};

	// One optional datetime: (?name: Datetime) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		fallibility: $fallibility:expr,
		(? $arg_name:ident : Datetime) -> $ret:ident,
		$impl_path:path
	) => {
		#[allow(non_upper_case_globals)]
		pub const $struct_name: $crate::exec::function::BuiltinScalarFn =
			$crate::exec::function::BuiltinScalarFn {
				name: $func_name,
				effects: $crate::expr::logical::EffectSet::CLOCK,
				fallibility: {
					#[allow(unused_imports)]
					use $crate::exec::ir::iface::Fallibility;
					$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
				},
				closure_args: &[],
				signature: || {
					$crate::exec::function::Signature::new()
						.optional(stringify!($arg_name), $crate::expr::Kind::Datetime)
						.returns($crate::expr::Kind::$ret)
				},
				exec: $crate::exec::function::ScalarExec::Context(|ctx, args| {
					let (Optional(value),): (Optional<Datetime>,) =
						$crate::fnc::args::FromArgs::from_args($func_name, args)?;
					let value = value.unwrap_or_else(|| statement_clock(ctx));
					$impl_path((Optional(Some(value)),))
				}),
			};
	};

	// One optional temporal argument: (?name: Temporal [Kind, ..]) -> ReturnType. The argument
	// may be any of the listed kinds; omitting it reads the clock as a datetime.
	(
		$struct_name:ident,
		$func_name:literal,
		fallibility: $fallibility:expr,
		(? $arg_name:ident : Temporal [$($kind:ident),+]) -> $ret:ident,
		$impl_path:path
	) => {
		#[allow(non_upper_case_globals)]
		pub const $struct_name: $crate::exec::function::BuiltinScalarFn =
			$crate::exec::function::BuiltinScalarFn {
				name: $func_name,
				effects: $crate::expr::logical::EffectSet::CLOCK,
				fallibility: {
					#[allow(unused_imports)]
					use $crate::exec::ir::iface::Fallibility;
					$crate::declare_fallibility!(Scalar, $func_name, $fallibility)
				},
				closure_args: &[],
				signature: || {
					$crate::exec::function::Signature::new()
						.optional(
							stringify!($arg_name),
							$crate::expr::Kind::Either(vec![$($crate::expr::Kind::$kind),+]),
						)
						.returns($crate::expr::Kind::$ret)
				},
				exec: $crate::exec::function::ScalarExec::Context(|ctx, args| {
					let (Optional(value),): (Optional<Value>,) =
						$crate::fnc::args::FromArgs::from_args($func_name, args)?;
					let value = value.unwrap_or_else(|| Value::Datetime(statement_clock(ctx)));
					$impl_path((Optional(Some(value)),))
				}),
			};
	};
}

// No argument functions
//
// `time::now` and `time::timezone` read the statement's instant.
define_clock_function!(TimeNow, "time::now", fallibility: Fallibility::INFALLIBLE, () -> Datetime, now_at);
// The host's UTC offset changes at daylight-saving transitions, so
// `time::timezone` is not fixed by the transaction's snapshot, like the other clock reads.
define_clock_function!(TimeTimezone, "time::timezone", fallibility: Fallibility::INFALLIBLE, () -> String, timezone_at);

// Optional datetime argument functions (default to the statement's instant)
//
// Omitting the argument makes these read the clock, so they declare
// `EffectSet::CLOCK` too. The effect belongs to the function name: with an
// argument they read only that value. Setting the effect per call would need
// the call's arity, which a declaration does not see.
define_clock_function!(TimeDay, "time::day", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Date, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::day);
define_clock_function!(TimeHour, "time::hour", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Time, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::hour);
define_clock_function!(TimeMicros, "time::micros", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Time, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::micros);
define_clock_function!(TimeMillis, "time::millis", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Time, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::millis);
define_clock_function!(TimeMinute, "time::minute", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Time, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::minute);
define_clock_function!(TimeMonth, "time::month", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Date, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::month);
define_clock_function!(TimeNano, "time::nano", fallibility: Fallibility::VALUE, (?value: Temporal [Time, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::nano);
define_clock_function!(TimeSecond, "time::second", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Time, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::second);
define_clock_function!(TimeUnix, "time::unix", fallibility: Fallibility::INFALLIBLE, (?value: Datetime) -> Int, crate::fnc::time::unix);
define_clock_function!(TimeWday, "time::wday", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Date, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::wday);
define_clock_function!(TimeWeek, "time::week", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Date, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::week);
define_clock_function!(TimeYday, "time::yday", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Date, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::yday);
define_clock_function!(TimeYear, "time::year", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Date, Timestamp, Datetime]) -> Int, crate::fnc::time_kinds::year);

// Two argument time functions
define_pure_function!(TimeCeil, "time::ceil", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (value: Any, duration: Duration) -> Any, crate::fnc::time_kinds::ceil);
define_pure_function!(TimeFloor, "time::floor", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (value: Any, duration: Duration) -> Any, crate::fnc::time_kinds::floor);
define_pure_function!(TimeFormat, "time::format", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any, format: String) -> String, crate::fnc::time_kinds::format);
define_pure_function!(TimeGroup, "time::group", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (value: Any, group: String) -> Any, crate::fnc::time_kinds::group);
define_pure_function!(TimeRound, "time::round", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (value: Any, duration: Duration) -> Any, crate::fnc::time_kinds::round);
define_pure_function!(TimeSetYear, "time::set_year", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (dt: Any, year: Number) -> Any, crate::fnc::time_kinds::set_year);
define_pure_function!(TimeSetMonth, "time::set_month", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (dt: Any, month: Number) -> Any, crate::fnc::time_kinds::set_month);
define_pure_function!(TimeSetDay, "time::set_day", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (dt: Any, day: Number) -> Any, crate::fnc::time_kinds::set_day);
define_pure_function!(TimeSetHour, "time::set_hour", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (dt: Any, hour: Number) -> Any, crate::fnc::time_kinds::set_hour);
define_pure_function!(TimeSetMinute, "time::set_minute", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (dt: Any, minute: Number) -> Any, crate::fnc::time_kinds::set_minute);
define_pure_function!(TimeSetSecond, "time::set_second", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (dt: Any, second: Number) -> Any, crate::fnc::time_kinds::set_second);
define_pure_function!(TimeSetNanosecond, "time::set_nanosecond", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (dt: Any, nanos: Number) -> Any, crate::fnc::time_kinds::set_nanosecond);

// Array argument functions
define_pure_function!(TimeMax, "time::max", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::ArrayElement(0), (array: Any) -> Any, crate::fnc::time_kinds::max);
define_pure_function!(TimeMin, "time::min", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::ArrayElement(0), (array: Any) -> Any, crate::fnc::time_kinds::min);

// Time from:: constructors
define_pure_function!(TimeFromMicros, "time::from_micros", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Datetime, crate::fnc::time::from::micros);
define_pure_function!(TimeFromMillis, "time::from_millis", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Datetime, crate::fnc::time::from::millis);
define_pure_function!(TimeFromNanos, "time::from_nanos", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Datetime, crate::fnc::time::from::nanos);
define_pure_function!(TimeFromSecs, "time::from_secs", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Datetime, crate::fnc::time::from::secs);
define_pure_function!(TimeFromUlid, "time::from_ulid", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: String) -> Datetime, crate::fnc::time::from::ulid);
define_pure_function!(TimeFromUnix, "time::from_unix", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Int) -> Datetime, crate::fnc::time::from::unix);
define_pure_function!(TimeFromUuid, "time::from_uuid", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Uuid) -> Datetime, crate::fnc::time::from::uuid);

// Constructors and calendar-aware functions over the temporal kinds
define_pure_function!(TimeMakeDate, "time::make_date", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (year: Number, month: Number, day: Number) -> Date, crate::fnc::time_kinds::make_date);
define_pure_function!(TimeMakeTime, "time::make_time", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (hour: Number, min: Number, sec: Number) -> Time, crate::fnc::time_kinds::make_time);
crate::__define_pure_scalar!(
	TimeMakeTimestamp,
	"time::make_timestamp",
	effects: EffectSet::PURE,
	fallibility: Fallibility::VALUE,
	sig: Signature::new()
		.arg("year", Kind::Number)
		.arg("month", Kind::Number)
		.arg("day", Kind::Number)
		.arg("hour", Kind::Number)
		.arg("min", Kind::Number)
		.arg("sec", Kind::Number)
		.returns(Kind::Timestamp),
	metered: [],
	crate::fnc::time_kinds::make_timestamp
);
define_pure_function!(TimeAge, "time::age", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Interval, crate::fnc::time_kinds::age);
define_pure_function!(TimeExtract, "time::extract", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any, field: String) -> Number, crate::fnc::time_kinds::extract);
define_pure_function!(TimeTrunc, "time::trunc", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (value: Any, unit: String) -> Any, crate::fnc::time_kinds::trunc);

// Time is:: functions
define_clock_function!(TimeIsLeapYear, "time::is_leap_year", fallibility: Fallibility::INFALLIBLE, (?value: Temporal [Date, Timestamp, Datetime]) -> Bool, crate::fnc::time_kinds::is_leap_year);

/// The `time::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&TimeAge,
	&TimeExtract,
	&TimeMakeDate,
	&TimeMakeTime,
	&TimeMakeTimestamp,
	&TimeTrunc,
	&TimeCeil,
	&TimeDay,
	&TimeFloor,
	&TimeFormat,
	&TimeFromMicros,
	&TimeFromMillis,
	&TimeFromNanos,
	&TimeFromSecs,
	&TimeFromUlid,
	&TimeFromUnix,
	&TimeFromUuid,
	&TimeGroup,
	&TimeHour,
	&TimeIsLeapYear,
	&TimeMax,
	&TimeMicros,
	&TimeMillis,
	&TimeMin,
	&TimeMinute,
	&TimeMonth,
	&TimeNano,
	&TimeNow,
	&TimeRound,
	&TimeSecond,
	&TimeTimezone,
	&TimeUnix,
	&TimeWday,
	&TimeWeek,
	&TimeYday,
	&TimeYear,
	&TimeSetYear,
	&TimeSetMonth,
	&TimeSetDay,
	&TimeSetHour,
	&TimeSetMinute,
	&TimeSetSecond,
	&TimeSetNanosecond,
];

#[cfg(test)]
mod tests {
	use super::statement_clock;
	use crate::ctx::Context;
	use crate::exec::EvalContext;
	use crate::exec::function::registry::FALLIBILITY;
	use crate::exec::function::{FnKind, FunctionRegistry};
	use crate::exec::operators::test_util::TestDb;
	use crate::expr::logical::EffectSet;
	use crate::val::{Datetime, Value};

	/// A clock builtin answers the instant the context carries, whenever it is
	/// called, and a datetime it is given is read as itself.
	#[tokio::test]
	async fn a_clock_builtin_answers_the_instant_the_context_carries() {
		let db = TestDb::new("").await;
		let exec = db.exec_ctx().await;
		let at: Datetime = "2025-03-10T12:00:00Z".parse().expect("a datetime");
		let mut ctx = Context::new_child(exec.ctx());
		ctx.set_statement_clock(at);
		let exec = exec.with_new_ctx(ctx.freeze());
		let eval = EvalContext::from_exec_ctx(&exec);
		assert_eq!(statement_clock(&eval), at);

		let registry = FunctionRegistry::with_builtins();
		let call = |name: &'static str, args: Vec<Value>| {
			let function = registry.get(name).expect("a registered builtin");
			let eval = &eval;
			async move { function.invoke_async(eval, args).await.expect("the call answers") }
		};
		assert_eq!(call("time::now", vec![]).await, Value::Datetime(at));
		assert_eq!(call("time::year", vec![]).await, Value::from(2025));
		let given: Datetime = "2001-01-01T00:00:00Z".parse().expect("a datetime");
		assert_eq!(call("time::year", vec![Value::Datetime(given)]).await, Value::from(2001));
		exec.txn().cancel().await.expect("cancel");
	}

	/// The CLOCK builtins are context-reading: none is pure, none runs
	/// through the synchronous `invoke`, and each keeps the fallibility class
	/// its table row states.
	#[test]
	fn every_clock_builtin_reads_the_context() {
		let registry = FunctionRegistry::with_builtins();
		let mut clocks: Vec<_> =
			registry.scalar_functions().filter(|f| f.effects() == EffectSet::CLOCK).collect();
		clocks.sort_by_key(|f| f.name());
		assert_eq!(clocks.len(), 16, "the sixteen `time::*` clock reads");
		for func in clocks {
			let name = func.name();
			assert!(name.starts_with("time::"), "{name}: a clock read outside `time::`");
			assert!(!func.is_pure(), "{name}: a clock read needs the context");
			assert!(!func.is_async(), "{name}: a clock read does not wait");
			let err = func.invoke(Vec::new()).expect_err("the synchronous path has no context");
			assert!(err.to_string().contains("requires context"), "{name}: {err}");
			let row = FALLIBILITY
				.iter()
				.find(|(kind, row, _)| *kind == FnKind::Scalar && *row == name)
				.map(|(_, _, class)| *class)
				.expect("a FALLIBILITY row");
			assert!(func.fallibility() == row, "{name}: fallibility differs from its row");
		}
	}
}
