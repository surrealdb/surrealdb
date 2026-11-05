use anyhow::{Result, bail};
use chrono::offset::TimeZone;
use chrono::{DateTime, Datelike, Local, Timelike, Utc};
use surrealdb_expr::expr::Error as ExprError;
use surrealdb_expr::val::{Datetime, Duration, Number, Value};

use super::args::Optional;

const NANOS_PER_SEC: i128 = 1_000_000_000;

fn duration_span_nanos(duration: &Duration, fn_name: &str) -> Result<i128> {
	let Some(span) = chrono::Duration::from_std(**duration)
		.ok()
		.and_then(|d| d.num_nanoseconds())
		.filter(|&n| n >= 0)
	else {
		return Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
			name: String::from(fn_name),
			message: String::from(
				"The second argument must be a duration, and must be able to be represented as nanoseconds.",
			),
		}));
	};
	Ok(i128::from(span))
}

fn datetime_to_nanos(val: &Datetime) -> i128 {
	i128::from(val.timestamp()) * NANOS_PER_SEC + i128::from(val.timestamp_subsec_nanos())
}

fn nanos_to_datetime(nanos: i128, fn_name: &str) -> Result<Value> {
	let secs = i64::try_from(nanos.div_euclid(NANOS_PER_SEC)).ok();
	let subsec = nanos.rem_euclid(NANOS_PER_SEC) as u32;
	if let Some(dt) = secs.and_then(|s| DateTime::from_timestamp(s, subsec)) {
		Ok(Datetime::from(dt).into())
	} else {
		Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
			name: String::from(fn_name),
			message: String::from(
				"The second argument must be a duration, and must be able to be represented as nanoseconds.",
			),
		}))
	}
}

pub fn ceil((val, duration): (Datetime, Duration)) -> Result<Value> {
	let span = duration_span_nanos(&duration, "time::ceil")?;
	if span == 0 {
		return Ok(Value::Datetime(val));
	}
	let stamp = datetime_to_nanos(&val);
	let rem = stamp.rem_euclid(span);
	let target = if rem == 0 {
		stamp
	} else {
		stamp + (span - rem)
	};
	nanos_to_datetime(target, "time::ceil")
}

pub fn day((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.day().into(),
		None => Datetime::now().day().into(),
	})
}

pub fn floor((val, duration): (Datetime, Duration)) -> Result<Value> {
	let span = duration_span_nanos(&duration, "time::floor")?;
	if span == 0 {
		return Ok(Value::Datetime(val));
	}
	let stamp = datetime_to_nanos(&val);
	let rem = stamp.rem_euclid(span);
	nanos_to_datetime(stamp - rem, "time::floor")
}

pub fn format((val, format): (Datetime, String)) -> Result<Value> {
	use std::fmt::Write;
	let mut res = String::new();
	let Ok(()) = write!(&mut res, "{}", val.format(&format)) else {
		bail!(ExprError::InvalidMethodArguments {
			name: "time::format".to_owned(),
			message: format!("`{}` is not a valid time formatting string", format)
		});
	};
	Ok(res.into())
}

pub fn group((val, group): (Datetime, String)) -> Result<Value> {
	match group.as_str() {
		"year" => Ok(Utc
			.with_ymd_and_hms(val.year(), 1, 1, 0, 0, 0)
			.earliest()
			.expect("valid datetime")
			.into()),
		"month" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), 1, 0, 0, 0)
			.earliest()
			.expect("valid datetime")
			.into()),
		"day" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), 0, 0, 0)
			.earliest()
			.expect("valid datetime")
			.into()),
		"hour" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), val.hour(), 0, 0)
			.earliest()
			.expect("valid datetime")
			.into()),
		"minute" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), val.hour(), val.minute(), 0)
			.earliest()
			.expect("valid datetime")
			.into()),
		"second" => Ok(Utc
			.with_ymd_and_hms(
				val.year(),
				val.month(),
				val.day(),
				val.hour(),
				val.minute(),
				val.second(),
			)
			.earliest()
			.expect("valid datetime")
			.into()),
		_ => Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
			name: String::from("time::group"),
			message: String::from(
				"The second argument must be a string, and can be one of 'year', 'month', 'day', 'hour', 'minute', or 'second'.",
			),
		})),
	}
}

pub fn hour((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.hour().into(),
		None => Datetime::now().hour().into(),
	})
}

pub fn max((array,): (Vec<Datetime>,)) -> Result<Value> {
	Ok(match array.into_iter().max() {
		Some(v) => v.into(),
		None => Value::None,
	})
}

pub fn min((array,): (Vec<Datetime>,)) -> Result<Value> {
	Ok(match array.into_iter().min() {
		Some(v) => v.into(),
		None => Value::None,
	})
}

pub fn minute((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.minute().into(),
		None => Datetime::now().minute().into(),
	})
}

pub fn month((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.month().into(),
		None => Datetime::now().month().into(),
	})
}

pub fn nano((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	let val = val.unwrap_or_else(Datetime::now);
	val.timestamp_nanos_opt()
		.map(|x| x.into())
		.ok_or_else(|| ExprError::ArithmeticOverflow(format!("time::nano({val})")))
		.map_err(anyhow::Error::new)
}

pub fn millis((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.timestamp_millis().into(),
		None => Datetime::now().timestamp_millis().into(),
	})
}

pub fn micros((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.timestamp_micros().into(),
		None => Datetime::now().timestamp_micros().into(),
	})
}

pub fn now(_: ()) -> Result<Value> {
	Ok(Datetime::now().into())
}

pub fn round((val, duration): (Datetime, Duration)) -> Result<Value> {
	let span = duration_span_nanos(&duration, "time::round")?;
	if span == 0 {
		return Ok(Value::Datetime(val));
	}
	let stamp = datetime_to_nanos(&val);
	let rem = stamp.rem_euclid(span);
	let delta_down = rem;
	let delta_up = span - rem;
	let target = if delta_up <= delta_down {
		stamp + delta_up
	} else {
		stamp - delta_down
	};
	nanos_to_datetime(target, "time::round")
}

pub fn second((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.second().into(),
		None => Datetime::now().second().into(),
	})
}

pub fn timezone(_: ()) -> Result<Value> {
	Ok(Local::now().offset().to_string().into())
}

pub fn unix((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.timestamp().into(),
		None => Datetime::now().timestamp().into(),
	})
}

pub fn wday((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.weekday().number_from_monday().into(),
		None => Datetime::now().weekday().number_from_monday().into(),
	})
}

pub fn week((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.iso_week().week().into(),
		None => Datetime::now().iso_week().week().into(),
	})
}

pub fn yday((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.ordinal().into(),
		None => Datetime::now().ordinal().into(),
	})
}

pub fn year((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.year().into(),
		None => Datetime::now().year().into(),
	})
}

pub fn set_year((dt, year): (Datetime, Number)) -> Result<Value> {
	let Number::Int(year) = year else {
		return Err(anyhow::anyhow!("Unable to set datetime to year {year}"));
	};

	let dt =
		dt.0.with_year(i32::try_from(year)?)
			.ok_or_else(|| anyhow::anyhow!("Unable to set datetime to year {year}"))?;

	Ok(Value::Datetime(Datetime(dt)))
}

pub fn set_month((dt, month): (Datetime, Number)) -> Result<Value> {
	let Number::Int(month) = month else {
		return Err(anyhow::anyhow!("Unable to set datetime to month {month}"));
	};

	match dt.0.with_month(u32::try_from(month)?) {
		Some(dt) => Ok(Value::Datetime(Datetime(dt))),
		None => Err(anyhow::anyhow!("Unable to set datetime to month {month}")),
	}
}

pub fn set_day((dt, day): (Datetime, Number)) -> Result<Value> {
	let Number::Int(day) = day else {
		return Err(anyhow::anyhow!("Unable to set datetime to day {day}"));
	};
	match dt.0.with_day(u32::try_from(day)?) {
		Some(dt) => Ok(Value::Datetime(Datetime(dt))),
		None => Err(anyhow::anyhow!("Unable to set datetime to day {day}")),
	}
}

pub fn set_hour((dt, hour): (Datetime, Number)) -> Result<Value> {
	let Number::Int(hour) = hour else {
		return Err(anyhow::anyhow!("Unable to set datetime to hour {hour}"));
	};
	match dt.0.with_hour(u32::try_from(hour)?) {
		Some(dt) => Ok(Value::Datetime(Datetime(dt))),
		None => Err(anyhow::anyhow!("Unable to set datetime to hour {hour}")),
	}
}

pub fn set_minute((dt, minute): (Datetime, Number)) -> Result<Value> {
	let Number::Int(minute) = minute else {
		return Err(anyhow::anyhow!("Unable to set datetime to {minute} minutes"));
	};
	match dt.0.with_minute(u32::try_from(minute)?) {
		Some(dt) => Ok(Value::Datetime(Datetime(dt))),
		None => Err(anyhow::anyhow!("Unable to set datetime to {minute} minutes")),
	}
}

pub fn set_second((dt, second): (Datetime, Number)) -> Result<Value> {
	let Number::Int(second) = second else {
		return Err(anyhow::anyhow!("Unable to set datetime to {second} seconds"));
	};
	match dt.0.with_second(u32::try_from(second)?) {
		Some(dt) => Ok(Value::Datetime(Datetime(dt))),
		None => Err(anyhow::anyhow!("Unable to set datetime to {second} seconds")),
	}
}

pub fn set_nanosecond((dt, nanos): (Datetime, Number)) -> Result<Value> {
	let Number::Int(nanos) = nanos else {
		return Err(anyhow::anyhow!("Unable to set datetime to {nanos} nanoseconds"));
	};
	match dt.0.with_nanosecond(u32::try_from(nanos)?) {
		Some(dt) => Ok(Value::Datetime(Datetime(dt))),
		None => Err(anyhow::anyhow!("Unable to set datetime to {nanos} nanoseconds")),
	}
}

pub mod is {
	use anyhow::Result;
	use surrealdb_expr::val::{Datetime, Value};

	use crate::args::Optional;

	pub fn leap_year((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
		Ok(match val {
			Some(v) => v.naive_utc().date().leap_year().into(),
			None => Datetime::now().naive_utc().date().leap_year().into(),
		})
	}
}

pub mod from {

	use anyhow::Result;
	use chrono::DateTime;
	use common::fail;
	use surrealdb_expr::expr::Error as ExprError;
	use surrealdb_expr::val::{Datetime, Uuid, Value};
	use ulid::Ulid;

	pub fn nanos((val,): (i64,)) -> Result<Value> {
		const NANOS_PER_SEC: i64 = 1_000_000_000;

		let seconds = val.div_euclid(NANOS_PER_SEC);
		let nanoseconds = val.rem_euclid(NANOS_PER_SEC) as u32;

		match DateTime::from_timestamp(seconds, nanoseconds) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
				name: String::from("time::from_nanos"),
				message: String::from(
					"The argument must be a number of nanoseconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn micros((val,): (i64,)) -> Result<Value> {
		match DateTime::from_timestamp_micros(val) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
				name: String::from("time::from_micros"),
				message: String::from(
					"The argument must be a number of microseconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn millis((val,): (i64,)) -> Result<Value> {
		match DateTime::from_timestamp_millis(val) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
				name: String::from("time::from_millis"),
				message: String::from(
					"The argument must be a number of milliseconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn secs((val,): (i64,)) -> Result<Value> {
		match DateTime::from_timestamp(val, 0) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
				name: String::from("time::from_secs"),
				message: String::from(
					"The argument must be a number of seconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn unix((val,): (i64,)) -> Result<Value> {
		match DateTime::from_timestamp(val, 0) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
				name: String::from("time::from_unix"),
				message: String::from(
					"The argument must be a number of seconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn ulid((val,): (String,)) -> Result<Value> {
		match Ulid::from_string(&val) {
			Ok(v) => Ok(Datetime::from(DateTime::from(v.datetime())).into()),
			_ => Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
				name: String::from("time::from_ulid"),
				message: String::from(
					"The first argument must be a string, containing a valid ULID.",
				),
			})),
		}
	}

	pub fn uuid((val,): (Uuid,)) -> Result<Value> {
		match val.0.get_timestamp() {
			Some(v) => {
				// v1 and v6 count 100ns ticks from 1582-10-15, so the Unix offset is
				// subtracted in signed arithmetic to keep pre-1970 instants negative.
				let (secs, nanos) = if matches!(val.0.get_version_num(), 1 | 6) {
					let (ticks, _) = v.to_gregorian();
					let ticks =
						i128::from(ticks) - i128::from(uuid::timestamp::UUID_TICKS_BETWEEN_EPOCHS);
					(
						ticks.div_euclid(10_000_000) as i64,
						(ticks.rem_euclid(10_000_000) * 100) as u32,
					)
				} else {
					let (s, ns) = v.to_unix();
					match i64::try_from(s) {
						Ok(s) => (s, ns),
						Err(_) => fail!("Failed to convert UUID Timestamp to Datetime."),
					}
				};
				match Datetime::try_from((secs, nanos)) {
					Ok(v) => Ok(v.into()),
					_ => fail!("Failed to convert UUID Timestamp to Datetime."),
				}
			}
			None => Err(anyhow::Error::new(ExprError::InvalidFunctionArguments {
				name: String::from("time::from_uuid"),
				message: String::from("The first argument must be a v1, v6 or v7 UUID."),
			})),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration as StdDuration;

	use chrono::{TimeZone, Utc};
	use surrealdb_expr::val::{Datetime, Duration, Value};

	use super::{ceil, floor, round};

	#[test]
	fn floor_ceil_round_outside_i64_nanos_range() {
		let early = Datetime::from(Utc.with_ymd_and_hms(1500, 6, 15, 12, 34, 56).single().unwrap());
		let late = Datetime::from(Utc.with_ymd_and_hms(2400, 6, 15, 12, 34, 56).single().unwrap());
		let one_hour = Duration::from(StdDuration::from_secs(3600));

		assert_eq!(
			floor((early, one_hour)).unwrap(),
			Value::Datetime(Datetime::from(
				Utc.with_ymd_and_hms(1500, 6, 15, 12, 0, 0).single().unwrap()
			))
		);
		assert_eq!(
			ceil((early, one_hour)).unwrap(),
			Value::Datetime(Datetime::from(
				Utc.with_ymd_and_hms(1500, 6, 15, 13, 0, 0).single().unwrap()
			))
		);
		assert_eq!(
			round((early, one_hour)).unwrap(),
			Value::Datetime(Datetime::from(
				Utc.with_ymd_and_hms(1500, 6, 15, 13, 0, 0).single().unwrap()
			))
		);

		assert_eq!(
			floor((late, one_hour)).unwrap(),
			Value::Datetime(Datetime::from(
				Utc.with_ymd_and_hms(2400, 6, 15, 12, 0, 0).single().unwrap()
			))
		);
		assert_eq!(
			ceil((late, one_hour)).unwrap(),
			Value::Datetime(Datetime::from(
				Utc.with_ymd_and_hms(2400, 6, 15, 13, 0, 0).single().unwrap()
			))
		);
		assert_eq!(
			round((late, one_hour)).unwrap(),
			Value::Datetime(Datetime::from(
				Utc.with_ymd_and_hms(2400, 6, 15, 13, 0, 0).single().unwrap()
			))
		);
	}
}
