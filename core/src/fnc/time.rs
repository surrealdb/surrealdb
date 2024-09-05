use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::duration::Duration;
use crate::sql::value::Value;
use chrono::offset::TimeZone;
use chrono::{DateTime, Datelike, DurationRound, Local, Timelike, Utc};

pub fn ceil((val, duration): (Datetime, Duration)) -> Result<Value, Error> {
	match chrono::Duration::from_std(*duration) {
		Ok(d) => {
			let floor_to_ceil = |floor: DateTime<Utc>| -> Option<DateTime<Utc>> {
				if floor == *val {
					Some(floor)
				} else {
					floor.checked_add_signed(d)
				}
			};
			// Check for zero duration.
			if d.is_zero() {
				return Ok(Value::Datetime(val));
			}
			let result = val
				.duration_trunc(d)
				.ok()
				.and_then(floor_to_ceil);

			match result {
				Some(v) => Ok(v.into()),
				_ => Err(Error::InvalidArguments {
					name: String::from("time::ceil"),
					message: String::from("The second argument must be a duration, and must be able to be represented as nanoseconds."),
				}),
			}
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("time::ceil"),
			message: String::from("The second argument must be a duration, and must be able to be represented as nanoseconds."),
		}),
	}
}

pub fn day((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.day().into(),
		None => Datetime::default().day().into(),
	})
}

pub fn floor((val, duration): (Datetime, Duration)) -> Result<Value, Error> {
	match chrono::Duration::from_std(*duration) {
		Ok(d) => {
			// Check for zero duration
			if d.is_zero() {
				return Ok(Value::Datetime(val));
			}
			match val.duration_trunc(d){
				Ok(v) => Ok(v.into()),
				_ => Err(Error::InvalidArguments {
					name: String::from("time::floor"),
					message: String::from("The second argument must be a duration, and must be able to be represented as nanoseconds."),
				}),
			}
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("time::floor"),
			message: String::from("The second argument must be a duration, and must be able to be represented as nanoseconds."),
		}),
	}
}

pub fn format((val, format): (Datetime, String)) -> Result<Value, Error> {
	Ok(val.format(&format).to_string().into())
}

pub fn group((val, group): (Datetime, String)) -> Result<Value, Error> {
	match group.as_str() {
		"year" => Ok(Utc
			.with_ymd_and_hms(val.year(), 1, 1, 0,0,0)
			.earliest()
			.unwrap()
			.into()),
		"month" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), 1, 0,0,0)
			.earliest()
			.unwrap()
			.into()),
		"day" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), 0,0,0)
			.earliest()
			.unwrap()
			.into()),
		"hour" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), val.hour(),0,0)
			.earliest()
			.unwrap()
			.into()),
		"minute" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), val.hour(), val.minute(),0)
			.earliest()
			.unwrap()
			.into()),
		"second" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), val.hour(), val.minute(), val.second())
			.earliest()
			.unwrap()
			.into()),
		_ => Err(Error::InvalidArguments {
			name: String::from("time::group"),
			message: String::from("The second argument must be a string, and can be one of 'year', 'month', 'day', 'hour', 'minute', or 'second'."),
		}),
	}
}

pub fn hour((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.hour().into(),
		None => Datetime::default().hour().into(),
	})
}

pub fn max((array,): (Vec<Datetime>,)) -> Result<Value, Error> {
	Ok(match array.into_iter().max() {
		Some(v) => v.into(),
		None => Value::None,
	})
}

pub fn min((array,): (Vec<Datetime>,)) -> Result<Value, Error> {
	Ok(match array.into_iter().min() {
		Some(v) => v.into(),
		None => Value::None,
	})
}

pub fn minute((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.minute().into(),
		None => Datetime::default().minute().into(),
	})
}

pub fn month((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.month().into(),
		None => Datetime::default().month().into(),
	})
}

pub fn nano((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.timestamp_nanos_opt().unwrap_or_default().into(),
		None => Datetime::default().timestamp_nanos_opt().unwrap_or_default().into(),
	})
}

pub fn millis((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.timestamp_millis().into(),
		None => Datetime::default().timestamp_millis().into(),
	})
}

pub fn micros((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.timestamp_micros().into(),
		None => Datetime::default().timestamp_micros().into(),
	})
}

pub fn now(_: ()) -> Result<Value, Error> {
	Ok(Datetime::default().into())
}

pub fn round((val, duration): (Datetime, Duration)) -> Result<Value, Error> {
	match chrono::Duration::from_std(*duration) {
		Ok(d) => {
			// Check for zero duration
			if d.is_zero() {
				return Ok(Value::Datetime(val));
			}
			match val.duration_round(d) {
				Ok(v) => Ok(v.into()),
				_ => Err(Error::InvalidArguments {
					name: String::from("time::round"),
					message: String::from("The second argument must be a duration, and must be able to be represented as nanoseconds."),
				}),
			}
		},
		_ => Err(Error::InvalidArguments {
			name: String::from("time::round"),
			message: String::from("The second argument must be a duration, and must be able to be represented as nanoseconds."),
		}),
	}
}

pub fn second((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.second().into(),
		None => Datetime::default().second().into(),
	})
}

pub fn timezone(_: ()) -> Result<Value, Error> {
	Ok(Local::now().offset().to_string().into())
}

pub fn unix((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.timestamp().into(),
		None => Datetime::default().timestamp().into(),
	})
}

pub fn wday((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.weekday().number_from_monday().into(),
		None => Datetime::default().weekday().number_from_monday().into(),
	})
}

pub fn week((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.iso_week().week().into(),
		None => Datetime::default().iso_week().week().into(),
	})
}

pub fn yday((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.ordinal().into(),
		None => Datetime::default().ordinal().into(),
	})
}

pub fn year((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.year().into(),
		None => Datetime::default().year().into(),
	})
}

pub mod is {
	use crate::err::Error;
	use crate::sql::{Datetime, Value};

	pub fn leap_year((val,): (Option<Datetime>,)) -> Result<Value, Error> {
		Ok(match val {
			Some(v) => v.naive_utc().date().leap_year().into(),
			None => Datetime::default().naive_utc().date().leap_year().into(),
		})
	}
}

pub mod from {

	use crate::err::Error;
	use crate::sql::datetime::Datetime;
	use crate::sql::value::Value;
	use chrono::DateTime;

	pub fn nanos((val,): (i64,)) -> Result<Value, Error> {
		const NANOS_PER_SEC: i64 = 1_000_000_000;

		let seconds = val.div_euclid(NANOS_PER_SEC);
		let nanoseconds = val.rem_euclid(NANOS_PER_SEC) as u32;

		match DateTime::from_timestamp(seconds, nanoseconds) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::nanos"),
				message: String::from("The first argument must be an in-bounds number of nanoseconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}

	pub fn micros((val,): (i64,)) -> Result<Value, Error> {
		match DateTime::from_timestamp_micros(val) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::micros"),
				message: String::from("The first argument must be an in-bounds number of microseconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}

	pub fn millis((val,): (i64,)) -> Result<Value, Error> {
		match DateTime::from_timestamp_millis(val) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::millis"),
				message: String::from("The first argument must be an in-bounds number of milliseconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}

	pub fn secs((val,): (i64,)) -> Result<Value, Error> {
		match DateTime::from_timestamp(val, 0) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::secs"),
				message: String::from("The first argument must be an in-bounds number of seconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}

	pub fn unix((val,): (i64,)) -> Result<Value, Error> {
		match DateTime::from_timestamp(val, 0) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::unix"),
				message: String::from("The first argument must be an in-bounds number of seconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}
}
