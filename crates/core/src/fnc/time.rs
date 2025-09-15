use anyhow::Result;
use chrono::offset::TimeZone;
use chrono::{DateTime, Datelike, DurationRound, Local, Timelike, Utc};

use super::args::Optional;
use crate::err::Error;
use crate::val::{Datetime, Duration, Value};

pub fn ceil((val, duration): (Datetime, Duration)) -> Result<Value> {
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
			let result = val.duration_trunc(d).ok().and_then(floor_to_ceil);

			match result {
				Some(v) => Ok(v.into()),
				_ => Err(anyhow::Error::new(Error::InvalidArguments {
					name: String::from("time::ceil"),
					message: String::from(
						"The second argument must be a duration, and must be able to be represented as nanoseconds.",
					),
				})),
			}
		}
		_ => Err(anyhow::Error::new(Error::InvalidArguments {
			name: String::from("time::ceil"),
			message: String::from(
				"The second argument must be a duration, and must be able to be represented as nanoseconds.",
			),
		})),
	}
}

pub fn day((Optional(val),): (Optional<Datetime>,)) -> Result<Value> {
	Ok(match val {
		Some(v) => v.day().into(),
		None => Datetime::now().day().into(),
	})
}

pub fn floor((val, duration): (Datetime, Duration)) -> Result<Value> {
	match chrono::Duration::from_std(*duration) {
		Ok(d) => {
			// Check for zero duration
			if d.is_zero() {
				return Ok(Value::Datetime(val));
			}
			match val.duration_trunc(d) {
				Ok(v) => Ok(v.into()),
				_ => Err(anyhow::Error::new(Error::InvalidArguments {
					name: String::from("time::floor"),
					message: String::from(
						"The second argument must be a duration, and must be able to be represented as nanoseconds.",
					),
				})),
			}
		}
		_ => Err(anyhow::Error::new(Error::InvalidArguments {
			name: String::from("time::floor"),
			message: String::from(
				"The second argument must be a duration, and must be able to be represented as nanoseconds.",
			),
		})),
	}
}

pub fn format((val, format): (Datetime, String)) -> Result<Value> {
	Ok(val.format(&format).to_string().into())
}

pub fn group((val, group): (Datetime, String)) -> Result<Value> {
	match group.as_str() {
		"year" => Ok(Utc.with_ymd_and_hms(val.year(), 1, 1, 0, 0, 0).earliest().unwrap().into()),
		"month" => {
			Ok(Utc.with_ymd_and_hms(val.year(), val.month(), 1, 0, 0, 0).earliest().unwrap().into())
		}
		"day" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), 0, 0, 0)
			.earliest()
			.unwrap()
			.into()),
		"hour" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), val.hour(), 0, 0)
			.earliest()
			.unwrap()
			.into()),
		"minute" => Ok(Utc
			.with_ymd_and_hms(val.year(), val.month(), val.day(), val.hour(), val.minute(), 0)
			.earliest()
			.unwrap()
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
			.unwrap()
			.into()),
		_ => Err(anyhow::Error::new(Error::InvalidArguments {
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
	Ok(match val {
		Some(v) => v.timestamp_nanos_opt().unwrap_or_default().into(),
		None => Datetime::now().timestamp_nanos_opt().unwrap_or_default().into(),
	})
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
	match chrono::Duration::from_std(*duration) {
		Ok(d) => {
			// Check for zero duration
			if d.is_zero() {
				return Ok(Value::Datetime(val));
			}
			match val.duration_round(d) {
				Ok(v) => Ok(v.into()),
				_ => Err(anyhow::Error::new(Error::InvalidArguments {
					name: String::from("time::round"),
					message: String::from(
						"The second argument must be a duration, and must be able to be represented as nanoseconds.",
					),
				})),
			}
		}
		_ => Err(anyhow::Error::new(Error::InvalidArguments {
			name: String::from("time::round"),
			message: String::from(
				"The second argument must be a duration, and must be able to be represented as nanoseconds.",
			),
		})),
	}
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

pub mod is {
	use anyhow::Result;

	use crate::fnc::args::Optional;
	use crate::val::{Datetime, Value};

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
	use ulid::Ulid;

	use crate::err::Error;
	use crate::val::{Datetime, Uuid, Value};

	pub fn nanos((val,): (i64,)) -> Result<Value> {
		const NANOS_PER_SEC: i64 = 1_000_000_000;

		let seconds = val.div_euclid(NANOS_PER_SEC);
		let nanoseconds = val.rem_euclid(NANOS_PER_SEC) as u32;

		match DateTime::from_timestamp(seconds, nanoseconds) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(Error::InvalidArguments {
				name: String::from("time::from::nanos"),
				message: String::from(
					"The argument must be a number of nanoseconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn micros((val,): (i64,)) -> Result<Value> {
		match DateTime::from_timestamp_micros(val) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(Error::InvalidArguments {
				name: String::from("time::from::micros"),
				message: String::from(
					"The argument must be a number of microseconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn millis((val,): (i64,)) -> Result<Value> {
		match DateTime::from_timestamp_millis(val) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(Error::InvalidArguments {
				name: String::from("time::from::millis"),
				message: String::from(
					"The argument must be a number of milliseconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn secs((val,): (i64,)) -> Result<Value> {
		match DateTime::from_timestamp(val, 0) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(Error::InvalidArguments {
				name: String::from("time::from::secs"),
				message: String::from(
					"The argument must be a number of seconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn unix((val,): (i64,)) -> Result<Value> {
		match DateTime::from_timestamp(val, 0) {
			Some(v) => Ok(Datetime::from(v).into()),
			None => Err(anyhow::Error::new(Error::InvalidArguments {
				name: String::from("time::from::unix"),
				message: String::from(
					"The argument must be a number of seconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
				),
			})),
		}
	}

	pub fn ulid((val,): (String,)) -> Result<Value> {
		match Ulid::from_string(&val) {
			Ok(v) => Ok(Datetime::from(DateTime::from(v.datetime())).into()),
			_ => Err(anyhow::Error::new(Error::InvalidArguments {
				name: String::from("time::from::ulid"),
				message: String::from(
					"The first argument must be a string, containing a valid ULID.",
				),
			})),
		}
	}

	pub fn uuid((val,): (Uuid,)) -> Result<Value> {
		match val.0.get_timestamp() {
			Some(v) => {
				let (s, ns) = v.to_unix();
				match Datetime::try_from((s as i64, ns)) {
					Ok(v) => Ok(v.into()),
					_ => fail!("Failed to convert UUID Timestamp to Datetime."),
				}
			}
			None => Err(anyhow::Error::new(Error::InvalidArguments {
				name: String::from("time::from::uuid"),
				message: String::from("The first argument must be a v1, v6 or v7 UUID."),
			})),
		}
	}
}
