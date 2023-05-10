use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::duration::Duration;
use crate::sql::value::Value;
use chrono::offset::TimeZone;
use chrono::Datelike;
use chrono::DurationRound;
use chrono::Local;
use chrono::Timelike;
use chrono::Utc;

pub fn day((val,): (Option<Datetime>,)) -> Result<Value, Error> {
	Ok(match val {
		Some(v) => v.day().into(),
		None => Datetime::default().day().into(),
	})
}

pub fn floor((val, duration): (Datetime, Duration)) -> Result<Value, Error> {
	match chrono::Duration::from_std(*duration) {
		Ok(d) => match val.duration_trunc(d) {
			Ok(v) => Ok(v.into()),
			_ => Err(Error::InvalidArguments {
				name: String::from("time::floor"),
				message: String::from("The second argument must be a duration, and must be able to be represented as nanoseconds."),
			}),
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
		Some(v) => v.timestamp_nanos().into(),
		None => Datetime::default().timestamp_nanos().into(),
	})
}

pub fn now(_: ()) -> Result<Value, Error> {
	Ok(Datetime::default().into())
}

pub fn round((val, duration): (Datetime, Duration)) -> Result<Value, Error> {
	match chrono::Duration::from_std(*duration) {
		Ok(d) => match val.duration_round(d) {
			Ok(v) => Ok(v.into()),
			_ => Err(Error::InvalidArguments {
				name: String::from("time::round"),
				message: String::from("The second argument must be a duration, and must be able to be represented as nanoseconds."),
			}),
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

pub mod from {

	use crate::err::Error;
	use crate::sql::datetime::Datetime;
	use crate::sql::value::Value;
	use chrono::{NaiveDateTime, Offset, TimeZone, Utc};

	pub fn micros((val,): (i64,)) -> Result<Value, Error> {
		match NaiveDateTime::from_timestamp_micros(val) {
			Some(v) => match Utc.fix().from_local_datetime(&v).earliest() {
				Some(v) => Ok(Datetime::from(v.with_timezone(&Utc)).into()),
				None => Err(Error::InvalidArguments {
					name: String::from("time::from::micros"),
					message: String::from("The first argument must be an in-bounds number of microseconds relative to January 1, 1970 0:00:00 UTC."),
				}),
			}
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::micros"),
				message: String::from("The first argument must be an in-bounds number of microseconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}

	pub fn millis((val,): (i64,)) -> Result<Value, Error> {
		match NaiveDateTime::from_timestamp_millis(val) {
			Some(v) => match Utc.fix().from_local_datetime(&v).earliest() {
				Some(v) => Ok(Datetime::from(v.with_timezone(&Utc)).into()),
				None => Err(Error::InvalidArguments {
					name: String::from("time::from::millis"),
					message: String::from("The first argument must be an in-bounds number of milliseconds relative to January 1, 1970 0:00:00 UTC."),
				}),
			}
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::millis"),
				message: String::from("The first argument must be an in-bounds number of milliseconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}

	pub fn secs((val,): (i64,)) -> Result<Value, Error> {
		match NaiveDateTime::from_timestamp_opt(val, 0) {
			Some(v) => match Utc.fix().from_local_datetime(&v).earliest() {
				Some(v) => Ok(Datetime::from(v.with_timezone(&Utc)).into()),
				None => Err(Error::InvalidArguments {
					name: String::from("time::from::secs"),
					message: String::from("The first argument must be an in-bounds number of seconds relative to January 1, 1970 0:00:00 UTC."),
				}),
			}
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::secs"),
				message: String::from("The first argument must be an in-bounds number of seconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}

	pub fn unix((val,): (i64,)) -> Result<Value, Error> {
		match NaiveDateTime::from_timestamp_opt(val, 0) {
			Some(v) => match Utc.fix().from_local_datetime(&v).earliest() {
				Some(v) => Ok(Datetime::from(v.with_timezone(&Utc)).into()),
				None => Err(Error::InvalidArguments {
					name: String::from("time::from::unix"),
					message: String::from("The first argument must be an in-bounds number of seconds relative to January 1, 1970 0:00:00 UTC."),
				}),
			}
			None => Err(Error::InvalidArguments {
				name: String::from("time::from::unix"),
				message: String::from("The first argument must be an in-bounds number of seconds relative to January 1, 1970 0:00:00 UTC."),
			}),
		}
	}
}
