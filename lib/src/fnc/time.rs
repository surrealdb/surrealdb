use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::value::Value;
use chrono::prelude::*;
use chrono::Datelike;
use chrono::DurationRound;
use chrono::Timelike;
use chrono::Utc;

pub fn day((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.day().into())
}

pub fn floor((datetime, duration): (Value, Value)) -> Result<Value, Error> {
	Ok(match (datetime, duration) {
		(Value::Datetime(v), Value::Duration(w)) => match chrono::Duration::from_std(*w) {
			Ok(d) => match v.duration_trunc(d) {
				Ok(v) => v.into(),
				_ => Value::None,
			},
			_ => Value::None,
		},
		_ => Value::None,
	})
}

pub fn format((datetime, format): (Value, String)) -> Result<Value, Error> {
	Ok(match datetime {
		Value::Datetime(v) => v.format(&format).to_string().into(),
		_ => Value::None,
	})
}

pub fn group((datetime, strand): (Value, Value)) -> Result<Value, Error> {
	match datetime {
		Value::Datetime(v) => match strand {
			Value::Strand(g) => match g.as_str() {
				"year" => Ok(Utc.ymd(v.year(), 1, 1).and_hms(0, 0, 0).into()),
				"month" => Ok(Utc.ymd(v.year(), v.month(), 1).and_hms(0, 0, 0).into()),
				"day" => Ok(Utc
					.ymd(v.year(), v.month(), v.day())
					.and_hms(0, 0, 0)
					.into()),
				"hour" => Ok(Utc
					.ymd(v.year(), v.month(), v.day())
					.and_hms(v.hour(), 0, 0)
					.into()),
				"minute" => Ok(Utc
					.ymd(v.year(), v.month(), v.day())
					.and_hms(v.hour(), v.minute(), 0)
					.into()),
				"second" => Ok(Utc
					.ymd(v.year(), v.month(), v.day())
					.and_hms(v.hour(), v.minute(), v.second())
					.into()),
				_ => Err(Error::InvalidArguments {
					name: String::from("time::group"),
					message: String::from("The second argument must be a string, and can be one of 'year', 'month', 'day', 'hour', 'minute', or 'second'."),
				}),
			},
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn hour((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.hour().into())
}

pub fn minute((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.minute().into())
}

pub fn month((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.month().into())
}

pub fn nano((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.timestamp_nanos().into())
}

pub fn now(_: ()) -> Result<Value, Error> {
	Ok(Datetime::default().into())
}

pub fn round((datetime, duration): (Value, Value)) -> Result<Value, Error> {
	Ok(match (datetime, duration) {
		(Value::Datetime(v), Value::Duration(w)) => match chrono::Duration::from_std(*w) {
			Ok(d) => match v.duration_round(d) {
				Ok(v) => v.into(),
				_ => Value::None,
			},
			_ => Value::None,
		},
		_ => Value::None,
	})
}

pub fn second((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.second().into())
}

pub fn unix((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.timestamp().into())
}

pub fn wday((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.weekday().number_from_monday().into())
}

pub fn week((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.iso_week().week().into())
}

pub fn yday((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.ordinal().into())
}

pub fn year((datetime,): (Option<Value>,)) -> Result<Value, Error> {
	let date = match datetime {
		Some(Value::Datetime(v)) => v,
		None => Datetime::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(date.year().into())
}
