use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::value::Value;
use chrono::prelude::*;
use chrono::Datelike;
use chrono::DurationRound;
use chrono::Timelike;
use chrono::Utc;

pub fn day(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().day().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.day().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn floor(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Datetime(v) => match args.remove(0) {
			Value::Duration(w) => match chrono::Duration::from_std(w.value) {
				Ok(d) => match v.value.duration_trunc(d) {
					Ok(v) => Ok(v.into()),
					_ => Ok(Value::None),
				},
				_ => Ok(Value::None),
			},
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn group(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Datetime(v) => match args.remove(0) {
			Value::Strand(g) => match g.as_str() {
				"year" => Ok(Utc.ymd(v.value.year(), 1, 1).and_hms(0, 0, 0).into()),
				"month" => Ok(Utc.ymd(v.value.year(), v.value.month(), 1).and_hms(0, 0, 0).into()),
				"day" => Ok(Utc
					.ymd(v.value.year(), v.value.month(), v.value.day())
					.and_hms(0, 0, 0)
					.into()),
				"hour" => Ok(Utc
					.ymd(v.value.year(), v.value.month(), v.value.day())
					.and_hms(v.value.hour(), 0, 0)
					.into()),
				"minute" => Ok(Utc
					.ymd(v.value.year(), v.value.month(), v.value.day())
					.and_hms(v.value.hour(), v.value.minute(), 0)
					.into()),
				"second" => Ok(Utc
					.ymd(v.value.year(), v.value.month(), v.value.day())
					.and_hms(v.value.hour(), v.value.minute(), v.value.second())
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

pub fn hour(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().hour().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.hour().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn mins(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().minute().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.minute().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn month(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().day().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.day().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn nano(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().timestamp_nanos().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.timestamp_nanos().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn now(_: &Runtime, _: Vec<Value>) -> Result<Value, Error> {
	Ok(Datetime::default().into())
}

pub fn round(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Datetime(v) => match args.remove(0) {
			Value::Duration(w) => match chrono::Duration::from_std(w.value) {
				Ok(d) => match v.value.duration_round(d) {
					Ok(v) => Ok(v.into()),
					_ => Ok(Value::None),
				},
				_ => Ok(Value::None),
			},
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn secs(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().second().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.second().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn unix(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().timestamp().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.timestamp().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn wday(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().weekday().number_from_monday().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.weekday().number_from_monday().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn week(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().iso_week().week().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.iso_week().week().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn yday(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().ordinal().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.ordinal().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn year(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().year().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.value.year().into()),
			_ => Ok(Value::None),
		},
	}
}
