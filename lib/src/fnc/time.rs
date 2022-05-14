use crate::ctx::Context;
use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::value::Value;
use chrono::prelude::*;
use chrono::Datelike;
use chrono::DurationRound;
use chrono::Timelike;
use chrono::Utc;

pub fn day(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().day().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.day().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn floor(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Datetime(v) => match args.remove(0) {
			Value::Duration(w) => match chrono::Duration::from_std(*w) {
				Ok(d) => match v.duration_trunc(d) {
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

pub fn group(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Datetime(v) => match args.remove(0) {
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

pub fn hour(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().hour().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.hour().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn mins(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().minute().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.minute().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn month(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().day().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.day().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn nano(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().timestamp_nanos().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.timestamp_nanos().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn now(_: &Context, _: Vec<Value>) -> Result<Value, Error> {
	Ok(Datetime::default().into())
}

pub fn round(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Datetime(v) => match args.remove(0) {
			Value::Duration(w) => match chrono::Duration::from_std(*w) {
				Ok(d) => match v.duration_round(d) {
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

pub fn secs(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().second().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.second().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn unix(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().timestamp().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.timestamp().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn wday(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().weekday().number_from_monday().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.weekday().number_from_monday().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn week(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().iso_week().week().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.iso_week().week().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn yday(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().ordinal().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.ordinal().into()),
			_ => Ok(Value::None),
		},
	}
}

pub fn year(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Utc::now().year().into()),
		_ => match args.remove(0) {
			Value::Datetime(v) => Ok(v.year().into()),
			_ => Ok(Value::None),
		},
	}
}
