use anyhow::Result;

use crate::val::{Duration, Value};

pub fn days((val,): (Duration,)) -> Result<Value> {
	Ok(val.days().into())
}

pub fn hours((val,): (Duration,)) -> Result<Value> {
	Ok(val.hours().into())
}

pub fn micros((val,): (Duration,)) -> Result<Value> {
	Ok(val.micros().into())
}

pub fn millis((val,): (Duration,)) -> Result<Value> {
	Ok(val.millis().into())
}

pub fn mins((val,): (Duration,)) -> Result<Value> {
	Ok(val.mins().into())
}

pub fn nanos((val,): (Duration,)) -> Result<Value> {
	Ok(val.nanos().into())
}

pub fn secs((val,): (Duration,)) -> Result<Value> {
	Ok(val.secs().into())
}

pub fn weeks((val,): (Duration,)) -> Result<Value> {
	Ok(val.weeks().into())
}

pub fn years((val,): (Duration,)) -> Result<Value> {
	Ok(val.years().into())
}

pub mod from {

	use anyhow::Result;

	use crate::err::Error;
	use crate::val::{Duration, Value};

	fn to_unsigned(name: &str, val: i64) -> Result<u64> {
		u64::try_from(val).map_err(|_| {
			anyhow::Error::new(Error::ArithmeticNegativeOverflow(format!("{name}({val})")))
		})
	}

	pub fn days((val,): (i64,)) -> Result<Value> {
		let n = to_unsigned("duration::from_days", val)?;
		Duration::from_days(n)
			.map(|x| x.into())
			.ok_or_else(|| Error::ArithmeticOverflow(format!("duration::from_days({n})")))
			.map_err(anyhow::Error::new)
	}

	pub fn hours((val,): (i64,)) -> Result<Value> {
		let n = to_unsigned("duration::from_hours", val)?;
		Duration::from_hours(n)
			.map(|x| x.into())
			.ok_or_else(|| Error::ArithmeticOverflow(format!("duration::from_hours({n})")))
			.map_err(anyhow::Error::new)
	}

	pub fn micros((val,): (i64,)) -> Result<Value> {
		let n = to_unsigned("duration::from_micros", val)?;
		Ok(Duration::from_micros(n).into())
	}

	pub fn millis((val,): (i64,)) -> Result<Value> {
		let n = to_unsigned("duration::from_millis", val)?;
		Ok(Duration::from_millis(n).into())
	}

	pub fn mins((val,): (i64,)) -> Result<Value> {
		let n = to_unsigned("duration::from_mins", val)?;
		Duration::from_mins(n)
			.map(|x| x.into())
			.ok_or_else(|| Error::ArithmeticOverflow(format!("duration::from_mins({n})")))
			.map_err(anyhow::Error::new)
	}

	pub fn nanos((val,): (i64,)) -> Result<Value> {
		let n = to_unsigned("duration::from_nanos", val)?;
		Ok(Duration::from_nanos(n).into())
	}

	pub fn secs((val,): (i64,)) -> Result<Value> {
		let n = to_unsigned("duration::from_secs", val)?;
		Ok(Duration::from_secs(n).into())
	}

	pub fn weeks((val,): (i64,)) -> Result<Value> {
		let n = to_unsigned("duration::from_weeks", val)?;
		Duration::from_weeks(n)
			.map(|x| x.into())
			.ok_or_else(|| Error::ArithmeticOverflow(format!("duration::from_weeks({n})")))
			.map_err(anyhow::Error::new)
	}
}
