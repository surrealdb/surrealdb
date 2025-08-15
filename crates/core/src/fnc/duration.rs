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

	pub fn days((val,): (i64,)) -> Result<Value> {
		// TODO: Deal with truncation:
		let val = val as u64;

		Duration::from_days(val)
			.map(|x| x.into())
			.ok_or_else(|| Error::ArithmeticOverflow(format!("duration::from::days({val})")))
			.map_err(anyhow::Error::new)
	}

	pub fn hours((val,): (i64,)) -> Result<Value> {
		// TODO: Deal with truncation:
		let val = val as u64;

		Duration::from_hours(val)
			.map(|x| x.into())
			.ok_or_else(|| Error::ArithmeticOverflow(format!("duration::from::hours({val})")))
			.map_err(anyhow::Error::new)
	}

	pub fn micros((val,): (i64,)) -> Result<Value> {
		// TODO: Deal with truncation:
		let val = val as u64;

		Ok(Duration::from_micros(val).into())
	}

	pub fn millis((val,): (i64,)) -> Result<Value> {
		// TODO: Deal with truncation:
		let val = val as u64;

		Ok(Duration::from_millis(val).into())
	}

	pub fn mins((val,): (i64,)) -> Result<Value> {
		// TODO: Deal with truncation:
		let val = val as u64;

		Duration::from_mins(val)
			.map(|x| x.into())
			.ok_or_else(|| Error::ArithmeticOverflow(format!("duration::from::mins({val})")))
			.map_err(anyhow::Error::new)
	}

	pub fn nanos((val,): (i64,)) -> Result<Value> {
		// TODO: Deal with truncation:
		let val = val as u64;

		Ok(Duration::from_nanos(val).into())
	}

	pub fn secs((val,): (i64,)) -> Result<Value> {
		// TODO: Deal with truncation:
		let val = val as u64;

		Ok(Duration::from_secs(val).into())
	}

	pub fn weeks((val,): (i64,)) -> Result<Value> {
		// TODO: Deal with truncation:
		let val = val as u64;

		Duration::from_weeks(val)
			.map(|x| x.into())
			.ok_or_else(|| Error::ArithmeticOverflow(format!("duration::from::weeks({val})")))
			.map_err(anyhow::Error::new)
	}
}
