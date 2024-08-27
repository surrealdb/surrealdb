use crate::err::Error;
use crate::sql::duration::Duration;
use crate::sql::value::Value;

pub fn days((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.days().into())
}

pub fn hours((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.hours().into())
}

pub fn micros((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.micros().into())
}

pub fn millis((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.millis().into())
}

pub fn mins((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.mins().into())
}

pub fn nanos((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.nanos().into())
}

pub fn secs((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.secs().into())
}

pub fn weeks((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.weeks().into())
}

pub fn years((val,): (Duration,)) -> Result<Value, Error> {
	Ok(val.years().into())
}

pub mod from {

	use crate::err::Error;
	use crate::sql::duration::Duration;
	use crate::sql::value::Value;

	pub fn days((val,): (u64,)) -> Result<Value, Error> {
		Ok(Duration::from_days(val).into())
	}

	pub fn hours((val,): (u64,)) -> Result<Value, Error> {
		Ok(Duration::from_hours(val).into())
	}

	pub fn micros((val,): (u64,)) -> Result<Value, Error> {
		Ok(Duration::from_micros(val).into())
	}

	pub fn millis((val,): (u64,)) -> Result<Value, Error> {
		Ok(Duration::from_millis(val).into())
	}

	pub fn mins((val,): (u64,)) -> Result<Value, Error> {
		Ok(Duration::from_mins(val).into())
	}

	pub fn nanos((val,): (u64,)) -> Result<Value, Error> {
		Ok(Duration::from_nanos(val).into())
	}

	pub fn secs((val,): (u64,)) -> Result<Value, Error> {
		Ok(Duration::from_secs(val).into())
	}

	pub fn weeks((val,): (u64,)) -> Result<Value, Error> {
		Ok(Duration::from_weeks(val).into())
	}
}
