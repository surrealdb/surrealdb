use crate::err::Error;
use crate::sql::Value;
use semver::Version;

pub fn compare((left, right): (String, String)) -> Result<Value, Error> {
	let left = match Version::parse(&left) {
		Ok(left) => left,
		Err(_) => return Ok(Value::None),
	};

	let right = match Version::parse(&right) {
		Ok(right) => right,
		Err(_) => return Ok(Value::None),
	};

	Ok((left.cmp(&right) as i32).into())
}

pub fn major((version,): (String,)) -> Result<Value, Error> {
	Ok(Version::parse(&version).map_or_else(|_| Value::None, |version| version.major.into()))
}

pub fn minor((version,): (String,)) -> Result<Value, Error> {
	Ok(Version::parse(&version).map_or_else(|_| Value::None, |version| version.minor.into()))
}

pub fn patch((version,): (String,)) -> Result<Value, Error> {
	Ok(Version::parse(&version).map_or_else(|_| Value::None, |version| version.patch.into()))
}

pub mod increment {
	use crate::err::Error;
	use crate::sql::Value;
	use semver::Version;

	pub fn major((version,): (String,)) -> Result<Value, Error> {
		Ok(Version::parse(&version).map_or_else(
			|_| Value::None,
			|mut version| {
				version.major += 1;
				version.minor = 0;
				version.patch = 0;
				version.to_string().into()
			},
		))
	}

	pub fn minor((version,): (String,)) -> Result<Value, Error> {
		Ok(Version::parse(&version).map_or_else(
			|_| Value::None,
			|mut version| {
				version.minor += 1;
				version.patch = 0;
				version.to_string().into()
			},
		))
	}

	pub fn patch((version,): (String,)) -> Result<Value, Error> {
		Ok(Version::parse(&version).map_or_else(
			|_| Value::None,
			|mut version| {
				version.patch += 1;
				version.to_string().into()
			},
		))
	}
}

pub mod set {
	use crate::err::Error;
	use crate::sql::Value;
	use semver::Version;

	pub fn major((version, value): (String, u64)) -> Result<Value, Error> {
		Ok(Version::parse(&version).map_or_else(
			|_| Value::None,
			|mut version| {
				version.major = value.into();
				version.to_string().into()
			},
		))
	}

	pub fn minor((version, value): (String, u64)) -> Result<Value, Error> {
		Ok(Version::parse(&version).map_or_else(
			|_| Value::None,
			|mut version| {
				version.minor = value;
				version.to_string().into()
			},
		))
	}

	pub fn patch((version, value): (String, u64)) -> Result<Value, Error> {
		Ok(Version::parse(&version).map_or_else(
			|_| Value::None,
			|mut version| {
				version.patch = value;
				version.to_string().into()
			},
		))
	}
}
