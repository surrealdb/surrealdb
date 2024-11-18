use crate::cnf::ID_CHARS;
use crate::err::Error;
use crate::sql::uuid::Uuid;
use crate::sql::value::Value;
use crate::sql::Datetime;
use chrono::{TimeZone, Utc};
use nanoid::nanoid;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::IteratorRandom;
use rand::Rng;
use ulid::Ulid;

pub fn rand(_: ()) -> Result<Value, Error> {
	Ok(rand::random::<f64>().into())
}

pub fn bool(_: ()) -> Result<Value, Error> {
	Ok(rand::random::<bool>().into())
}

pub fn r#enum(mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(match args.len() {
		0 => Value::None,
		1 => match args.remove(0) {
			Value::Array(v) => v.into_iter().choose(&mut rand::thread_rng()).unwrap_or(Value::None),
			v => v,
		},
		_ => args.into_iter().choose(&mut rand::thread_rng()).unwrap(),
	})
}

pub fn float((range,): (Option<(f64, f64)>,)) -> Result<Value, Error> {
	Ok(if let Some((min, max)) = range {
		if max < min {
			rand::thread_rng().gen_range(max..=min)
		} else {
			rand::thread_rng().gen_range(min..=max)
		}
	} else {
		rand::random::<f64>()
	}
	.into())
}

pub fn guid((arg1, arg2): (Option<i64>, Option<i64>)) -> Result<Value, Error> {
	// Set a reasonable maximum length
	const LIMIT: i64 = 64;
	// Check the function input arguments
	let val = if let Some((min, max)) = arg1.zip(arg2) {
		match min {
			min if (1..=LIMIT).contains(&min) => match max {
				max if min <= max && max <= LIMIT => rand::thread_rng().gen_range(min as usize..=max as usize),
				max if max >= 1 && max <= min => rand::thread_rng().gen_range(max as usize..=min as usize),
				_ => return Err(Error::InvalidArguments {
					name: String::from("rand::guid"),
					message: format!("To generate a guid of between X and Y characters in length, the 2 arguments must be positive numbers and no higher than {LIMIT}."),
				}),
			},
			_ => return Err(Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: format!("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers and no higher than {LIMIT}."),
			}),
		}
	} else if let Some(len) = arg1 {
		if (1..=LIMIT).contains(&len) {
			len as usize
		} else {
			return Err(Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: format!("To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."),
			});
		}
	} else {
		20
	};
	// Generate the random guid
	Ok(nanoid!(val, &ID_CHARS).into())
}

pub fn int((range,): (Option<(i64, i64)>,)) -> Result<Value, Error> {
	Ok(if let Some((min, max)) = range {
		if max < min {
			rand::thread_rng().gen_range(max..=min)
		} else {
			rand::thread_rng().gen_range(min..=max)
		}
	} else {
		rand::random::<i64>()
	}
	.into())
}

pub fn string((arg1, arg2): (Option<i64>, Option<i64>)) -> Result<Value, Error> {
	// Set a reasonable maximum length
	const LIMIT: i64 = 65536;
	// Check the function input arguments
	let val = if let Some((min, max)) = arg1.zip(arg2) {
		match min {
			min if (1..=LIMIT).contains(&min) => match max {
				max if min <= max && max <= LIMIT => rand::thread_rng().gen_range(min as usize..=max as usize),
				max if max >= 1 && max <= min => rand::thread_rng().gen_range(max as usize..=min as usize),
				_ => return Err(Error::InvalidArguments {
					name: String::from("rand::string"),
					message: format!("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers and no higher than {LIMIT}."),
				}),
			},
			_ => return Err(Error::InvalidArguments {
				name: String::from("rand::string"),
				message: format!("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers and no higher than {LIMIT}."),
			}),
		}
	} else if let Some(len) = arg1 {
		if (1..=LIMIT).contains(&len) {
			len as usize
		} else {
			return Err(Error::InvalidArguments {
				name: String::from("rand::string"),
				message: format!("To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."),
			});
		}
	} else {
		32
	};
	// Generate the random string
	Ok(Alphanumeric.sample_string(&mut rand::thread_rng(), val).into())
}

pub fn time((range,): (Option<(i64, i64)>,)) -> Result<Value, Error> {
	// Set the maximum valid seconds
	const LIMIT: i64 = 8210298412799;
	// Check the function input arguments
	let val = if let Some((min, max)) = range {
		match min {
			min if (1..=LIMIT).contains(&min) => match max {
				max if min <= max && max <= LIMIT => rand::thread_rng().gen_range(min..=max),
				max if max >= 1 && max <= min => rand::thread_rng().gen_range(max..=min),
				_ => return Err(Error::InvalidArguments {
					name: String::from("rand::time"),
					message: format!("To generate a time between X and Y seconds, the 2 arguments must be positive numbers and no higher than {LIMIT}."),
				}),
			},
			_ => return Err(Error::InvalidArguments {
				name: String::from("rand::time"),
				message: format!("To generate a time between X and Y seconds, the 2 arguments must be positive numbers and no higher than {LIMIT}."),
			}),
		}
	} else {
		rand::thread_rng().gen_range(0..=LIMIT)
	};
	// Generate the random time
	match Utc.timestamp_opt(val, 0).earliest() {
		Some(v) => Ok(v.into()),
		_ => Err(Error::Unreachable("Expected to find a datetime here".into()))
	}
}

pub fn ulid((timestamp,): (Option<Datetime>,)) -> Result<Value, Error> {
	let ulid = match timestamp {
		Some(timestamp) => {
			#[cfg(target_arch = "wasm32")]
			if timestamp.0 < chrono::DateTime::UNIX_EPOCH {
				return Err(Error::InvalidArguments {
					name: String::from("rand::ulid"),
					message: format!(
						"To generate a ULID from a datetime, it must be a time beyond UNIX epoch."
					),
				});
			}

			Ulid::from_datetime(timestamp.0.into())
		}
		None => Ulid::new(),
	};

	Ok(ulid.to_string().into())
}

pub fn uuid((timestamp,): (Option<Datetime>,)) -> Result<Value, Error> {
	let uuid = match timestamp {
		Some(timestamp) => {
			#[cfg(target_arch = "wasm32")]
			if timestamp.0 < chrono::DateTime::UNIX_EPOCH {
				return Err(Error::InvalidArguments {
					name: String::from("rand::ulid"),
					message: format!(
						"To generate a ULID from a datetime, it must be a time beyond UNIX epoch."
					),
				});
			}

			Uuid::new_v7_from_datetime(timestamp)
		}
		None => Uuid::new(),
	};
	Ok(uuid.into())
}

pub mod uuid {

	use crate::err::Error;
	use crate::sql::uuid::Uuid;
	use crate::sql::value::Value;
	use crate::sql::Datetime;

	pub fn v4(_: ()) -> Result<Value, Error> {
		Ok(Uuid::new_v4().into())
	}

	pub fn v7((timestamp,): (Option<Datetime>,)) -> Result<Value, Error> {
		let uuid = match timestamp {
			Some(timestamp) => {
				#[cfg(target_arch = "wasm32")]
				if timestamp.0 < chrono::DateTime::UNIX_EPOCH {
					return Err(Error::InvalidArguments {
						name: String::from("rand::ulid"),
						message: format!(
							"To generate a ULID from a datetime, it must be a time beyond UNIX epoch."
						),
					});
				}

				Uuid::new_v7_from_datetime(timestamp)
			}
			None => Uuid::new(),
		};
		Ok(uuid.into())
	}
}
