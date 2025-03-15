use crate::cnf::ID_CHARS;
use crate::err::Error;
use crate::sql::uuid::Uuid;
use crate::sql::value::Value;
use crate::sql::{Datetime, Number};
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

// TODO (Delskayn): Don't agree with the inclusive ranges in the functions here,
// seems inconsistent with general use of ranges not including the upperbound.
// These should probably all be exclusive.
//
// TODO (Delskayn): Switching of min and max if min > max is also inconsistent with rest of
// functions and the range type. The functions should either return NONE or an error if the lowerbound
// of the ranges here are larger then the upperbound.
pub fn float((range,): (Option<(f64, f64)>,)) -> Result<Value, Error> {
	let res = if let Some((min, max)) = range {
		if max < min {
			rand::thread_rng().gen_range(max..=min)
		} else {
			rand::thread_rng().gen_range(min..=max)
		}
	} else {
		rand::random::<f64>()
	};
	Ok(res.into())
}

pub fn guid((arg1, arg2): (Option<i64>, Option<i64>)) -> Result<Value, Error> {
	// Set a reasonable maximum length
	const LIMIT: i64 = 64;

	// rand::guid(NULL,10) is not allowed by the calling infrastructure.
	let lower = arg1.unwrap_or(20);
	let len = if let Some(upper) = arg2 {
		if lower > upper {
			return Err(Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: "Lowerbound of number of characters must be less then the upperbound."
					.to_string(),
			});
		}
		if upper > LIMIT {
			return Err(Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: format!("To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."),
			});
		}

		rand::thread_rng().gen_range((lower as usize)..=(upper as usize))
	} else {
		if lower > LIMIT {
			return Err(Error::InvalidArguments {
			name: String::from("rand::guid"),
			message: format!("To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."),
		});
		}
		lower as usize
	};

	// Generate the random guid
	Ok(nanoid!(len, &ID_CHARS).into())
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
	// rand::guid(NULL,10) is not allowed by the calling infrastructure.
	let lower = arg1.unwrap_or(32);
	let len = if let Some(upper) = arg2 {
		if lower > upper {
			return Err(Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: "Lowerbound of number of characters must be less then the upperbound."
					.to_string(),
			});
		}
		if upper > LIMIT {
			return Err(Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: format!("To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."),
			});
		}

		rand::thread_rng().gen_range((lower as usize)..=(upper as usize))
	} else {
		if lower > LIMIT {
			return Err(Error::InvalidArguments {
			name: String::from("rand::guid"),
			message: format!("To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."),
		});
		}
		lower as usize
	};
	// Generate the random string
	Ok(Alphanumeric.sample_string(&mut rand::thread_rng(), len).into())
}

pub fn time((range,): (Option<(Value, Value)>,)) -> Result<Value, Error> {
	// Process the arguments
	let range = match range {
		None => None,
		Some((Value::Number(Number::Int(min)), Value::Number(Number::Int(max)))) => {
			Some((min, max))
		}
		Some((Value::Datetime(min), Value::Datetime(max))) => match (min.to_i64(), max.to_i64()) {
			(Some(min), Some(max)) => Some((min, max)),
			_ => {
				return Err(Error::InvalidArguments {
					name: String::from("rand::time"),
					message: String::from("Failed to convert datetime arguments to i64 timestamps"),
				})
			}
		},
		_ => {
			return Err(Error::InvalidArguments {
				name: String::from("rand::time"),
				message: String::from(
					"Expected an optional pair of datetimes or pair of i64 numbers to be passed",
				),
			})
		}
	};
	// Set the maximum valid seconds
	const LIMIT: i64 = 8210298412799;
	// Check the function input arguments
	let (min, max) = if let Some((min, max)) = range {
		match min {
			min if (1..=LIMIT).contains(&min) => match max {
				max if min <= max && max <= LIMIT => (min, max),
				max if max >= 1 && max <= min => (max, min),
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
		(0, LIMIT)
	};
	// Generate the random time, try up to 5 times
	for _ in 0..5 {
		let val = rand::thread_rng().gen_range(min..=max);
		if let Some(v) = Utc.timestamp_opt(val, 0).earliest() {
			return Ok(v.into());
		}
	}
	// We were unable to generate a valid random datetime
	Err(fail!("Expected a valid datetime, but were unable to generate one"))
}

pub fn ulid((timestamp,): (Option<Datetime>,)) -> Result<Value, Error> {
	let ulid = match timestamp {
		Some(timestamp) => {
			#[cfg(target_family = "wasm")]
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
			#[cfg(target_family = "wasm")]
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
				#[cfg(target_family = "wasm")]
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
