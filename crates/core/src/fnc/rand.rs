use crate::cnf::ID_CHARS;
use crate::err::Error;
use crate::sql::uuid::Uuid;
use crate::sql::value::Value;
use crate::sql::{Datetime, Duration, Number};
use chrono::{TimeZone, Utc};
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::IteratorRandom;
use rand::seq::SliceRandom;
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
	let mut rng = rand::thread_rng();
	let id: String = (0..len).map(|_| *ID_CHARS.choose(&mut rng).unwrap_or(&'0')).collect();
	Ok(Value::from(id))
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

pub fn duration((dur1, dur2): (Duration, Duration)) -> Result<Value, Error> {
	// Sort from low to high
	let (from, to) = match dur2 > dur1 {
		true => (dur1, dur2),
		false => (dur2, dur1),
	};

	let rand = rand::thread_rng().gen_range(from.as_nanos()..=to.as_nanos());

	let nanos = (rand % 1_000_000_000) as u32;

	// Max Duration is made of (u64::MAX, NANOS_PER_SEC - 1) so will never overflow
	let Ok(secs) = u64::try_from(rand / 1_000_000_000) else {
		return Err(Error::Unreachable("Overflow inside rand::duration()".into()));
	};

	Ok(Value::Duration(Duration::new(secs, nanos)))
}

pub fn time((range,): (Option<(Value, Value)>,)) -> Result<Value, Error> {
	// Process the arguments
	let range = match range {
		None => None,
		Some((Value::Number(Number::Int(min)), Value::Number(Number::Int(max)))) => {
			Some((min, max))
		}
		Some((Value::Datetime(min), Value::Datetime(max))) => Some((min.to_secs(), max.to_secs())),
		Some((Value::Number(Number::Int(min)), Value::Datetime(max))) => Some((min, max.to_secs())),
		Some((Value::Datetime(min), Value::Number(Number::Int(max)))) => Some((min.to_secs(), max)),
		_ => {
			return Err(Error::InvalidArguments {
				name: String::from("rand::time"),
				message: String::from("Expected two arguments of type datetime or int"),
			})
		}
	};

	// Set the minimum valid seconds
	const MINIMUM: i64 = -8334601228800;
	// Set the maximum valid seconds
	const LIMIT: i64 = 8210266876799;

	// Check the function input arguments
	let (min, max) = if let Some((min, max)) = range {
		match min {
			min if (MINIMUM..=LIMIT).contains(&min) => match max {
				max if min <= max && max <= LIMIT => (min, max),
				max if max >= MINIMUM && max <= min => (max, min),
				_ => return Err(Error::InvalidArguments {
					name: String::from("rand::time"),
					message: format!("To generate a random time, the 2 arguments must be numbers between {MINIMUM} and {LIMIT} seconds from the UNIX epoch or a 'datetime' within the range d'-262143-01-01T00:00:00Z' and +262142-12-31T23:59:59Z'."),
				}),
			},
			_ => return Err(Error::InvalidArguments {
				name: String::from("rand::time"),
				message: format!("To generate a random time, the 2 arguments must be numbers between {MINIMUM} and {LIMIT} seconds from the UNIX epoch or a 'datetime' within the range d'-262143-01-01T00:00:00Z' and +262142-12-31T23:59:59Z'."),
			}),
		}
	} else {
		// Datetime between d'0000-01-01T00:00:00Z' and d'9999-12-31T23:59:59Z'
		(-62167219200, 253402300799)
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

#[cfg(test)]
mod tests {
	use std::thread;

	use super::*;

	#[test]
	fn test_rand_guid_concurrency() {
		let mut handles = vec![];
		for _ in 0..100 {
			handles.push(thread::spawn(|| {
				for _ in 0..1000 {
					let _ = guid((Some(0), Some(10))).unwrap();
				}
			}));
		}
		for handle in handles {
			handle.join().unwrap();
		}
	}

	#[test]
	fn test_rand_guid_len_0() {
		let res = guid((Some(0), Some(0))).unwrap();
		assert_eq!(res, Value::from(""));
	}
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
