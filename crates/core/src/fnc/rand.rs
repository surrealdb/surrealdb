use anyhow::{Result, bail, ensure};
use chrono::{TimeZone, Utc};
use nanoid::nanoid;
use rand::Rng;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::IteratorRandom;
use ulid::Ulid;

use super::args::{Any, Args, Arity, FromArg, Optional};
use crate::cnf::ID_CHARS;
use crate::err::Error;
use crate::val::{Datetime, Duration, Number, Uuid, Value};

pub fn rand(_: ()) -> Result<Value> {
	Ok(rand::random::<f64>().into())
}

pub fn bool(_: ()) -> Result<Value> {
	Ok(rand::random::<bool>().into())
}

pub fn r#enum(Any(mut args): Any) -> Result<Value> {
	Ok(match args.len() {
		0 => Value::None,
		1 => match args.remove(0) {
			Value::Array(v) => v.into_iter().choose(&mut rand::thread_rng()).unwrap_or(Value::None),
			v => v,
		},
		_ => args.into_iter().choose(&mut rand::thread_rng()).unwrap(),
	})
}

pub struct NoneOrRange<T>(Option<(T, T)>);

impl<T: FromArg> FromArg for NoneOrRange<T> {
	fn arity() -> Arity {
		Arity {
			lower: 0,
			upper: Some(2),
		}
	}

	fn from_arg(name: &str, args: &mut Args) -> Result<Self> {
		if !args.has_next() {
			return Ok(NoneOrRange(None));
		}

		let a = T::from_arg(name, args)?;

		ensure!(
			args.has_next(),
			Error::InvalidArguments {
				name: name.to_owned(),
				message: "Expected 0 or 2 arguments".to_string(),
			}
		);

		let b = T::from_arg(name, args)?;

		Ok(NoneOrRange(Some((a, b))))
	}
}

// TODO (Delskayn): Don't agree with the inclusive ranges in the functions here,
// seems inconsistent with general use of ranges not including the upperbound.
// These should probably all be exclusive.
//
// TODO (Delskayn): Switching of min and max if min > max is also inconsistent
// with rest of functions and the range type. The functions should either return
// NONE or an error if the lowerbound of the ranges here are larger then the
// upperbound.
pub fn float((NoneOrRange(range),): (NoneOrRange<f64>,)) -> Result<Value> {
	let v = if let Some((min, max)) = range {
		if max < min {
			rand::thread_rng().gen_range(max..=min)
		} else {
			rand::thread_rng().gen_range(min..=max)
		}
	} else {
		rand::random::<f64>()
	};
	Ok(Value::from(v))
}

pub fn guid((Optional(arg1), Optional(arg2)): (Optional<i64>, Optional<i64>)) -> Result<Value> {
	// Set a reasonable maximum length
	const LIMIT: i64 = 64;

	// rand::guid(NULL,10) is not allowed by the calling infrastructure.
	let lower = arg1.unwrap_or(20);
	let len = if let Some(upper) = arg2 {
		ensure!(
			lower <= upper,
			Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: "Lowerbound of number of characters must be less then the upperbound."
					.to_string(),
			}
		);
		ensure!(
			upper <= LIMIT,
			Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: format!(
					"To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."
				),
			}
		);

		rand::thread_rng().gen_range((lower as usize)..=(upper as usize))
	} else {
		ensure!(
			lower <= LIMIT,
			Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: format!(
					"To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."
				),
			}
		);
		lower as usize
	};

	// Generate the random guid
	Ok(nanoid!(len, &ID_CHARS).into())
}

pub fn int((NoneOrRange(range),): (NoneOrRange<i64>,)) -> Result<Value> {
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

pub fn string((Optional(arg1), Optional(arg2)): (Optional<i64>, Optional<i64>)) -> Result<Value> {
	// Set a reasonable maximum length
	const LIMIT: i64 = 65536;
	// rand::guid(NULL,10) is not allowed by the calling infrastructure.
	let lower = arg1.unwrap_or(32);
	let len = if let Some(upper) = arg2 {
		ensure!(
			lower <= upper,
			Error::InvalidArguments {
				name: String::from("rand::string"),
				message: "Lowerbound of number of characters must be less then the upperbound."
					.to_string(),
			}
		);
		ensure!(
			upper <= LIMIT,
			Error::InvalidArguments {
				name: String::from("rand::string"),
				message: format!(
					"To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."
				),
			}
		);

		rand::thread_rng().gen_range((lower as usize)..=(upper as usize))
	} else {
		ensure!(
			lower <= LIMIT,
			Error::InvalidArguments {
				name: String::from("rand::string"),
				message: format!(
					"To generate a string of X characters in length, the argument must be a positive number and no higher than {LIMIT}."
				),
			}
		);
		lower as usize
	};
	// Generate the random string
	Ok(Alphanumeric.sample_string(&mut rand::thread_rng(), len).into())
}

pub fn duration((dur1, dur2): (Duration, Duration)) -> Result<Value> {
	// Sort from low to high
	let (from, to) = if dur2 > dur1 {
		(dur1, dur2)
	} else {
		(dur2, dur1)
	};

	let rand = rand::thread_rng().gen_range(from.as_nanos()..=to.as_nanos());

	let nanos = (rand % 1_000_000_000) as u32;

	// Max Duration is made of (u64::MAX, NANOS_PER_SEC - 1) so will never overflow
	let Ok(secs) = u64::try_from(rand / 1_000_000_000) else {
		fail!("Overflow inside rand::duration()");
	};

	Ok(Value::Duration(Duration::new(secs, nanos)))
}

pub fn time((NoneOrRange(range),): (NoneOrRange<Value>,)) -> Result<Value> {
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
			bail!(Error::InvalidArguments {
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
				_ => bail!(Error::InvalidArguments {
					name: String::from("rand::time"),
					message: format!(
						"To generate a random time, the 2 arguments must be numbers between {MINIMUM} and {LIMIT} seconds from the UNIX epoch or a 'datetime' within the range d'-262143-01-01T00:00:00Z' and +262142-12-31T23:59:59Z'."
					),
				}),
			},
			_ => bail!(Error::InvalidArguments {
				name: String::from("rand::time"),
				message: format!(
					"To generate a random time, the 2 arguments must be numbers between {MINIMUM} and {LIMIT} seconds from the UNIX epoch or a 'datetime' within the range d'-262143-01-01T00:00:00Z' and +262142-12-31T23:59:59Z'."
				),
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
	fail!("Expected a valid datetime, but were unable to generate one")
}

pub fn ulid((Optional(timestamp),): (Optional<Datetime>,)) -> Result<Value> {
	let ulid = match timestamp {
		Some(timestamp) => {
			#[cfg(target_family = "wasm")]
			ensure!(
				timestamp.0 >= chrono::DateTime::UNIX_EPOCH,
				Error::InvalidArguments {
					name: String::from("rand::ulid"),
					message: format!(
						"To generate a ULID from a datetime, it must be a time beyond UNIX epoch."
					),
				}
			);

			Ulid::from_datetime(timestamp.0.into())
		}
		None => Ulid::new(),
	};

	Ok(ulid.to_string().into())
}

pub fn uuid((Optional(timestamp),): (Optional<Datetime>,)) -> Result<Value> {
	let uuid = match timestamp {
		Some(timestamp) => {
			#[cfg(target_family = "wasm")]
			ensure!(
				timestamp.0 >= chrono::DateTime::UNIX_EPOCH,
				Error::InvalidArguments {
					name: String::from("rand::ulid"),
					message: format!(
						"To generate a ULID from a datetime, it must be a time beyond UNIX epoch."
					),
				}
			);

			Uuid::new_v7_from_datetime(timestamp)
		}
		None => Uuid::new(),
	};
	Ok(uuid.into())
}

pub mod uuid {

	use anyhow::Result;

	use crate::fnc::args::Optional;
	use crate::val::{Datetime, Uuid, Value};

	pub fn v4(_: ()) -> Result<Value> {
		Ok(Uuid::new_v4().into())
	}

	pub fn v7((Optional(timestamp),): (Optional<Datetime>,)) -> Result<Value> {
		let uuid = match timestamp {
			Some(timestamp) => {
				#[cfg(target_family = "wasm")]
				anyhow::ensure!(
					timestamp.0 >= chrono::DateTime::UNIX_EPOCH,
					crate::err::Error::InvalidArguments {
						name: String::from("rand::ulid"),
						message: format!(
							"To generate a ULID from a datetime, it must be a time beyond UNIX epoch."
						),
					}
				);

				Uuid::new_v7_from_datetime(timestamp)
			}
			None => Uuid::new(),
		};
		Ok(uuid.into())
	}
}
