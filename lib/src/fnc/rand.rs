use crate::cnf::ID_CHARS;
use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::uuid::Uuid;
use crate::sql::value::Value;
use nanoid::nanoid;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::IteratorRandom;
use rand::Rng;

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

pub fn guid((len,): (Option<usize>,)) -> Result<Value, Error> {
	// Only need 53 to uniquely identify all atoms in observable universe.
	const LIMIT: usize = 64;

	let len = match len {
		Some(len) if len <= LIMIT => len,
		None => 20,
		_ => {
			return Err(Error::InvalidArguments {
				name: String::from("rand::guid"),
				message: format!("The maximum length of a GUID is {}.", LIMIT),
			})
		}
	};

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
	// Limit how much time and bandwidth is spent.
	const LIMIT: i64 = 2i64.pow(16);

	let len = if let Some((min, max)) = arg1.zip(arg2) {
		match min {
			min if (0..=LIMIT).contains(&min) => match max {
				max if min <= max && max <= LIMIT => rand::thread_rng().gen_range(min as usize..=max as usize),
				max if max >= 0 && max <= min => rand::thread_rng().gen_range(max as usize..=min as usize),
				_ => return Err(Error::InvalidArguments {
					name: String::from("rand::string"),
					message: format!("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers and no higher than {}.", LIMIT),
				}),
			},
			_ => return Err(Error::InvalidArguments {
				name: String::from("rand::string"),
				message: format!("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers and no higher than {}.", LIMIT),
			}),
		}
	} else if let Some(len) = arg1 {
		if (0..=LIMIT).contains(&len) {
			len as usize
		} else {
			return Err(Error::InvalidArguments {
				name: String::from("rand::string"),
				message: format!("To generate a string of X characters in length, the argument must be a positive number and no higher than {}.", LIMIT),
			});
		}
	} else {
		32
	};

	Ok(Alphanumeric.sample_string(&mut rand::thread_rng(), len).into())
}

pub fn time((range,): (Option<(i64, i64)>,)) -> Result<Value, Error> {
	let i = if let Some((min, max)) = range {
		let range = if max < min {
			max..=min
		} else {
			min..=max
		};
		rand::thread_rng().gen_range(range)
	} else {
		rand::random::<i32>() as i64
	};
	Ok(Datetime::from(i).into())
}

pub fn uuid(_: ()) -> Result<Value, Error> {
	Ok(Uuid::new().into())
}

pub mod uuid {

	use crate::err::Error;
	use crate::sql::uuid::Uuid;
	use crate::sql::value::Value;

	pub fn v4(_: ()) -> Result<Value, Error> {
		Ok(Uuid::new_v4().into())
	}

	pub fn v7(_: ()) -> Result<Value, Error> {
		Ok(Uuid::new_v7().into())
	}
}
