use crate::cnf::ID_CHARS;
use crate::ctx::Context;
use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::uuid::Uuid;
use crate::sql::value::Value;
use nanoid::nanoid;
use rand::distributions::Alphanumeric;
use rand::prelude::IteratorRandom;
use rand::Rng;

pub fn rand(_: &Context, _: Vec<Value>) -> Result<Value, Error> {
	Ok(rand::random::<f64>().into())
}

pub fn bool(_: &Context, _: Vec<Value>) -> Result<Value, Error> {
	Ok(rand::random::<bool>().into())
}

pub fn r#enum(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(match args.len() {
		0 => Value::None,
		1 => match args.remove(0) {
			Value::Array(v) => v.into_iter().choose(&mut rand::thread_rng()).unwrap_or(Value::None),
			v => v,
		},
		_ => args.into_iter().choose(&mut rand::thread_rng()).unwrap(),
	})
}

pub fn float(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => {
			let min = args.remove(0).as_float();
			match args.remove(0).as_float() {
				max if max < min => Ok(rand::thread_rng().gen_range(max..=min).into()),
				max => Ok(rand::thread_rng().gen_range(min..=max).into()),
			}
		}
		0 => Ok(rand::random::<f64>().into()),
		_ => unreachable!(),
	}
}

pub fn guid(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		1 => {
			// Only need 53 to uniquely identify all atoms in observable universe.
			const LIMIT: usize = 64;
			let len = args.remove(0).as_int() as usize;
			if len > LIMIT {
				Err(Error::InvalidArguments {
					name: String::from("rand::guid"),
					message: format!("The maximum length of a GUID is {}.", LIMIT),
				})
			} else {
				Ok(nanoid!(len, &ID_CHARS).into())
			}
		}
		0 => Ok(nanoid!(20, &ID_CHARS).into()),
		_ => unreachable!(),
	}
}

pub fn int(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => {
			let min = args.remove(0).as_int();
			match args.remove(0).as_int() {
				max if max < min => Ok(rand::thread_rng().gen_range(max..=min).into()),
				max => Ok(rand::thread_rng().gen_range(min..=max).into()),
			}
		}
		0 => Ok(rand::random::<i64>().into()),
		_ => unreachable!(),
	}
}

pub fn string(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	// Limit how much time and bandwidth is spent.
	const LIMIT: i64 = 2i64.pow(16);
	match args.len() {
		2 => match args.remove(0).as_int() {
			min if min >= 0 && min <= LIMIT => match args.remove(0).as_int() {
				max if min <= max && max <= LIMIT => Ok(rand::thread_rng()
					.sample_iter(&Alphanumeric)
					.take(rand::thread_rng().gen_range(min as usize..=max as usize))
					.map(char::from)
					.collect::<String>()
					.into()),
				max if max >= 0 && max <= min => Ok(rand::thread_rng()
					.sample_iter(&Alphanumeric)
					.take(rand::thread_rng().gen_range(max as usize..=min as usize))
					.map(char::from)
					.collect::<String>()
					.into()),
				_ => Err(Error::InvalidArguments {
					name: String::from("rand::string"),
					message: format!("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers and no higher than {}.", LIMIT),
				}),
			},
			_ => Err(Error::InvalidArguments {
				name: String::from("rand::string"),
				message: format!("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers and no higher than {}.", LIMIT),
			}),
		},
		1 => match args.remove(0).as_int() {
			x if x >= 0 && x <= LIMIT => Ok(rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(x as usize)
				.map(char::from)
				.collect::<String>()
				.into()),
			_ => Err(Error::InvalidArguments {
				name: String::from("rand::string"),
				message: format!("To generate a string of X characters in length, the argument must be a positive number and no higher than {}.", LIMIT),
			}),
		},
		0 => Ok(rand::thread_rng()
			.sample_iter(&Alphanumeric)
			.take(32)
			.map(char::from)
			.collect::<String>()
			.into()),
		_ => unreachable!(),
	}
}

pub fn time(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => {
			let min = args.remove(0).as_int();
			match args.remove(0).as_int() {
				max if max < min => {
					let i = rand::thread_rng().gen_range(max..=min);
					Ok(Datetime::from(i).into())
				}
				max => {
					let i = rand::thread_rng().gen_range(min..=max);
					Ok(Datetime::from(i).into())
				}
			}
		}
		0 => {
			let i = rand::random::<i32>();
			Ok(Datetime::from(i as i64).into())
		}
		_ => unreachable!(),
	}
}

pub fn uuid(_: &Context, _: Vec<Value>) -> Result<Value, Error> {
	Ok(Uuid::new().into())
}
