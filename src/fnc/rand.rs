use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::value::Value;
use rand::distributions::Alphanumeric;
use rand::Rng;
use uuid::Uuid;
use xid;

pub fn rand(_: &Runtime, _: Vec<Value>) -> Result<Value, Error> {
	Ok(rand::random::<f64>().into())
}

pub fn bool(_: &Runtime, _: Vec<Value>) -> Result<Value, Error> {
	Ok(rand::random::<bool>().into())
}

pub fn r#enum(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		0 => Ok(Value::None),
		1 => match args.remove(0) {
			Value::Array(mut v) => match v.value.len() {
				0 => Ok(Value::None),
				n => {
					let i = rand::thread_rng().gen_range(0..n);
					Ok(v.value.remove(i))
				}
			},
			v => Ok(v),
		},
		n => {
			let i = rand::thread_rng().gen_range(0..n);
			Ok(args.remove(i))
		}
	}
}

pub fn float(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0).as_float() {
			min => match args.remove(0).as_float() {
				max if max < min => Ok(rand::thread_rng().gen_range(max..=min).into()),
				max => Ok(rand::thread_rng().gen_range(min..=max).into()),
			},
		},
		0 => Ok(rand::random::<f64>().into()),
		_ => unreachable!(),
	}
}

pub fn guid(_: &Runtime, _: Vec<Value>) -> Result<Value, Error> {
	Ok(xid::new().to_string().into())
}

pub fn int(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0).as_int() {
			min => match args.remove(0).as_int() {
				max if max < min => Ok(rand::thread_rng().gen_range(max..=min).into()),
				max => Ok(rand::thread_rng().gen_range(min..=max).into()),
			},
		},
		0 => Ok(rand::random::<i64>().into()),
		_ => unreachable!(),
	}
}

pub fn string(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0).as_int() {
			min if min >= 0 => match args.remove(0).as_int() {
				max if max >= 0 && max < min => Ok(rand::thread_rng()
					.sample_iter(&Alphanumeric)
					.take(rand::thread_rng().gen_range(max as usize..=min as usize))
					.map(char::from)
					.collect::<String>()
					.into()),
				max if max >= 0 => Ok(rand::thread_rng()
					.sample_iter(&Alphanumeric)
					.take(rand::thread_rng().gen_range(min as usize..=max as usize))
					.map(char::from)
					.collect::<String>()
					.into()),
				_ => Err(Error::ArgumentsError {
					name: String::from("rand::string"),
					message: String::from("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers."),
				}),
			},
			_ => Err(Error::ArgumentsError {
				name: String::from("rand::string"),
				message: String::from("To generate a string of between X and Y characters in length, the 2 arguments must be positive numbers."),
			}),
		},
		1 => match args.remove(0).as_int() {
			x if x >= 0 => Ok(rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(x as usize)
				.map(char::from)
				.collect::<String>()
				.into()),
			_ => Err(Error::ArgumentsError {
				name: String::from("rand::string"),
				message: String::from("To generate a string of X characters in length, the argument must be a positive number."),
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

pub fn time(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0).as_int() {
			min => match args.remove(0).as_int() {
				max if max < min => {
					let i = rand::thread_rng().gen_range(max..=min);
					Ok(Datetime::from(i).into())
				}
				max => {
					let i = rand::thread_rng().gen_range(min..=max);
					Ok(Datetime::from(i).into())
				}
			},
		},
		0 => {
			let i = rand::random::<i64>();
			Ok(Datetime::from(i).into())
		}
		_ => unreachable!(),
	}
}

pub fn uuid(_: &Runtime, _: Vec<Value>) -> Result<Value, Error> {
	Ok(Uuid::new_v4().to_string().into())
}
