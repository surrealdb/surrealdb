use crate::dbs::Runtime;
use crate::err::Error;
use crate::fnc::util::math::bottom::Bottom;
use crate::fnc::util::math::deviation::Deviation;
use crate::fnc::util::math::interquartile::Interquartile;
use crate::fnc::util::math::mean::Mean;
use crate::fnc::util::math::median::Median;
use crate::fnc::util::math::midhinge::Midhinge;
use crate::fnc::util::math::mode::Mode;
use crate::fnc::util::math::nearestrank::Nearestrank;
use crate::fnc::util::math::percentile::Percentile;
use crate::fnc::util::math::spread::Spread;
use crate::fnc::util::math::top::Top;
use crate::fnc::util::math::trimean::Trimean;
use crate::fnc::util::math::variance::Variance;
use crate::sql::number::Number;
use crate::sql::value::Value;

pub fn abs(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_number().abs().into())
}

pub fn bottom(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => {
			let c = args.remove(0).as_int();
			Ok(v.as_numbers().bottom(c).into())
		}
		_ => Ok(Value::None),
	}
}

pub fn ceil(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_number().ceil().into())
}

pub fn fixed(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	let v = args.remove(0);
	match args.remove(0).as_int() {
		p if p > 0 => Ok(v.as_number().fixed(p as usize).into()),
		_ => Err(Error::ArgumentsError {
			name: String::from("math::fixed"),
			message: String::from("The second argument must be an integer greater than 0."),
		}),
	}
}

pub fn floor(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_number().floor().into())
}

pub fn interquartile(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().interquartile().into()),
		_ => Ok(Value::None),
	}
}

pub fn max(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match v.as_numbers().into_iter().max() {
			Some(v) => Ok(v.into()),
			None => Ok(Value::None),
		},
		v => Ok(v),
	}
}

pub fn mean(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().mean().into()),
		_ => Ok(Value::None),
	}
}

pub fn median(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().median().into()),
		_ => Ok(Value::None),
	}
}

pub fn midhinge(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().midhinge().into()),
		_ => Ok(Value::None),
	}
}

pub fn min(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match v.as_numbers().into_iter().min() {
			Some(v) => Ok(v.into()),
			None => Ok(Value::None),
		},
		v => Ok(v),
	}
}

pub fn mode(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().mode().into()),
		_ => Ok(Value::None),
	}
}

pub fn nearestrank(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().nearestrank(args.remove(0).as_number()).into()),
		_ => Ok(Value::None),
	}
}

pub fn percentile(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().percentile(args.remove(0).as_number()).into()),
		_ => Ok(Value::None),
	}
}

pub fn product(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().into_iter().product::<Number>().into()),
		_ => Ok(Value::None),
	}
}

pub fn round(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_number().round().into())
}

pub fn spread(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().spread().into()),
		_ => Ok(Value::None),
	}
}

pub fn sqrt(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	Ok(args.remove(0).as_number().sqrt().into())
}

pub fn stddev(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().deviation().into()),
		_ => Ok(Value::None),
	}
}

pub fn sum(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().into_iter().sum::<Number>().into()),
		_ => Ok(Value::None),
	}
}

pub fn top(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => {
			let c = args.remove(0).as_int();
			Ok(v.as_numbers().top(c).into())
		}
		_ => Ok(Value::None),
	}
}

pub fn trimean(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().trimean().into()),
		_ => Ok(Value::None),
	}
}

pub fn variance(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().variance().into()),
		_ => Ok(Value::None),
	}
}
