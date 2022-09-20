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

pub fn abs((arg,): (Number,)) -> Result<Value, Error> {
	Ok(arg.abs().into())
}

pub fn bottom((array, c): (Value, i64)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().bottom(c).into()),
		_ => Ok(Value::None),
	}
}

pub fn ceil((arg,): (Number,)) -> Result<Value, Error> {
	Ok(arg.ceil().into())
}

pub fn fixed((v, p): (Number, i64)) -> Result<Value, Error> {
	if p > 0 {
		Ok(v.fixed(p as usize).into())
	} else {
		Err(Error::InvalidArguments {
			name: String::from("math::fixed"),
			message: String::from("The second argument must be an integer greater than 0."),
		})
	}
}

pub fn floor((arg,): (Number,)) -> Result<Value, Error> {
	Ok(arg.floor().into())
}

pub fn interquartile((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().interquartile().into()),
		_ => Ok(Value::None),
	}
}

pub fn max((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => match v.as_numbers().into_iter().max() {
			Some(v) => Ok(v.into()),
			None => Ok(Value::None),
		},
		v => Ok(v),
	}
}

pub fn mean((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => match v.is_empty() {
			true => Ok(Value::None),
			false => Ok(v.as_numbers().mean().into()),
		},
		_ => Ok(Value::None),
	}
}

pub fn median((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => match v.is_empty() {
			true => Ok(Value::None),
			false => Ok(v.as_numbers().median().into()),
		},
		_ => Ok(Value::None),
	}
}

pub fn midhinge((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().midhinge().into()),
		_ => Ok(Value::None),
	}
}

pub fn min((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => match v.as_numbers().into_iter().min() {
			Some(v) => Ok(v.into()),
			None => Ok(Value::None),
		},
		v => Ok(v),
	}
}

pub fn mode((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().mode().into()),
		_ => Ok(Value::None),
	}
}

pub fn nearestrank((array, n): (Value, Number)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().nearestrank(n).into()),
		_ => Ok(Value::None),
	}
}

pub fn percentile((array, n): (Value, Number)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().percentile(n).into()),
		_ => Ok(Value::None),
	}
}

pub fn product((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().into_iter().product::<Number>().into()),
		_ => Ok(Value::None),
	}
}

pub fn round((arg,): (Number,)) -> Result<Value, Error> {
	Ok(arg.round().into())
}

pub fn spread((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().spread().into()),
		_ => Ok(Value::None),
	}
}

pub fn sqrt((arg,): (Number,)) -> Result<Value, Error> {
	Ok(match arg {
		v if v >= Number::Int(0) => v.sqrt().into(),
		_ => Value::None,
	})
}

pub fn stddev((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().deviation().into()),
		_ => Ok(Value::None),
	}
}

pub fn sum((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().into_iter().sum::<Number>().into()),
		v => Ok(v.as_number().into()),
	}
}

pub fn top((array, c): (Value, i64)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().top(c).into()),
		_ => Ok(Value::None),
	}
}

pub fn trimean((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().trimean().into()),
		_ => Ok(Value::None),
	}
}

pub fn variance((array,): (Value,)) -> Result<Value, Error> {
	match array {
		Value::Array(v) => Ok(v.as_numbers().variance().into()),
		_ => Ok(Value::None),
	}
}
