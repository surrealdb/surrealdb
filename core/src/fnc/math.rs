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
use crate::sql::number::{Number, Sort};
use crate::sql::value::{TryPow, Value};

pub fn abs((arg,): (Number,)) -> Result<Value, Error> {
	Ok(arg.abs().into())
}

pub fn bottom((array, c): (Vec<Number>, i64)) -> Result<Value, Error> {
	if c > 0 {
		Ok(array.bottom(c).into())
	} else {
		Err(Error::InvalidArguments {
			name: String::from("math::bottom"),
			message: String::from("The second argument must be an integer greater than 0."),
		})
	}
}

pub fn ceil((arg,): (Number,)) -> Result<Value, Error> {
	Ok(arg.ceil().into())
}

pub fn fixed((arg, p): (Number, i64)) -> Result<Value, Error> {
	if p > 0 {
		Ok(arg.fixed(p as usize).into())
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

pub fn interquartile((mut array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.sorted().interquartile().into())
}

pub fn max((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(match array.into_iter().max() {
		Some(v) => v.into(),
		None => Value::None,
	})
}

pub fn mean((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.mean().into())
}

pub fn median((mut array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(match array.is_empty() {
		true => Value::None,
		false => array.sorted().median().into(),
	})
}

pub fn midhinge((mut array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.sorted().midhinge().into())
}

pub fn min((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(match array.into_iter().min() {
		Some(v) => v.into(),
		None => Value::None,
	})
}

pub fn mode((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.mode().into())
}

pub fn nearestrank((mut array, n): (Vec<Number>, Number)) -> Result<Value, Error> {
	Ok(array.sorted().nearestrank(n).into())
}

pub fn percentile((mut array, n): (Vec<Number>, Number)) -> Result<Value, Error> {
	Ok(array.sorted().percentile(n).into())
}

pub fn pow((arg, pow): (Number, Number)) -> Result<Value, Error> {
	Ok(arg.try_pow(pow)?.into())
}

pub fn product((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.into_iter().product::<Number>().into())
}

pub fn round((arg,): (Number,)) -> Result<Value, Error> {
	Ok(arg.round().into())
}

pub fn spread((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.spread().into())
}

pub fn sqrt((arg,): (Number,)) -> Result<Value, Error> {
	Ok(match arg {
		v if v >= Number::Int(0) => v.sqrt().into(),
		_ => Value::None,
	})
}

pub fn stddev((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.deviation(true).into())
}

pub fn sum((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.into_iter().sum::<Number>().into())
}

pub fn top((array, c): (Vec<Number>, i64)) -> Result<Value, Error> {
	if c > 0 {
		Ok(array.top(c).into())
	} else {
		Err(Error::InvalidArguments {
			name: String::from("math::top"),
			message: String::from("The second argument must be an integer greater than 0."),
		})
	}
}

pub fn trimean((mut array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.sorted().trimean().into())
}

pub fn variance((array,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(array.variance(true).into())
}
