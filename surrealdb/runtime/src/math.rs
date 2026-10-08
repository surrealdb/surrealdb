use core::f64;

use anyhow::{Result, bail, ensure};
use surrealdb_cnf::GENERATION_ALLOCATION_LIMIT;
use surrealdb_expr::expr::Error;
use surrealdb_expr::val::number::Sorted;
use surrealdb_expr::val::{Number, TryPow, Value};
use surrealdb_types::ToSql;

use crate::util::math::interquartile::Interquartile;
use crate::util::math::median::Median;
use crate::util::math::midhinge::Midhinge;
use crate::util::math::nearestrank::Nearestrank;
use crate::util::math::percentile::Percentile;
use crate::util::math::trimean::Trimean;
use crate::{fuel, util};

/// Define each synchronous function as its walk driven to its end at once
/// ([`fuel::now`]).
macro_rules! now {
	($($name:ident = $walk:ident($args:ty);)+) => {
		$(
			pub fn $name(args: $args) -> Result<Value> {
				fuel::now($walk(args))
			}
		)+
	};
}

now! {
	bottom = bottom_walk((Vec<Number>, i64));
	interquartile = interquartile_walk((Vec<Number>,));
	max = max_walk((Vec<Number>,));
	mean = mean_walk((Vec<Number>,));
	median = median_walk((Vec<Number>,));
	midhinge = midhinge_walk((Vec<Number>,));
	min = min_walk((Vec<Number>,));
	mode = mode_walk((Vec<Number>,));
	nearestrank = nearestrank_walk((Vec<Number>, Number));
	percentile = percentile_walk((Vec<Number>, Number));
	product = product_walk((Vec<Number>,));
	spread = spread_walk((Vec<Number>,));
	stddev = stddev_walk((Vec<Number>,));
	sum = sum_walk((Vec<Number>,));
	top = top_walk((Vec<Number>, i64));
	trimean = trimean_walk((Vec<Number>,));
	variance = variance_walk((Vec<Number>,));
}

/// `array` in ascending order, a quantum at a time.
async fn sorted(array: Vec<Number>) -> Sorted<Vec<Number>> {
	Sorted(fuel::sort_by(array, Number::cmp, usize::MAX, fuel::quantum()).await)
}

pub fn abs((arg,): (Number,)) -> Result<Value> {
	let Some(x) = arg.checked_abs() else {
		bail!(Error::ArithmeticOverflow(format!("math::abs({})", arg.to_sql())));
	};
	Ok(x.into())
}

pub fn acos((arg,): (Number,)) -> Result<Value> {
	Ok(arg.acos().into())
}

pub fn acot((arg,): (Number,)) -> Result<Value> {
	Ok(arg.acot().into())
}

pub fn asin((arg,): (Number,)) -> Result<Value> {
	Ok(arg.asin().into())
}

pub fn atan((arg,): (Number,)) -> Result<Value> {
	Ok(arg.atan().into())
}

pub async fn bottom_walk((array, c): (Vec<Number>, i64)) -> Result<Value> {
	ensure!(
		c > 0,
		Error::InvalidFunctionArguments {
			name: String::from("math::bottom"),
			message: String::from("The second argument must be an integer greater than 0."),
		}
	);
	Ok(util::math::bottom::bottom(array, c)
		.await
		.into_iter()
		.map(Value::from)
		.collect::<Vec<_>>()
		.into())
}

pub fn ceil((arg,): (Number,)) -> Result<Value> {
	Ok(arg.ceil().into())
}

pub fn clamp((arg, min, max): (Number, Number, Number)) -> Result<Value> {
	ensure!(
		min <= max,
		Error::InvalidFunctionArguments {
			name: "math::clamp".to_string(),
			message: "Lowerbound for clamp must be smaller than the upperbound".to_string(),
		}
	);
	Ok(arg.clamp(min, max).into())
}

pub fn cos((arg,): (Number,)) -> Result<Value> {
	Ok(arg.cos().into())
}
pub fn cot((arg,): (Number,)) -> Result<Value> {
	Ok(arg.cot().into())
}

pub fn deg2rad((arg,): (Number,)) -> Result<Value> {
	Ok(arg.deg2rad().into())
}

pub fn fixed((arg, p): (Number, i64)) -> Result<Value> {
	ensure!(
		p > 0,
		Error::InvalidFunctionArguments {
			name: String::from("math::fixed"),
			message: String::from("The second argument must be an integer greater than 0."),
		}
	);
	// Bound the intermediate `format!` allocation in `Number::fixed`.
	ensure!(
		p <= *GENERATION_ALLOCATION_LIMIT as i64,
		Error::InvalidFunctionArguments {
			name: String::from("math::fixed"),
			message: format!(
				"The second argument must not exceed {}.",
				*GENERATION_ALLOCATION_LIMIT
			),
		}
	);
	Ok(arg.fixed(p as usize).into())
}

pub fn floor((arg,): (Number,)) -> Result<Value> {
	Ok(arg.floor().into())
}

pub async fn interquartile_walk((array,): (Vec<Number>,)) -> Result<Value> {
	let sorted = sorted(array).await;
	Ok(Sorted(&sorted.0).interquartile().into())
}

pub fn lerp((from, to, factor): (Number, Number, Number)) -> Result<Value> {
	Ok(factor.lerp(from, to).into())
}

pub fn lerpangle((from, to, factor): (Number, Number, Number)) -> Result<Value> {
	Ok(factor.lerp_angle(from, to).into())
}

pub fn ln((arg,): (Number,)) -> Result<Value> {
	Ok(arg.ln().into())
}

pub fn log((arg, base): (Number, Number)) -> Result<Value> {
	Ok(arg.log(base).into())
}

pub fn log10((arg,): (Number,)) -> Result<Value> {
	Ok(arg.log10().into())
}

pub fn log2((arg,): (Number,)) -> Result<Value> {
	Ok(arg.log2().into())
}

pub async fn max_walk((array,): (Vec<Number>,)) -> Result<Value> {
	// The last of equal greatest numbers, as `Iterator::max` answers.
	let max = fuel::fold(array, None, |max: Option<Number>, n| match max {
		Some(max) if max > n => Some(max),
		_ => Some(n),
	})
	.await;
	Ok(match max {
		Some(v) => v.into(),
		None => f64::NEG_INFINITY.into(),
	})
}

pub async fn mean_walk((array,): (Vec<Number>,)) -> Result<Value> {
	util::math::mean(&array).await.map(Value::Number)
}

pub async fn median_walk((array,): (Vec<Number>,)) -> Result<Value> {
	Ok(if array.is_empty() {
		Value::None
	} else {
		let sorted = sorted(array).await;
		Sorted(&sorted.0).median().into()
	})
}

pub async fn midhinge_walk((array,): (Vec<Number>,)) -> Result<Value> {
	let sorted = sorted(array).await;
	Ok(Sorted(&sorted.0).midhinge().into())
}

pub async fn min_walk((array,): (Vec<Number>,)) -> Result<Value> {
	// The first of equal least numbers, as `Iterator::min` answers.
	let min = fuel::fold(array, None, |min: Option<Number>, n| match min {
		Some(min) if min <= n => Some(min),
		_ => Some(n),
	})
	.await;
	Ok(match min {
		Some(v) => v.into(),
		None => f64::INFINITY.into(),
	})
}

pub async fn mode_walk((array,): (Vec<Number>,)) -> Result<Value> {
	Ok(util::math::mode::mode(array).await.into())
}

pub async fn nearestrank_walk((array, n): (Vec<Number>, Number)) -> Result<Value> {
	let sorted = sorted(array).await;
	Ok(Sorted(&sorted.0).nearestrank(n).into())
}

pub async fn percentile_walk((array, n): (Vec<Number>, Number)) -> Result<Value> {
	let sorted = sorted(array).await;
	Ok(Sorted(&sorted.0).percentile(n).into())
}

pub fn pow((arg, pow): (Number, Number)) -> Result<Value> {
	Ok(arg.try_pow(pow)?.into())
}

pub async fn product_walk((array,): (Vec<Number>,)) -> Result<Value> {
	Ok(fuel::fold(array, Number::Int(1), |product, n| product * n).await.into())
}

pub fn rad2deg((arg,): (Number,)) -> Result<Value> {
	Ok(arg.rad2deg().into())
}

pub fn round((arg,): (Number,)) -> Result<Value> {
	Ok(arg.round().into())
}

pub fn sign((arg,): (Number,)) -> Result<Value> {
	Ok(arg.sign().into())
}

pub fn sin((arg,): (Number,)) -> Result<Value> {
	Ok(arg.sin().into())
}

pub async fn spread_walk((array,): (Vec<Number>,)) -> Result<Value> {
	let init = array.first().copied();
	let (min, max) = fuel::fold(array.iter(), (init, init), |(min, max), n| {
		(std::cmp::min(min, Some(*n)), std::cmp::max(max, Some(*n)))
	})
	.await;
	Ok(match (min, max) {
		(Some(min), Some(max)) => max - min,
		_ => Number::NAN,
	}
	.into())
}

pub fn sqrt((arg,): (Number,)) -> Result<Value> {
	if arg >= Number::Int(0) {
		Ok(arg.sqrt().into())
	} else {
		Ok(f64::NAN.into())
	}
}

pub async fn stddev_walk((array,): (Vec<Number>,)) -> Result<Value> {
	util::math::deviation(&array).await.map(Value::Number)
}

pub async fn sum_walk((array,): (Vec<Number>,)) -> Result<Value> {
	Ok(fuel::fold(array, Number::Int(0), |sum, n| sum + n).await.into())
}
pub fn tan((arg,): (Number,)) -> Result<Value> {
	Ok(arg.tan().into())
}

pub async fn top_walk((array, c): (Vec<Number>, i64)) -> Result<Value> {
	ensure!(
		c > 0,
		Error::InvalidFunctionArguments {
			name: String::from("math::top"),
			message: String::from("The second argument must be an integer greater than 0."),
		}
	);
	Ok(util::math::top::top(array, c).await.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub async fn trimean_walk((array,): (Vec<Number>,)) -> Result<Value> {
	let sorted = sorted(array).await;
	Ok(Sorted(&sorted.0).trimean().into())
}

pub async fn variance_walk((array,): (Vec<Number>,)) -> Result<Value> {
	util::math::variance(&array).await.map(Value::Number)
}
