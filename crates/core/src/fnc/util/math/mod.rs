use core::f64;

use anyhow::Result;
use rust_decimal::{Decimal, MathematicalOps};

use crate::val::{Number, TryAdd, TryPow};

pub mod bottom;
pub mod interquartile;
pub mod median;
pub mod midhinge;
pub mod mode;
pub mod nearestrank;
pub mod percentile;
pub mod spread;
pub mod top;
pub mod trimean;
pub mod vector;

pub(crate) trait ToFloat {
	fn to_float(&self) -> f64;
}

impl ToFloat for f64 {
	fn to_float(&self) -> f64 {
		*self
	}
}

impl ToFloat for f32 {
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i64 {
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i32 {
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i16 {
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

pub(crate) fn mean(array: &[Number]) -> Result<Number> {
	if array.is_empty() {
		return Ok(f64::NAN.into());
	}
	let len = array.len();
	let mut sum: Number = 0.0f64.into();
	for n in array {
		sum = sum.try_add(*n)?;
	}
	match sum {
		Number::Int(_) => unreachable!(),
		Number::Float(x) => Ok((x / len as f64).into()),
		Number::Decimal(d) => Ok((d / Decimal::from(len)).into()),
	}
}

pub(crate) fn variance(array: &[Number]) -> Result<Number> {
	match array.len() {
		0 => return Ok(f64::NAN.into()),
		1 => return Ok(0.0.into()),
		_ => {}
	}
	let mean = mean(array)?;
	let div = array.len() - 1;
	let mut sum = Number::from(0.0);
	for n in array.iter() {
		sum = sum.try_add((*n - mean).try_pow(2.into())?)?;
	}
	match sum {
		Number::Int(_) => unreachable!(),
		Number::Float(x) => Ok(Number::from(x / div as f64)),
		Number::Decimal(x) => Ok(Number::from(x / Decimal::from(div))),
	}
}

pub(crate) fn deviation(array: &[Number]) -> Result<Number> {
	match variance(array)? {
		Number::Int(_) => unreachable!(),
		Number::Float(x) => Ok(Number::from(x.sqrt())),
		Number::Decimal(x) => {
			Ok(x.sqrt().map(Number::from).unwrap_or_else(|| Number::from(f64::NAN)))
		}
	}
}
