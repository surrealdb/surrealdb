use core::f64;

use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, MathematicalOps};
use surrealdb_expr::val::{Number, TryAdd, TryPow, Wide};
use surrealdb_types::BigDecimal;

pub mod bottom;
pub mod interquartile;
pub mod median;
pub mod midhinge;
pub mod mode;
pub mod nearestrank;
pub mod percentile;
pub mod top;
pub mod trimean;
pub mod vector;

/// Re-exported so a caller reaching for the vector helpers gets the trait
/// their element type must satisfy without a second import.
pub use surrealdb_expr::val::number::ToFloat;

const fn is_integer(n: &Number) -> bool {
	match n {
		Number::Int(_)
		| Number::I8(_)
		| Number::U8(_)
		| Number::I16(_)
		| Number::U16(_)
		| Number::I32(_)
		| Number::U32(_)
		| Number::U64(_) => true,
		Number::Float(_)
		| Number::Decimal(_)
		| Number::BigDecimal(_)
		| Number::F16(_)
		| Number::F32(_) => false,
	}
}

pub async fn mean(array: &[Number]) -> Result<Number> {
	if array.is_empty() {
		return Ok(f64::NAN.into());
	}
	let len = array.len();
	let (sum, all_int) =
		crate::fuel::try_fold(array, (Number::Int(0), true), |(sum, all_int), n| {
			Ok::<_, anyhow::Error>((sum.add_exact(n.clone())?, all_int && is_integer(n)))
		})
		.await?;
	match sum.wide() {
		// Integers are summed exactly, so the result is the one division of
		// the true total by the count and does not depend on element order.
		Wide::Int(x) => Ok((x as f64 / len as f64).into()),
		Wide::Float(x) => Ok((x / len as f64).into()),
		// Decimal inputs keep a decimal mean. A decimal total of integers
		// (one that left `i64`) is a float mean, as for any integers.
		Wide::Decimal(d) if all_int => Ok((d.to_f64().unwrap_or(f64::NAN) / len as f64).into()),
		Wide::Decimal(d) => Ok((d / Decimal::from(len)).into()),
		Wide::BigDecimal(d) if all_int => Ok((d.to_f64() / len as f64).into()),
		Wide::BigDecimal(d) => {
			Ok(Number::BigDecimal(Box::new(d.div_default(&BigDecimal::from(len as u64))?)))
		}
	}
}

pub async fn variance(array: &[Number]) -> Result<Number> {
	match array.len() {
		0 => return Ok(f64::NAN.into()),
		1 => return Ok(0.0.into()),
		_ => {}
	}
	let mean = mean(array).await?;
	let div = array.len() - 1;
	let init: Number = match mean.wide() {
		Wide::Decimal(_) | Wide::BigDecimal(_) => Decimal::ZERO.into(),
		Wide::Int(_) | Wide::Float(_) => 0.0.into(),
	};
	let sum = crate::fuel::try_fold(array, init, |sum, n| {
		sum.try_add((n.widened() - mean.clone()).try_pow(2.into())?)
	})
	.await?;
	match sum.wide() {
		Wide::Int(x) => Ok(Number::from(x as f64 / div as f64)),
		Wide::Float(x) => Ok(Number::from(x / div as f64)),
		Wide::Decimal(x) => Ok(Number::from(x / Decimal::from(div))),
		Wide::BigDecimal(x) => {
			Ok(Number::BigDecimal(Box::new(x.div_default(&BigDecimal::from(div as u64))?)))
		}
	}
}

pub async fn deviation(array: &[Number]) -> Result<Number> {
	match variance(array).await?.wide() {
		Wide::Int(x) => Ok(Number::from((x as f64).sqrt())),
		Wide::Float(x) => Ok(Number::from(x.sqrt())),
		Wide::Decimal(x) => {
			Ok(x.sqrt().map(Number::from).unwrap_or_else(|| Number::from(f64::NAN)))
		}
		// The square root is taken in `decimal`, so a variance beyond it is refused.
		Wide::BigDecimal(x) => match Decimal::try_from(x) {
			Ok(d) => Ok(d.sqrt().map(Number::from).unwrap_or_else(|| Number::from(f64::NAN))),
			Err(_) => Err(surrealdb_expr::expr::Error::InvalidFunctionArguments {
				name: "math::stddev".to_string(),
				message: format!(
					"The variance {x} is outside the range of a decimal, in which the deviation is computed."
				),
			}
			.into()),
		},
	}
}
