use core::f64;

use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, MathematicalOps};
use surrealdb_expr::val::{Number, TryAdd, TryPow};

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

pub async fn mean(array: &[Number]) -> Result<Number> {
	if array.is_empty() {
		return Ok(f64::NAN.into());
	}
	let len = array.len();
	let (sum, all_int) =
		crate::fuel::try_fold(array, (Number::Int(0), true), |(sum, all_int), n| {
			Ok::<_, anyhow::Error>((sum.add_exact(*n)?, all_int && n.is_int()))
		})
		.await?;
	match sum {
		// Integers are summed exactly, so the result is the one division of
		// the true total by the count and does not depend on element order.
		Number::Int(x) => Ok((x as f64 / len as f64).into()),
		Number::Float(x) => Ok((x / len as f64).into()),
		// Decimal inputs keep a decimal mean. A decimal total of integers
		// (one that left `i64`) is a float mean, as for any integers.
		Number::Decimal(d) if all_int => Ok((d.to_f64().unwrap_or(f64::NAN) / len as f64).into()),
		Number::Decimal(d) => Ok((d / Decimal::from(len)).into()),
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
	let sum = crate::fuel::try_fold(array, Number::from(0.0), |sum, n| {
		sum.try_add((*n - mean).try_pow(2.into())?)
	})
	.await?;
	match sum {
		Number::Int(_) => unreachable!(),
		Number::Float(x) => Ok(Number::from(x / div as f64)),
		Number::Decimal(x) => Ok(Number::from(x / Decimal::from(div))),
	}
}

pub async fn deviation(array: &[Number]) -> Result<Number> {
	match variance(array).await? {
		Number::Int(_) => unreachable!(),
		Number::Float(x) => Ok(Number::from(x.sqrt())),
		Number::Decimal(x) => {
			Ok(x.sqrt().map(Number::from).unwrap_or_else(|| Number::from(f64::NAN)))
		}
	}
}
