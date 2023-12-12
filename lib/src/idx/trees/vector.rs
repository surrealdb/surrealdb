use crate::err::Error;
use crate::sql::index::VectorType;
use crate::sql::Number;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// In the context of a Symmetric MTree index, the term object refers to a vector, representing the indexed item.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[revisioned(revision = 1)]
pub enum Vector {
	F64(Vec<f64>),
	F32(Vec<f32>),
	I64(Vec<i64>),
	I32(Vec<i32>),
	I16(Vec<i16>),
}

/// For vectors, as we want to support very large vectors, we want to avoid copy or clone.
/// So the requirement is multiple ownership but not thread safety.
/// However, because we are running in an async context, and because we are using cache structures that use the Arc as a key,
/// the cached objects has to be Sent, which then requires the use of Arc (rather than just Rc).
pub(crate) type SharedVector = Arc<Vector>;

impl Hash for Vector {
	fn hash<H: Hasher>(&self, state: &mut H) {
		use Vector::*;
		match self {
			F64(v) => {
				0.hash(state);
				for item in v {
					state.write_u64(item.to_bits());
				}
			}
			F32(v) => {
				1.hash(state);
				for item in v {
					state.write_u32(item.to_bits());
				}
			}
			I64(v) => {
				2.hash(state);
				for item in v {
					state.write_i64(*item);
				}
			}
			I32(v) => {
				3.hash(state);
				for item in v {
					state.write_i32(*item);
				}
			}
			I16(v) => {
				4.hash(state);
				for item in v {
					state.write_i16(*item);
				}
			}
		}
	}
}

impl PartialEq for Vector {
	fn eq(&self, other: &Self) -> bool {
		use Vector::*;
		match (self, other) {
			(F64(v), F64(v_o)) => v == v_o,
			(F32(v), F32(v_o)) => v == v_o,
			(I64(v), I64(v_o)) => v == v_o,
			(I32(v), I32(v_o)) => v == v_o,
			(I16(v), I16(v_o)) => v == v_o,
			_ => false,
		}
	}
}

impl Eq for Vector {}

impl PartialOrd for Vector {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Vector {
	fn cmp(&self, other: &Self) -> Ordering {
		use Vector::*;
		match (self, other) {
			(F64(v), F64(v_o)) => v.partial_cmp(v_o).unwrap_or(Ordering::Equal),
			(F32(v), F32(v_o)) => v.partial_cmp(v_o).unwrap_or(Ordering::Equal),
			(I64(v), I64(v_o)) => v.cmp(v_o),
			(I32(v), I32(v_o)) => v.cmp(v_o),
			(I16(v), I16(v_o)) => v.cmp(v_o),
			(F64(_), _) => Ordering::Less,
			(_, F64(_)) => Ordering::Greater,
			(F32(_), _) => Ordering::Less,
			(_, F32(_)) => Ordering::Greater,
			(I64(_), _) => Ordering::Less,
			(_, I64(_)) => Ordering::Greater,
			(I32(_), _) => Ordering::Less,
			(_, I32(_)) => Ordering::Greater,
		}
	}
}

impl Vector {
	pub(super) fn new(t: VectorType, l: usize) -> Self {
		match t {
			VectorType::F64 => Self::F64(Vec::with_capacity(l)),
			VectorType::F32 => Self::F32(Vec::with_capacity(l)),
			VectorType::I64 => Self::I64(Vec::with_capacity(l)),
			VectorType::I32 => Self::I32(Vec::with_capacity(l)),
			VectorType::I16 => Self::I16(Vec::with_capacity(l)),
		}
	}

	pub(super) fn add(&mut self, n: Number) {
		match self {
			Vector::F64(v) => v.push(n.to_float()),
			Vector::F32(v) => v.push(n.to_float() as f32),
			Vector::I64(v) => v.push(n.to_int()),
			Vector::I32(v) => v.push(n.to_int() as i32),
			Vector::I16(v) => v.push(n.to_int() as i16),
		};
	}

	pub(super) fn len(&self) -> usize {
		match self {
			Vector::F64(v) => v.len(),
			Vector::F32(v) => v.len(),
			Vector::I64(v) => v.len(),
			Vector::I32(v) => v.len(),
			Vector::I16(v) => v.len(),
		}
	}

	fn check_same_dimension(fnc: &str, a: &Vector, b: &Vector) -> Result<(), Error> {
		if a.len() != b.len() {
			Err(Error::InvalidArguments {
				name: String::from(fnc),
				message: String::from("The two vectors must be of the same dimension."),
			})
		} else {
			Ok(())
		}
	}
	pub(super) fn euclidean_distance(&self, other: &Self) -> Result<f64, Error> {
		Self::check_same_dimension("vector::distance::euclidean", self, other)?;
		match (self, other) {
			(Vector::F64(a), Vector::F64(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).powi(2)).sum::<f64>().sqrt())
			}
			(Vector::F32(a), Vector::F32(b)) => Ok(a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (*a as f64 - *b as f64).powi(2))
				.sum::<f64>()
				.sqrt()),
			(Vector::I64(a), Vector::I64(b)) => {
				Ok((a.iter().zip(b.iter()).map(|(a, b)| (a - b).pow(2)).sum::<i64>() as f64).sqrt())
			}
			(Vector::I32(a), Vector::I32(b)) => {
				Ok((a.iter().zip(b.iter()).map(|(a, b)| (a - b).pow(2)).sum::<i32>() as f64).sqrt())
			}
			(Vector::I16(a), Vector::I16(b)) => {
				Ok((a.iter().zip(b.iter()).map(|(a, b)| (a - b).pow(2)).sum::<i16>() as f64).sqrt())
			}
			_ => Err(Error::Unreachable),
		}
	}

	pub(super) fn manhattan_distance(&self, other: &Self) -> Result<f64, Error> {
		Self::check_same_dimension("vector::distance::manhattan", self, other)?;
		match (self, other) {
			(Vector::F64(a), Vector::F64(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).sum())
			}
			(Vector::F32(a), Vector::F32(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (*a as f64 - *b as f64).abs()).sum::<f64>())
			}
			(Vector::I64(a), Vector::I64(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).sum::<i64>() as f64)
			}
			(Vector::I32(a), Vector::I32(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).sum::<i32>() as f64)
			}
			(Vector::I16(a), Vector::I16(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).sum::<i16>() as f64)
			}
			_ => Err(Error::Unreachable),
		}
	}
	pub(super) fn minkowski_distance(&self, other: &Self, order: &Number) -> Result<f64, Error> {
		Self::check_same_dimension("vector::distance::minkowski", self, other)?;
		let dist = match (self, other) {
			(Vector::F64(a), Vector::F64(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().powf(order.to_float()))
				.sum::<f64>(),
			(Vector::F32(a), Vector::F32(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().powf(order.to_float() as f32))
				.sum::<f32>() as f64,
			(Vector::I64(a), Vector::I64(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().pow(order.to_int() as u32))
				.sum::<i64>() as f64,
			(Vector::I32(a), Vector::I32(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().pow(order.to_int() as u32))
				.sum::<i32>() as f64,
			(Vector::I16(a), Vector::I16(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().pow(order.to_int() as u32))
				.sum::<i16>() as f64,
			_ => return Err(Error::Unreachable),
		};
		Ok(dist.powf(1.0 / order.to_float()))
	}
}
