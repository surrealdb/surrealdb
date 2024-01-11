use crate::err::Error;
use crate::fnc::util::math::mean::Mean;
use crate::fnc::util::math::vector::{check_same_dimension, PearsonSimilarity};
use crate::fnc::util::math::ToFloat;
use crate::sql::index::{Distance, VectorType};
use crate::sql::Number;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// In the context of a Symmetric MTree index, the term object refers to a vector, representing the indexed item.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[revisioned(revision = 1)]
pub enum TreeVector {
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
pub type SharedVector = Arc<TreeVector>;

impl Hash for TreeVector {
	fn hash<H: Hasher>(&self, state: &mut H) {
		use TreeVector::*;
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

impl PartialEq for TreeVector {
	fn eq(&self, other: &Self) -> bool {
		use TreeVector::*;
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

impl Eq for TreeVector {}

impl PartialOrd for TreeVector {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for TreeVector {
	fn cmp(&self, other: &Self) -> Ordering {
		use TreeVector::*;
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

impl TreeVector {
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
			TreeVector::F64(v) => v.push(n.to_float()),
			TreeVector::F32(v) => v.push(n.to_float() as f32),
			TreeVector::I64(v) => v.push(n.to_int()),
			TreeVector::I32(v) => v.push(n.to_int() as i32),
			TreeVector::I16(v) => v.push(n.to_int() as i16),
		};
	}

	pub(super) fn len(&self) -> usize {
		match self {
			TreeVector::F64(v) => v.len(),
			TreeVector::F32(v) => v.len(),
			TreeVector::I64(v) => v.len(),
			TreeVector::I32(v) => v.len(),
			TreeVector::I16(v) => v.len(),
		}
	}

	fn dot_f64(a: &[f64], b: &[f64]) -> f64 {
		a.iter().zip(b.iter()).map(|(a, b)| a * b).sum()
	}

	fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
		a.iter().zip(b.iter()).map(|(a, b)| a * b).sum()
	}

	fn dot_i64(a: &[i64], b: &[i64]) -> i64 {
		a.iter().zip(b.iter()).map(|(a, b)| a * b).sum()
	}

	fn dot_i32(a: &[i32], b: &[i32]) -> i32 {
		a.iter().zip(b.iter()).map(|(a, b)| a * b).sum()
	}

	fn dot_i16(a: &[i16], b: &[i16]) -> i16 {
		a.iter().zip(b.iter()).map(|(a, b)| a * b).sum()
	}

	fn magnitude_f64(v: &[f64]) -> f64 {
		v.iter().map(|a| a.powi(2)).sum::<f64>().sqrt()
	}

	fn magnitude_f32(v: &[f32]) -> f64 {
		(v.iter().map(|a| a.powi(2)).sum::<f32>() as f64).sqrt()
	}

	fn magnitude_i64(v: &[i64]) -> f64 {
		(v.iter().map(|a| a.pow(2)).sum::<i64>() as f64).sqrt()
	}

	fn magnitude_i32(v: &[i32]) -> f64 {
		(v.iter().map(|a| a.pow(2)).sum::<i32>() as f64).sqrt()
	}

	fn magnitude_i16(v: &[i16]) -> f64 {
		(v.iter().map(|a| a.pow(2)).sum::<i16>() as f64).sqrt()
	}

	pub(crate) fn chebyshev_distance(&self, other: &Self) -> Result<f64, Error> {
		check_same_dimension("vector::distance::chebyshev", self, other)?;
		match (self, other) {
			(TreeVector::F64(a), TreeVector::F64(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).fold(f64::MIN, f64::max))
			}
			(TreeVector::F32(a), TreeVector::F32(b)) => Ok(a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (*a as f64 - *b as f64).abs())
				.fold(f64::MIN, f64::max)),
			(TreeVector::I64(a), TreeVector::I64(b)) => Ok(a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (*a as f64 - *b as f64).abs())
				.fold(f64::MIN, f64::max)),
			(TreeVector::I32(a), TreeVector::I32(b)) => Ok(a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (*a as f64 - *b as f64).abs())
				.fold(f64::MIN, f64::max)),
			(TreeVector::I16(a), TreeVector::I16(b)) => Ok(a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (*a as f64 - *b as f64).abs())
				.fold(f64::MIN, f64::max)),
			_ => Err(Error::Unreachable("Vector::chebyshev_distance")),
		}
	}
	pub(crate) fn cosine_distance(&self, other: &Self) -> Result<f64, Error> {
		check_same_dimension("vector::distance::cosine", self, other)?;
		match (self, other) {
			(TreeVector::F64(a), TreeVector::F64(b)) => {
				Ok(Self::dot_f64(a, b) / Self::magnitude_f64(a) * Self::magnitude_f64(b))
			}
			(TreeVector::F32(a), TreeVector::F32(b)) => {
				Ok((Self::dot_f32(a, b) as f64) / Self::magnitude_f32(a) * Self::magnitude_f32(b))
			}
			(TreeVector::I64(a), TreeVector::I64(b)) => {
				Ok((Self::dot_i64(a, b) as f64) / Self::magnitude_i64(a) * Self::magnitude_i64(b))
			}
			(TreeVector::I32(a), TreeVector::I32(b)) => {
				Ok((Self::dot_i32(a, b) as f64) / Self::magnitude_i32(a) * Self::magnitude_i32(b))
			}
			(TreeVector::I16(a), TreeVector::I16(b)) => {
				Ok((Self::dot_i16(a, b) as f64) / Self::magnitude_i16(a) * Self::magnitude_i16(b))
			}
			_ => Err(Error::Unreachable("Vector::cosine_distance")),
		}
	}

	pub(crate) fn euclidean_distance(&self, other: &Self) -> Result<f64, Error> {
		check_same_dimension("vector::distance::euclidean", self, other)?;
		match (self, other) {
			(TreeVector::F64(a), TreeVector::F64(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).powi(2)).sum::<f64>().sqrt())
			}
			(TreeVector::F32(a), TreeVector::F32(b)) => Ok(a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (*a as f64 - *b as f64).powi(2))
				.sum::<f64>()
				.sqrt()),
			(TreeVector::I64(a), TreeVector::I64(b)) => {
				Ok((a.iter().zip(b.iter()).map(|(a, b)| (a - b).pow(2)).sum::<i64>() as f64).sqrt())
			}
			(TreeVector::I32(a), TreeVector::I32(b)) => {
				Ok((a.iter().zip(b.iter()).map(|(a, b)| (a - b).pow(2)).sum::<i32>() as f64).sqrt())
			}
			(TreeVector::I16(a), TreeVector::I16(b)) => {
				Ok((a.iter().zip(b.iter()).map(|(a, b)| (a - b).pow(2)).sum::<i16>() as f64).sqrt())
			}
			_ => Err(Error::Unreachable("Vector::euclidean_distance")),
		}
	}

	pub(crate) fn hamming_distance(&self, other: &Self) -> Result<f64, Error> {
		check_same_dimension("vector::distance::hamming", self, other)?;
		match (self, other) {
			(TreeVector::F64(a), TreeVector::F64(b)) => {
				Ok(a.iter().zip(b.iter()).filter(|&(a, b)| a != b).count() as f64)
			}
			(TreeVector::F32(a), TreeVector::F32(b)) => {
				Ok(a.iter().zip(b.iter()).filter(|&(a, b)| a != b).count() as f64)
			}
			(TreeVector::I64(a), TreeVector::I64(b)) => {
				Ok(a.iter().zip(b.iter()).filter(|&(a, b)| a != b).count() as f64)
			}
			(TreeVector::I32(a), TreeVector::I32(b)) => {
				Ok(a.iter().zip(b.iter()).filter(|&(a, b)| a != b).count() as f64)
			}
			(TreeVector::I16(a), TreeVector::I16(b)) => {
				Ok(a.iter().zip(b.iter()).filter(|&(a, b)| a != b).count() as f64)
			}
			_ => Err(Error::Unreachable("Vector::hamming_distance")),
		}
	}

	fn jaccard_f64(a: &[f64], b: &[f64]) -> f64 {
		let set_a: HashSet<u64> = HashSet::from_iter(a.iter().map(|f| f.to_bits()));
		let set_b: HashSet<u64> = HashSet::from_iter(b.iter().map(|f| f.to_bits()));
		let intersection_size = set_a.intersection(&set_b).count() as f64;
		let union_size = set_a.union(&set_b).count() as f64;
		intersection_size / union_size
	}

	fn jaccard_f32(a: &[f32], b: &[f32]) -> f64 {
		let set_a: HashSet<u32> = HashSet::from_iter(a.iter().map(|f| f.to_bits()));
		let set_b: HashSet<u32> = HashSet::from_iter(b.iter().map(|f| f.to_bits()));
		let intersection_size = set_a.intersection(&set_b).count() as f64;
		let union_size = set_a.union(&set_b).count() as f64;
		intersection_size / union_size
	}

	fn jaccard_integers<T>(a: &[T], b: &[T]) -> f64
	where
		T: Eq + Hash,
	{
		let set_a: HashSet<&T> = HashSet::from_iter(a.iter());
		let set_b: HashSet<&T> = HashSet::from_iter(b.iter());
		let intersection_size = set_a.intersection(&set_b).count() as f64;
		let union_size = set_a.union(&set_b).count() as f64;
		intersection_size / union_size
	}

	pub(crate) fn jaccard_similarity(&self, other: &Self) -> Result<f64, Error> {
		check_same_dimension("vector::distance::jaccard", self, other)?;
		match (self, other) {
			(TreeVector::F64(a), TreeVector::F64(b)) => Ok(Self::jaccard_f64(a, b)),
			(TreeVector::F32(a), TreeVector::F32(b)) => Ok(Self::jaccard_f32(a, b)),
			(TreeVector::I64(a), TreeVector::I64(b)) => Ok(Self::jaccard_integers(a, b)),
			(TreeVector::I32(a), TreeVector::I32(b)) => Ok(Self::jaccard_integers(a, b)),
			(TreeVector::I16(a), TreeVector::I16(b)) => Ok(Self::jaccard_integers(a, b)),
			_ => Err(Error::Unreachable("Vector::jaccard_similarity")),
		}
	}

	pub(crate) fn manhattan_distance(&self, other: &Self) -> Result<f64, Error> {
		check_same_dimension("vector::distance::manhattan", self, other)?;
		match (self, other) {
			(TreeVector::F64(a), TreeVector::F64(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).sum())
			}
			(TreeVector::F32(a), TreeVector::F32(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (*a as f64 - *b as f64).abs()).sum::<f64>())
			}
			(TreeVector::I64(a), TreeVector::I64(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).sum::<i64>() as f64)
			}
			(TreeVector::I32(a), TreeVector::I32(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).sum::<i32>() as f64)
			}
			(TreeVector::I16(a), TreeVector::I16(b)) => {
				Ok(a.iter().zip(b.iter()).map(|(a, b)| (a - b).abs()).sum::<i16>() as f64)
			}
			_ => Err(Error::Unreachable("Vector::manhattan_distance")),
		}
	}
	pub(crate) fn minkowski_distance(&self, other: &Self, order: &Number) -> Result<f64, Error> {
		check_same_dimension("vector::distance::minkowski", self, other)?;
		let dist = match (self, other) {
			(TreeVector::F64(a), TreeVector::F64(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().powf(order.to_float()))
				.sum::<f64>(),
			(TreeVector::F32(a), TreeVector::F32(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().powf(order.to_float() as f32))
				.sum::<f32>() as f64,
			(TreeVector::I64(a), TreeVector::I64(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().pow(order.to_int() as u32))
				.sum::<i64>() as f64,
			(TreeVector::I32(a), TreeVector::I32(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().pow(order.to_int() as u32))
				.sum::<i32>() as f64,
			(TreeVector::I16(a), TreeVector::I16(b)) => a
				.iter()
				.zip(b.iter())
				.map(|(a, b)| (a - b).abs().pow(order.to_int() as u32))
				.sum::<i16>() as f64,
			_ => return Err(Error::Unreachable("Vector::minkowski_distance")),
		};
		Ok(dist.powf(1.0 / order.to_float()))
	}

	fn pearson<T>(&a: &[T], b: &[T]) -> f64
	where
		T: Mean + ToFloat,
	{
		let m1 = a.mean();
		let m2 = b.mean();
		let covar: f64 =
			a.iter().zip(b.iter()).map(|(x, y)| (x.to_float() - m1) * (y.to_float() - m2)).sum();
		let covar = covar / a.len() as f64;
		let std_dev1 = crate::fnc::util::math::deviation::deviation(a, m1, false);
		let std_dev2 = crate::fnc::util::math::deviation::deviation(b, m2, false);
		Ok((covar / (std_dev1 * std_dev2)).into())
	}

	fn pearson_similarity(&self, other: &Self) -> Result<Number, Error> {
		check_same_dimension("vector::similarity::pearson", self, other)?;
		match (self, other) {
			(TreeVector::F64(a), TreeVector::F64(b)) => todo!(),
			(TreeVector::F32(a), TreeVector::F32(b)) => todo!(),
			(TreeVector::I64(a), TreeVector::I64(b)) => todo!(),
			(TreeVector::I32(a), TreeVector::I32(b)) => todo!(),
			(TreeVector::I16(a), TreeVector::I16(b)) => todo!(),
			_ => return Err(Error::Unreachable("Vector::pearson_similarity")),
		};
	}

	pub(crate) fn distance(&self, dist: &Distance, other: &Self) -> Result<f64, Error> {
		match dist {
			Distance::Chebyshev => self.chebyshev_distance(other),
			Distance::Cosine => self.cosine_distance(other),
			Distance::Euclidean => self.euclidean_distance(other),
			Distance::Hamming => self.hamming_distance(other),
			Distance::Jaccard => self.jaccard_similarity(other),
			Distance::Manhattan => self.manhattan_distance(other),
			Distance::Minkowski(order) => self.minkowski_distance(other, order),
			Distance::Pearson => todo!(),
		}
	}
}
