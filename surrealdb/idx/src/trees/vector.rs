//! Shared vector representations and distance helpers used by ANN index implementations.
//!
//! [`Vector`] is the in-memory ndarray representation used during search, while
//! [`SerializedVector`] is the persisted representation used in ANN keys and payloads. Values use
//! normal revisioned encoding; keys use a stable key-wire encoding so existing vector-document keys
//! remain reachable after new vector variants are added.

use std::cmp::PartialEq;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Arc, OnceLock};

use ahash::AHasher;
use anyhow::{Result, ensure};
use half::f16;
use ndarray::{Array1, Zip};
use ndarray_stats::DeviationExt;
// The persisted payload, its two encodings and the hash that turns one into a key
// are keyspace bytes, so they are declared below this layer; the in-memory form
// search operates on, and the conversion from a query value, stay here.
pub use surrealdb_datastore::values::vector::SerializedVector;
use surrealdb_runtime::util::math::ToFloat;
use surrealdb_runtime::util::math::vector::{pearson_rescaled, weighted_jaccard_similarity};

use super::kernels;
use crate::Error as IdxError;
use crate::catalog::{Distance, VectorType};
use crate::expr::Error;
use crate::val::{Number, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Vector {
	/// 64-bit floating-point vector.
	F64(Array1<f64>),
	/// 16-bit floating-point vector.
	F16(Array1<f16>),
	/// 32-bit floating-point vector.
	F32(Array1<f32>),
	/// 64-bit signed integer vector.
	I64(Array1<i64>),
	/// 32-bit signed integer vector.
	I32(Array1<i32>),
	/// 16-bit signed integer vector.
	I16(Array1<i16>),
	/// 8-bit signed integer vector.
	I8(Array1<i8>),
	/// 8-bit unsigned integer vector.
	U8(Array1<u8>),
}

impl From<&Vector> for SerializedVector {
	fn from(value: &Vector) -> Self {
		match value {
			Vector::F64(v) => Self::F64(v.to_vec()),
			Vector::F16(v) => Self::F16(v.iter().map(|v| v.to_bits()).collect()),
			Vector::F32(v) => Self::F32(v.to_vec()),
			Vector::I64(v) => Self::I64(v.to_vec()),
			Vector::I32(v) => Self::I32(v.to_vec()),
			Vector::I16(v) => Self::I16(v.to_vec()),
			Vector::I8(v) => Self::I8(v.to_vec()),
			Vector::U8(v) => Self::U8(v.to_vec()),
		}
	}
}

impl Vector {
	/// Overwrites this vector with `v`'s components, in place when both hold
	/// the same type and length, so a scratch vector is reused without
	/// allocating; otherwise replaces it.
	#[cfg(any(diskann, test))]
	pub(crate) fn assign(&mut self, v: &SerializedVector) {
		fn copy<T: Copy>(a: &mut Array1<T>, b: &[T]) -> bool {
			if a.len() != b.len() {
				return false;
			}
			// A vector built from a `Vec` is contiguous, so this is one memcpy.
			match a.as_slice_mut() {
				Some(slice) => slice.copy_from_slice(b),
				None => a.iter_mut().zip(b).for_each(|(x, y)| *x = *y),
			}
			true
		}
		fn convert<T, U: Copy>(a: &mut Array1<T>, b: &[U], f: impl Fn(U) -> T) -> bool {
			if a.len() != b.len() {
				return false;
			}
			// A contiguous vector converts in one flat loop over its slice,
			// which the compiler can vectorise.
			match a.as_slice_mut() {
				Some(slice) => slice.iter_mut().zip(b).for_each(|(x, y)| *x = f(*y)),
				None => a.iter_mut().zip(b).for_each(|(x, y)| *x = f(*y)),
			}
			true
		}
		let assigned = match (&mut *self, v) {
			(Self::F64(a), SerializedVector::F64(b)) => copy(a, b),
			(Self::F32(a), SerializedVector::F32(b)) => copy(a, b),
			(Self::I64(a), SerializedVector::I64(b)) => copy(a, b),
			(Self::I32(a), SerializedVector::I32(b)) => copy(a, b),
			(Self::I16(a), SerializedVector::I16(b)) => copy(a, b),
			(Self::I8(a), SerializedVector::I8(b)) => copy(a, b),
			(Self::U8(a), SerializedVector::U8(b)) => copy(a, b),
			(Self::F16(a), SerializedVector::F16(b)) => convert(a, b, f16::from_bits),
			_ => false,
		};
		if !assigned {
			*self = Self::from(v.clone());
		}
	}
}

impl From<SerializedVector> for Vector {
	fn from(value: SerializedVector) -> Self {
		match value {
			SerializedVector::F64(v) => Self::F64(Array1::from_vec(v)),
			SerializedVector::F16(v) => {
				Self::F16(Array1::from_vec(v.into_iter().map(f16::from_bits).collect()))
			}
			SerializedVector::F32(v) => Self::F32(Array1::from_vec(v)),
			SerializedVector::I64(v) => Self::I64(Array1::from_vec(v)),
			SerializedVector::I32(v) => Self::I32(Array1::from_vec(v)),
			SerializedVector::I16(v) => Self::I16(Array1::from_vec(v)),
			SerializedVector::I8(v) => Self::I8(Array1::from_vec(v)),
			SerializedVector::U8(v) => Self::U8(Array1::from_vec(v)),
		}
	}
}

/// Builds the persisted vector payload for `t` from a query value, checking that
/// every element is a number in `t`'s range. `d` is a capacity hint only; the
/// caller checks the resulting dimension.
///
/// A free function rather than a constructor on [`SerializedVector`]: the stored
/// form is declared below this layer and cannot name the index error type this
/// raises.
pub(super) fn serialized_vector_from_value(
	t: VectorType,
	d: usize,
	v: Value,
) -> Result<SerializedVector> {
	let res = match t {
		VectorType::F64 => {
			let mut vec = Vec::with_capacity(d);
			check_vector_value(v, &mut vec)?;
			SerializedVector::F64(vec)
		}
		VectorType::F16 => {
			let mut vec = Vec::with_capacity(d);
			check_vector_value_f16(v, &mut vec)?;
			SerializedVector::F16(vec)
		}
		VectorType::F32 => {
			let mut vec = Vec::with_capacity(d);
			check_vector_value(v, &mut vec)?;
			SerializedVector::F32(vec)
		}
		VectorType::I64 => {
			let mut vec = Vec::with_capacity(d);
			check_vector_value(v, &mut vec)?;
			SerializedVector::I64(vec)
		}
		VectorType::I32 => {
			let mut vec = Vec::with_capacity(d);
			check_vector_value(v, &mut vec)?;
			SerializedVector::I32(vec)
		}
		VectorType::I16 => {
			let mut vec = Vec::with_capacity(d);
			check_vector_value(v, &mut vec)?;
			SerializedVector::I16(vec)
		}
		VectorType::I8 => {
			let mut vec = Vec::with_capacity(d);
			check_vector_value(v, &mut vec)?;
			SerializedVector::I8(vec)
		}
		VectorType::U8 => {
			let mut vec = Vec::with_capacity(d);
			check_vector_value(v, &mut vec)?;
			SerializedVector::U8(vec)
		}
	};
	Ok(res)
}

fn check_vector_value_f16(value: Value, vec: &mut Vec<u16>) -> Result<()> {
	match value {
		Value::Array(a) => {
			for v in a.0 {
				check_vector_value_f16(v, vec)?;
			}
			Ok(())
		}
		Value::Number(n) => {
			let n: f32 = n.try_into()?;
			vec.push(f16::from_f32(n).to_bits());
			Ok(())
		}
		_ => Err(anyhow::Error::new(IdxError::InvalidVectorValue(value.to_raw_string()))),
	}
}

fn check_vector_value<T>(value: Value, vec: &mut Vec<T>) -> Result<()>
where
	T: TryFrom<Number, Error = Error>,
{
	match value {
		Value::Array(a) => {
			for v in a.0 {
				check_vector_value(v, vec)?;
			}
			Ok(())
		}
		Value::Number(n) => {
			vec.push(n.try_into()?);
			Ok(())
		}
		_ => Err(anyhow::Error::new(IdxError::InvalidVectorValue(value.to_raw_string()))),
	}
}

impl Vector {
	#[inline]
	fn dot_product<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		a.iter().zip(b.iter()).map(|(a, b)| a.to_float() * b.to_float()).sum()
	}

	#[inline]
	fn magnitude<T>(a: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		a.iter().map(|v| v.to_float().powi(2)).sum::<f64>().sqrt()
	}

	/// The differences of two I64 vectors' components, each taken exactly
	/// (in `i128`) before it becomes an `f64`: converting the components first
	/// would round integers above 2^53 that differ to the same value.
	fn i64_diffs<'a>(a: &'a Array1<i64>, b: &'a Array1<i64>) -> impl Iterator<Item = f64> + 'a {
		a.iter().zip(b.iter()).map(|(a, b)| (i128::from(*a) - i128::from(*b)) as f64)
	}

	#[inline]
	fn chebyshev<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		a.iter()
			.zip(b.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).abs())
			.fold(0.0_f64, f64::max)
	}

	fn chebyshev_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => a.linf_dist(b).unwrap_or(f64::INFINITY),
			(Self::F16(a), Self::F16(b)) => Self::chebyshev(a, b),
			(Self::F32(a), Self::F32(b)) => match a.linf_dist(b) {
				Ok(r) if r.is_finite() => r as f64,
				// An `f32` difference overflowed: measured in `f64` instead.
				Ok(_) => Self::chebyshev(a, b),
				Err(_) => f64::INFINITY,
			},
			// Integer components are compared as `f64`: a difference taken in
			// the element type overflows it.
			(Self::I64(a), Self::I64(b)) => {
				Self::i64_diffs(a, b).map(f64::abs).fold(0.0_f64, f64::max)
			}
			(Self::I32(a), Self::I32(b)) => Self::chebyshev(a, b),
			(Self::I16(a), Self::I16(b)) => Self::chebyshev(a, b),
			(Self::I8(a), Self::I8(b)) => Self::chebyshev(a, b),
			(Self::U8(a), Self::U8(b)) => Self::chebyshev(a, b),
			_ => f64::INFINITY,
		}
	}

	/// Dot product of two vectors, returned as `f64`.
	///
	/// The per-type primitives mirror those used by [`Self::cosine_distance`]:
	/// the f64 arm uses ndarray's `dot`, the f32 arm the SIMD kernel
	/// ([`kernels::dot_f32`]), and the remaining types the generic scalar
	/// [`Self::dot_product`]. [`Self::l2_norm`] uses the same primitive per
	/// type, which is what makes a cached-norm cosine bit-for-bit equal to the
	/// direct computation. Returns `NaN` on an element-type mismatch, which
	/// cannot occur within a single index (both operands share its vector type).
	pub(super) fn dot(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => a.dot(b),
			(Self::F32(a), Self::F32(b)) => {
				let dot = match (a.as_slice(), b.as_slice()) {
					(Some(a), Some(b)) if a.len() == b.len() => kernels::dot_f32(a, b),
					_ => a.dot(b),
				};
				// Large finite components overflow an `f32` accumulator: their
				// products are summed in `f64` instead.
				if dot.is_finite() {
					dot as f64
				} else {
					Self::dot_product(a, b)
				}
			}
			(Self::F16(a), Self::F16(b)) => Self::dot_product(a, b),
			(Self::I64(a), Self::I64(b)) => Self::dot_product(a, b),
			(Self::I32(a), Self::I32(b)) => Self::dot_product(a, b),
			(Self::I16(a), Self::I16(b)) => Self::dot_product(a, b),
			(Self::I8(a), Self::I8(b)) => Self::dot_product(a, b),
			(Self::U8(a), Self::U8(b)) => Self::dot_product(a, b),
			_ => f64::NAN,
		}
	}

	/// Asks the CPU to start loading the vector's components into its caches
	/// ([`kernels::prefetch`]); a hint that changes nothing.
	pub(super) fn prefetch(&self) {
		// A non-contiguous array has no slice to hint, and is skipped.
		fn prefetch_array<T>(a: &Array1<T>) {
			if let Some(s) = a.as_slice() {
				kernels::prefetch(s);
			}
		}
		match self {
			Self::F64(a) => prefetch_array(a),
			Self::F16(a) => prefetch_array(a),
			Self::F32(a) => prefetch_array(a),
			Self::I64(a) => prefetch_array(a),
			Self::I32(a) => prefetch_array(a),
			Self::I16(a) => prefetch_array(a),
			Self::I8(a) => prefetch_array(a),
			Self::U8(a) => prefetch_array(a),
		}
	}

	/// L2 (Euclidean) norm of the vector, returned as `f64`.
	///
	/// Every caller, cached or not, goes through here, so a norm computed once
	/// and cached is bit-identical to one recomputed on every call. The f32/f64
	/// arms take the sum of squares as the vector's dot product with itself,
	/// which reads it in place rather than materialising the squared elements.
	pub(super) fn l2_norm(&self) -> f64 {
		match self {
			Self::F64(a) => a.dot(a).sqrt(),
			Self::F32(a) => {
				let squares = match a.as_slice() {
					Some(s) => kernels::dot_f32(s, s),
					None => a.dot(a),
				};
				// Large finite components overflow an `f32` accumulator: their
				// squares are summed in `f64` instead.
				if squares.is_finite() {
					(squares as f64).sqrt()
				} else {
					Self::magnitude(a)
				}
			}
			Self::F16(a) => Self::magnitude(a),
			Self::I64(a) => Self::magnitude(a),
			Self::I32(a) => Self::magnitude(a),
			Self::I16(a) => Self::magnitude(a),
			Self::I8(a) => Self::magnitude(a),
			Self::U8(a) => Self::magnitude(a),
		}
	}

	fn cosine_distance(&self, other: &Self) -> f64 {
		// A type mismatch cannot occur for a single index but is preserved as
		// `INFINITY` to keep behaviour identical to the previous match arms.
		if std::mem::discriminant(self) != std::mem::discriminant(other) {
			return f64::INFINITY;
		}
		1.0 - self.dot(other) / (self.l2_norm() * other.l2_norm())
	}

	/// Cosine distance using pre-computed L2 norms for both operands.
	///
	/// Identical arithmetic to [`Self::cosine_distance`], but the caller supplies
	/// the (typically cached) magnitudes instead of recomputing them, which
	/// removes two of the three sum-of-products passes per evaluation.
	fn cosine_distance_with_norms(&self, other: &Self, self_norm: f64, other_norm: f64) -> f64 {
		if std::mem::discriminant(self) != std::mem::discriminant(other) {
			return f64::INFINITY;
		}
		1.0 - self.dot(other) / (self_norm * other_norm)
	}

	/// `1 - a·b` for vectors normalised to unit length, through [`Self::dot`]
	/// and so the same kernels as cosine. Infinite on an element-type
	/// mismatch.
	fn cosine_normalized_distance(&self, other: &Self) -> f64 {
		if std::mem::discriminant(self) != std::mem::discriminant(other) {
			return f64::INFINITY;
		}
		1.0 - self.dot(other)
	}

	#[inline]
	fn euclidean<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		match (a.as_slice(), b.as_slice()) {
			(Some(a), Some(b)) => Self::sum_of_squared_differences(a, b).sqrt(),
			_ => Zip::from(a)
				.and(b)
				.map_collect(|x, y| (x.to_float() - y.to_float()).powi(2))
				.sum()
				.sqrt(),
		}
	}

	/// `Σ (aᵢ - bᵢ)²` over two contiguous slices of equal length, without
	/// materialising the squared differences.
	///
	/// Accumulates in eight independent lanes, combined in the order ndarray's
	/// `sum` combines its own, so the result is bit-identical to summing a
	/// collected array of the squared differences — and, unlike a single
	/// running total, the lanes carry no dependency on one another, which is
	/// what lets the loop vectorise.
	#[inline]
	fn sum_of_squared_differences<T>(a: &[T], b: &[T]) -> f64
	where
		T: ToFloat,
	{
		let sq = |x: &T, y: &T| (x.to_float() - y.to_float()).powi(2);
		let mut p = [0.0f64; 8];
		let (chunks_a, chunks_b) = (a.chunks_exact(8), b.chunks_exact(8));
		let (rest_a, rest_b) = (chunks_a.remainder(), chunks_b.remainder());
		for (x, y) in chunks_a.zip(chunks_b) {
			for lane in 0..8 {
				p[lane] += sq(&x[lane], &y[lane]);
			}
		}
		let mut acc = 0.0 + (p[0] + p[4]);
		acc += p[1] + p[5];
		acc += p[2] + p[6];
		acc += p[3] + p[7];
		for (x, y) in rest_a.iter().zip(rest_b) {
			acc += sq(x, y);
		}
		acc
	}
	fn euclidean_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => a.l2_dist(b).unwrap_or(f64::INFINITY),
			(Self::F16(a), Self::F16(b)) => Self::euclidean(a, b),
			(Self::F32(a), Self::F32(b)) => {
				let distance = match (a.as_slice(), b.as_slice()) {
					(Some(s), Some(t)) if s.len() == t.len() => {
						(kernels::squared_l2_f32(s, t) as f64).sqrt()
					}
					_ => a.l2_dist(b).unwrap_or(f64::INFINITY),
				};
				// An `f32` accumulator overflows on large finite components:
				// those are measured in `f64` instead.
				if distance.is_infinite() && a.len() == b.len() {
					Self::euclidean(a, b)
				} else {
					distance
				}
			}
			// Integer components are compared as `f64`: a difference taken in
			// the element type, or its square, overflows it.
			(Self::I64(a), Self::I64(b)) => {
				Self::i64_diffs(a, b).map(|d| d * d).sum::<f64>().sqrt()
			}
			(Self::I32(a), Self::I32(b)) => Self::euclidean(a, b),
			(Self::I16(a), Self::I16(b)) => Self::euclidean(a, b),
			(Self::I8(a), Self::I8(b)) => Self::euclidean(a, b),
			(Self::U8(a), Self::U8(b)) => Self::euclidean(a, b),
			_ => f64::INFINITY,
		}
	}

	#[inline]
	fn hamming<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: PartialEq,
	{
		Zip::from(a).and(b).fold(0, |acc, a, b| {
			if a != b {
				acc + 1
			} else {
				acc
			}
		}) as f64
	}

	fn hamming_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => Self::hamming(a, b),
			(Self::F16(a), Self::F16(b)) => Self::hamming(a, b),
			(Self::F32(a), Self::F32(b)) => Self::hamming(a, b),
			(Self::I64(a), Self::I64(b)) => Self::hamming(a, b),
			(Self::I32(a), Self::I32(b)) => Self::hamming(a, b),
			(Self::I16(a), Self::I16(b)) => Self::hamming(a, b),
			(Self::I8(a), Self::I8(b)) => Self::hamming(a, b),
			(Self::U8(a), Self::U8(b)) => Self::hamming(a, b),
			_ => f64::INFINITY,
		}
	}

	/// The weighted Jaccard distance. Infinite on an element-type or
	/// dimension mismatch, as the other distances are, so such a pair is
	/// never ranked rather than ranked last as `NaN`.
	pub(super) fn jaccard_distance(&self, other: &Self) -> f64 {
		fn distance<T: ToFloat>(a: &Array1<T>, b: &Array1<T>) -> f64 {
			if a.len() != b.len() {
				return f64::INFINITY;
			}
			weighted_jaccard_distance(
				a.iter().zip(b.iter()).map(|(a, b)| (a.to_float(), b.to_float())),
			)
		}
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => distance(a, b),
			(Self::F16(a), Self::F16(b)) => distance(a, b),
			(Self::F32(a), Self::F32(b)) => distance(a, b),
			(Self::I64(a), Self::I64(b)) => distance(a, b),
			(Self::I32(a), Self::I32(b)) => distance(a, b),
			(Self::I16(a), Self::I16(b)) => distance(a, b),
			(Self::I8(a), Self::I8(b)) => distance(a, b),
			(Self::U8(a), Self::U8(b)) => distance(a, b),
			_ => f64::INFINITY,
		}
	}

	#[inline]
	fn manhattan_float<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		a.iter().zip(b.iter()).map(|(a, b)| (a.to_float() - b.to_float()).abs()).sum()
	}

	pub(super) fn manhattan_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => a.l1_dist(b).unwrap_or(f64::INFINITY),
			(Self::F16(a), Self::F16(b)) => Self::manhattan_float(a, b),
			(Self::F32(a), Self::F32(b)) => match a.l1_dist(b) {
				Ok(r) if r.is_finite() => r as f64,
				// An `f32` sum overflowed: measured in `f64` instead.
				Ok(_) => Self::manhattan_float(a, b),
				Err(_) => f64::INFINITY,
			},
			// Integer components are compared as `f64`: a difference taken in
			// the element type overflows it.
			(Self::I64(a), Self::I64(b)) => Self::i64_diffs(a, b).map(f64::abs).sum(),
			(Self::I32(a), Self::I32(b)) => Self::manhattan_float(a, b),
			(Self::I16(a), Self::I16(b)) => Self::manhattan_float(a, b),
			(Self::I8(a), Self::I8(b)) => Self::manhattan_float(a, b),
			(Self::U8(a), Self::U8(b)) => Self::manhattan_float(a, b),
			_ => f64::INFINITY,
		}
	}

	#[inline]
	fn minkowski<T>(a: &Array1<T>, b: &Array1<T>, order: f64) -> f64
	where
		T: ToFloat,
	{
		let dist: f64 = a
			.iter()
			.zip(b.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).abs().powf(order))
			.sum();
		dist.powf(1.0 / order)
	}

	pub(super) fn minkowski_distance(&self, other: &Self, order: f64) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => Self::minkowski(a, b, order),
			(Self::F16(a), Self::F16(b)) => Self::minkowski(a, b, order),
			(Self::F32(a), Self::F32(b)) => Self::minkowski(a, b, order),
			// I64 differences are taken exactly before converting, as for the
			// other integer-exact metrics.
			(Self::I64(a), Self::I64(b)) => {
				let dist: f64 = Self::i64_diffs(a, b).map(|d| d.abs().powf(order)).sum();
				dist.powf(1.0 / order)
			}
			(Self::I32(a), Self::I32(b)) => Self::minkowski(a, b, order),
			(Self::I16(a), Self::I16(b)) => Self::minkowski(a, b, order),
			(Self::I8(a), Self::I8(b)) => Self::minkowski(a, b, order),
			(Self::U8(a), Self::U8(b)) => Self::minkowski(a, b, order),
			_ => f64::INFINITY,
		}
	}

	#[inline]
	fn pearson<T>(x: &Array1<T>, y: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		let mean_x = x.iter().map(ToFloat::to_float).sum::<f64>() / x.len() as f64;
		let mean_y = y.iter().map(ToFloat::to_float).sum::<f64>() / y.len() as f64;

		let mut sum_xy = 0.0;
		let mut sum_x2 = 0.0;
		let mut sum_y2 = 0.0;

		for (xi, yi) in x.iter().zip(y.iter()) {
			let diff_x = xi.to_float() - mean_x;
			let diff_y = yi.to_float() - mean_y;
			sum_xy += diff_x * diff_y;
			sum_x2 += diff_x.powi(2);
			sum_y2 += diff_y.powi(2);
		}

		let numerator = sum_xy;
		let denominator = (sum_x2 * sum_y2).sqrt();

		if !(numerator.is_finite() && denominator.is_finite()) {
			// Finite components whose sums overflow, or non-finite ones.
			return pearson_rescaled(
				x.iter().zip(y.iter()).map(|(a, b)| (a.to_float(), b.to_float())),
			);
		}
		if denominator == 0.0 {
			return 0.0; // Return 0 if the denominator is 0
		}

		numerator / denominator
	}

	/// Correlation distance, `1 - r`: `0` for perfectly correlated vectors,
	/// `2` for perfectly anti-correlated ones. Where no correlation is defined
	/// (a vector of zero variance or with a non-finite component), the vectors
	/// are uncorrelated, at distance `1`, as the brute-force Pearson distance
	/// puts them.
	fn pearson_distance(&self, other: &Self) -> f64 {
		if !comparable(self, other) {
			return f64::INFINITY;
		}
		let r = self.pearson_similarity(other);
		1.0 - if r.is_finite() {
			r
		} else {
			0.0
		}
	}

	fn pearson_similarity(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => Self::pearson(a, b),
			(Self::F16(a), Self::F16(b)) => Self::pearson(a, b),
			(Self::F32(a), Self::F32(b)) => Self::pearson(a, b),
			(Self::I64(a), Self::I64(b)) => Self::pearson(a, b),
			(Self::I32(a), Self::I32(b)) => Self::pearson(a, b),
			(Self::I16(a), Self::I16(b)) => Self::pearson(a, b),
			(Self::I8(a), Self::I8(b)) => Self::pearson(a, b),
			(Self::U8(a), Self::U8(b)) => Self::pearson(a, b),
			_ => f64::NAN,
		}
	}

	/// `-a·b`, through [`Self::dot`] and so the same kernels as cosine.
	/// Infinite on an element-type mismatch.
	fn inner_product_distance(&self, other: &Self) -> f64 {
		if std::mem::discriminant(self) != std::mem::discriminant(other) {
			return f64::INFINITY;
		}
		-self.dot(other)
	}

	fn mem_size(&self) -> usize {
		let s = match self {
			Self::F64(arr) => arr.len() * std::mem::size_of::<f64>(),
			Self::F16(arr) => arr.len() * std::mem::size_of::<f16>(),
			Self::F32(arr) => arr.len() * std::mem::size_of::<f32>(),
			Self::I64(arr) => arr.len() * std::mem::size_of::<i64>(),
			Self::I32(arr) => arr.len() * std::mem::size_of::<i32>(),
			Self::I16(arr) => arr.len() * std::mem::size_of::<i16>(),
			Self::I8(arr) => arr.len() * std::mem::size_of::<i8>(),
			Self::U8(arr) => arr.len() * std::mem::size_of::<u8>(),
		};
		// Array1 overhead (approximately 24 bytes for ndarray metadata)
		s + 24
	}
}

/// A [`Vector`] together with derived quantities that are expensive to recompute
/// on every distance evaluation. A shared vector is immutable once created, so
/// these caches can never go stale.
#[derive(Debug)]
struct VectorEntry {
	/// The underlying vector data.
	vector: Vector,
	/// Cached L2 norm, populated on first use by [`SharedVector::norm`]. Only the
	/// cosine distance reads it, so non-cosine indexes never pay to compute it.
	norm: OnceLock<f64>,
}

/// For vectors, as we want to support very large vectors, we want to avoid copy
/// or clone. So the requirement is multiple ownership but not thread safety.
/// However, because we are running in an async context, and because we are
/// using cache structures that use the Arc as a key, the cached objects has to
/// be Sent, which then requires the use of Arc (rather than just Rc).
/// As computing the hash for a large vector is costly, this structures also
/// caches the hashcode to avoid recomputing it.
#[derive(Debug, Clone)]
pub struct SharedVector(Arc<VectorEntry>, u64);
impl From<Vector> for SharedVector {
	fn from(v: Vector) -> Self {
		let mut h = AHasher::default();
		v.hash(&mut h);
		let hash = h.finish();
		Self(
			Arc::new(VectorEntry {
				vector: v,
				norm: OnceLock::new(),
			}),
			hash,
		)
	}
}

impl Deref for SharedVector {
	type Target = Vector;

	fn deref(&self) -> &Self::Target {
		&self.0.vector
	}
}

impl Hash for SharedVector {
	fn hash<H: Hasher>(&self, state: &mut H) {
		state.write_u64(self.1);
	}
}

impl PartialEq for SharedVector {
	fn eq(&self, other: &Self) -> bool {
		// The cached norm is derived from the vector, so it is excluded here
		// (and `OnceLock` is not `PartialEq` regardless).
		self.1 == other.1 && self.0.vector == other.0.vector
	}
}
impl Eq for SharedVector {}

impl SharedVector {
	pub(super) fn mem_size(&self) -> usize {
		// SharedVector stack size + Vector heap size + cached-norm cell + Arc heap overhead
		std::mem::size_of::<Self>()
			+ self.0.vector.mem_size()
			+ std::mem::size_of::<OnceLock<f64>>()
			+ 16
	}

	/// Returns the vector's L2 norm, computing and caching it on first access.
	///
	/// Used by the cosine distance so a stored vector's magnitude is computed
	/// once and then reused across every comparison for as long as the vector
	/// stays resident in the index cache.
	pub(super) fn norm(&self) -> f64 {
		*self.0.norm.get_or_init(|| self.0.vector.l2_norm())
	}
}

impl Hash for Vector {
	fn hash<H: Hasher>(&self, state: &mut H) {
		match self {
			Vector::F64(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x.to_bits());
				state.write_u64(h);
			}
			Vector::F16(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x.to_bits());
				state.write_u16(h);
			}
			Vector::F32(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x.to_bits());
				state.write_u32(h);
			}
			Vector::I64(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_i64(h);
			}
			Vector::I32(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_i32(h);
			}
			Vector::I16(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_i16(h);
			}
			Vector::I8(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_i8(h);
			}
			Vector::U8(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_u8(h);
			}
		}
	}
}

#[cfg(test)]
impl SharedVector {
	pub(crate) fn clone_vector(&self) -> Vector {
		self.0.vector.clone()
	}
}

#[cfg(test)]
impl From<&Vector> for Value {
	fn from(v: &Vector) -> Self {
		let vec: Vec<_> = match v {
			Vector::F64(a) => a.iter().map(|i| Number::Float(*i)).map(Value::from).collect(),
			Vector::F16(a) => {
				a.iter().map(|i| Number::Float(i.to_f64())).map(Value::from).collect()
			}
			Vector::F32(a) => a.iter().map(|i| Number::Float(*i as f64)).map(Value::from).collect(),
			Vector::I64(a) => a.iter().map(|i| Number::Int(*i)).map(Value::from).collect(),
			Vector::I32(a) => a.iter().map(|i| Number::Int(*i as i64)).map(Value::from).collect(),
			Vector::I16(a) => a.iter().map(|i| Number::Int(*i as i64)).map(Value::from).collect(),
			Vector::I8(a) => a.iter().map(|i| Number::Int(*i as i64)).map(Value::from).collect(),
			Vector::U8(a) => a.iter().map(|i| Number::Int(*i as i64)).map(Value::from).collect(),
		};
		Value::from(vec)
	}
}

impl Vector {
	#[cfg(test)]
	pub(super) fn try_from_value(t: VectorType, d: usize, v: Value) -> Result<Self> {
		let res = match t {
			VectorType::F64 => {
				let mut vec = Vec::with_capacity(d);
				check_vector_value(v, &mut vec)?;
				Vector::F64(Array1::from_vec(vec))
			}
			VectorType::F16 => {
				let mut vec = Vec::with_capacity(d);
				check_vector_value_f16(v, &mut vec)?;
				Vector::F16(Array1::from_vec(vec.into_iter().map(f16::from_bits).collect()))
			}
			VectorType::F32 => {
				let mut vec = Vec::with_capacity(d);
				check_vector_value(v, &mut vec)?;
				Vector::F32(Array1::from_vec(vec))
			}
			VectorType::I64 => {
				let mut vec = Vec::with_capacity(d);
				check_vector_value(v, &mut vec)?;
				Vector::I64(Array1::from_vec(vec))
			}
			VectorType::I32 => {
				let mut vec = Vec::with_capacity(d);
				check_vector_value(v, &mut vec)?;
				Vector::I32(Array1::from_vec(vec))
			}
			VectorType::I16 => {
				let mut vec = Vec::with_capacity(d);
				check_vector_value(v, &mut vec)?;
				Vector::I16(Array1::from_vec(vec))
			}
			VectorType::I8 => {
				let mut vec = Vec::with_capacity(d);
				check_vector_value(v, &mut vec)?;
				Vector::I8(Array1::from_vec(vec))
			}
			VectorType::U8 => {
				let mut vec = Vec::with_capacity(d);
				check_vector_value(v, &mut vec)?;
				Vector::U8(Array1::from_vec(vec))
			}
		};
		Ok(res)
	}

	pub(crate) fn try_from_vector(t: VectorType, v: &[Number]) -> Result<Self> {
		Self::try_from_numbers(t, v.iter().copied())
	}

	/// Converts a sequence of numbers into a vector of type `t`.
	pub(crate) fn try_from_numbers<I>(t: VectorType, v: I) -> Result<Self>
	where
		I: Iterator<Item = Number>,
	{
		/// Converts each number, into one allocation sized to the iterator's
		/// upper bound. Collecting into a `Result` would report a lower bound
		/// of zero and grow the vector geometrically, copying it each time.
		fn convert<T, I>(v: I, f: impl Fn(Number) -> Result<T, Error>) -> Result<Array1<T>>
		where
			I: Iterator<Item = Number>,
		{
			let (lower, upper) = v.size_hint();
			let mut out = Vec::with_capacity(upper.unwrap_or(lower));
			for n in v {
				out.push(f(n)?);
			}
			Ok(Array1::from_vec(out))
		}
		fn collect<T, I>(v: I) -> Result<Array1<T>>
		where
			T: TryFrom<Number, Error = Error>,
			I: Iterator<Item = Number>,
		{
			convert(v, T::try_from)
		}
		let res = match t {
			VectorType::F64 => Vector::F64(collect(v)?),
			VectorType::F16 => Vector::F16(convert(v, |n| f32::try_from(n).map(f16::from_f32))?),
			VectorType::F32 => Vector::F32(collect(v)?),
			VectorType::I64 => Vector::I64(collect(v)?),
			VectorType::I32 => Vector::I32(collect(v)?),
			VectorType::I16 => Vector::I16(collect(v)?),
			VectorType::I8 => Vector::I8(collect(v)?),
			VectorType::U8 => Vector::U8(collect(v)?),
		};
		Ok(res)
	}

	pub(crate) fn len(&self) -> usize {
		match self {
			Self::F64(v) => v.len(),
			Self::F16(v) => v.len(),
			Self::F32(v) => v.len(),
			Self::I64(v) => v.len(),
			Self::I32(v) => v.len(),
			Self::I16(v) => v.len(),
			Self::I8(v) => v.len(),
			Self::U8(v) => v.len(),
		}
	}

	pub(crate) fn check_expected_dimension(current: usize, expected: usize) -> Result<()> {
		ensure!(
			current == expected,
			IdxError::InvalidVectorDimension {
				current,
				expected,
			}
		);
		Ok(())
	}

	pub(crate) fn check_dimension(&self, expected_dim: usize) -> Result<()> {
		Self::check_expected_dimension(self.len(), expected_dim)
	}
}

/// Engine-side distance evaluation over shared vectors.
pub(crate) trait DistanceExt {
	/// Distance between two vectors, each norm computed afresh. The search
	/// paths use the two below; this is the reference they are checked against.
	#[cfg(test)]
	fn calculate(&self, a: &Vector, b: &Vector) -> f64;
	fn calculate_shared(&self, a: &SharedVector, b: &SharedVector) -> f64;
	fn calculate_against(&self, query: &SharedVector, v: &Vector) -> f64;
}

/// Whether `a` and `b` can be measured against each other: the same element
/// type and the same dimension. Within one index they always can; any other
/// pair is infinitely far apart under every distance, rather than ranked as
/// `NaN` or panicking inside a kernel that requires equal lengths.
#[inline]
fn comparable(a: &Vector, b: &Vector) -> bool {
	std::mem::discriminant(a) == std::mem::discriminant(b) && a.len() == b.len()
}

/// The distance between two vectors [`comparable`] has admitted, so every
/// entry point checks the pair once.
#[inline]
fn calculate_comparable(d: &Distance, a: &Vector, b: &Vector) -> f64 {
	match d {
		Distance::Chebyshev => a.chebyshev_distance(b),
		Distance::Cosine => a.cosine_distance(b),
		Distance::CosineNormalized => a.cosine_normalized_distance(b),
		Distance::Euclidean => a.euclidean_distance(b),
		Distance::Hamming => a.hamming_distance(b),
		Distance::InnerProduct => a.inner_product_distance(b),
		Distance::Jaccard => a.jaccard_distance(b),
		Distance::Manhattan => a.manhattan_distance(b),
		Distance::Minkowski(order) => a.minkowski_distance(b, order.to_float()),
		Distance::Pearson => a.pearson_distance(b),
	}
}

impl DistanceExt for Distance {
	#[cfg(test)]
	fn calculate(&self, a: &Vector, b: &Vector) -> f64 {
		if !comparable(a, b) {
			return f64::INFINITY;
		}
		calculate_comparable(self, a, b)
	}

	/// Distance between two shared vectors, reusing their cached L2 norms for the
	/// metrics that need them (currently only [`Distance::Cosine`]). Every other
	/// metric is computed as for any pair and is bit-for-bit unchanged.
	///
	/// This is the entry point for the HNSW hot path: because both the stored and
	/// query vectors are [`SharedVector`]s, each magnitude is computed at most
	/// once and then reused across the whole traversal, turning cosine from three
	/// sum-of-products passes into one.
	fn calculate_shared(&self, a: &SharedVector, b: &SharedVector) -> f64 {
		if !comparable(a, b) {
			return f64::INFINITY;
		}
		match self {
			Distance::Cosine => a.cosine_distance_with_norms(b, a.norm(), b.norm()),
			_ => calculate_comparable(self, a, b),
		}
	}

	/// Distance from a shared query vector to a vector scored once, reusing the
	/// query's cached L2 norm for [`Distance::Cosine`] so each evaluation takes
	/// only the candidate's norm and the dot product. Bit-for-bit equal to the
	/// distance of the same pair with both norms computed afresh.
	///
	/// This is the entry point for scoring a stream of candidates that are not
	/// resident index vectors — pending updates, record values — against one
	/// query.
	fn calculate_against(&self, query: &SharedVector, v: &Vector) -> f64 {
		if !comparable(query, v) {
			return f64::INFINITY;
		}
		match self {
			Distance::Cosine => query.cosine_distance_with_norms(v, query.norm(), v.l2_norm()),
			_ => calculate_comparable(self, query, v),
		}
	}
}

/// Weighted (Ruzicka) Jaccard distance over pairs of components: the
/// complement of `vector::similarity::jaccard`, `1 - Σ min(aᵢ, bᵢ) / Σ max(aᵢ, bᵢ)`,
/// which lies in `[0, 1]`. See [`weighted_jaccard_similarity`] for the
/// treatment of negative components and of vectors with no positive component.
pub(crate) fn weighted_jaccard_distance(pairs: impl Iterator<Item = (f64, f64)> + Clone) -> f64 {
	1.0 - weighted_jaccard_similarity(pairs)
}

#[cfg(test)]
mod tests {
	use ndarray::{Array1, Zip};
	use surrealdb_runtime::util::math::ToFloat;

	use super::{DistanceExt as _, serialized_vector_from_value};
	use crate::catalog::{Distance, VectorType};
	use crate::kernels::vector::{distance_compute, score as score_raw_values};
	use crate::trees::knn::tests::{RandomItemGenerator, get_seed_rnd, new_random_vec};
	use crate::trees::vector::{SerializedVector, SharedVector, Vector};
	use crate::val::{Array, Number, Value};

	fn value_array(values: Vec<Value>) -> Value {
		Value::Array(Array(values))
	}

	fn test_distance(dist: &Distance, a1: &[f64], a2: &[f64], res: f64) {
		// Convert the arrays to Vec<Number>
		let mut v1 = vec![];
		a1.iter().for_each(|&n| v1.push(n.into()));
		let mut v2 = vec![];
		a2.iter().for_each(|&n| v2.push(n.into()));

		// Check the generic distance implementation
		assert_eq!(distance_compute(dist, &v1, &v2).unwrap(), res.into());

		// Check the "Vector" optimised implementations
		let t = VectorType::F64;
		let v1: SharedVector = Vector::try_from_vector(t, &v1).unwrap().into();
		let v2: SharedVector = Vector::try_from_vector(t, &v2).unwrap().into();
		assert_eq!(dist.calculate(&v1, &v2), res);
		// The cached-norm paths must return a bit-identical result.
		assert_eq!(dist.calculate_shared(&v1, &v2), res);
		assert_eq!(dist.calculate_against(&v1, &v2), res);
	}

	fn test_distance_collection(dist: &Distance, size: usize, dim: usize) {
		let mut rng = get_seed_rnd();
		for vt in [
			VectorType::F64,
			VectorType::F32,
			VectorType::I64,
			VectorType::I32,
			VectorType::I16,
			VectorType::F16,
			VectorType::I8,
			VectorType::U8,
		] {
			let r#gen = RandomItemGenerator::new(dist, dim);
			let mut num_zero = 0;
			for i in 0..size {
				let v1 = new_random_vec(&mut rng, vt, dim, &r#gen);
				let v2 = new_random_vec(&mut rng, vt, dim, &r#gen);
				let d = dist.calculate(&v1, &v2);
				// The cached-norm path must be bit-identical to the direct one
				// for every metric and vector type.
				assert_eq!(
					d,
					dist.calculate_shared(&v1, &v2),
					"cached-norm mismatch - vt: {vt} - v1: {v1:?} - v2: {v2:?}"
				);
				assert_eq!(
					d,
					dist.calculate_against(&v1, &v2),
					"query-norm mismatch - vt: {vt} - v1: {v1:?} - v2: {v2:?}"
				);
				assert!(
					d.is_finite() && !d.is_nan(),
					"i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}"
				);
				assert_ne!(d, f64::NAN, "i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}");
				assert_ne!(d, f64::INFINITY, "i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}");
				if d == 0.0 {
					num_zero += 1;
				}
			}
			let zero_rate = num_zero as f64 / size as f64;
			assert!(zero_rate < 0.1, "vt: {vt} - zero_rate: {zero_rate}");
		}
	}

	#[test]
	fn test_distance_chebyshev() {
		test_distance_collection(&Distance::Chebyshev, 100, 1536);
		test_distance(&Distance::Chebyshev, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 1.0);
	}

	#[test]
	fn test_distance_cosine() {
		test_distance_collection(&Distance::Cosine, 100, 1536);
		test_distance(&Distance::Cosine, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 0.007416666029069652);
	}

	/// The norm and the euclidean distance, computed without materialising the
	/// squared terms, are bit-identical to summing a collected array of those
	/// terms, for lengths that leave every possible remainder after the
	/// eight-lane unrolling. The components are fractional and of mixed
	/// magnitude, so a different summation order would show in the low bits.
	/// F32, scored by the SIMD kernel, is held to f32 precision instead.
	#[test]
	fn norm_and_euclidean_match_a_collected_sum_bit_for_bit() {
		fn collected<T: ToFloat>(x: &Array1<T>, y: &Array1<T>) -> f64 {
			Zip::from(x)
				.and(y)
				.map_collect(|x, y| (x.to_float() - y.to_float()).powi(2))
				.sum()
				.sqrt()
		}
		let component = |seed: f64, i: usize| {
			let i = i as f64;
			(seed * i).sin() * 10f64.powi((i as i32 % 7) - 3) + (seed + i).cos() / 3.0
		};
		for dim in [1, 7, 8, 9, 15, 16, 767, 768, 1536] {
			for seed in [0.37, 1.1, 2.9] {
				let x: Vec<f64> = (0..dim).map(|i| component(seed, i)).collect();
				let y: Vec<f64> = (0..dim).map(|i| component(seed * 1.7, i)).collect();
				let x64 = Array1::from_vec(x.clone());
				let x32: Array1<f32> = x.iter().map(|v| *v as f32).collect();
				assert_eq!(
					Vector::F64(x64.clone()).l2_norm(),
					(&x64 * &x64).sum().sqrt(),
					"f64 {dim}"
				);
				// F32 goes through the SIMD kernel, which sums in its own order:
				// it agrees with an f64 reference to f32 precision.
				let reference = x32.iter().map(|v| (*v as f64).powi(2)).sum::<f64>().sqrt();
				let norm = Vector::F32(x32.clone()).l2_norm();
				assert!((norm - reference).abs() <= 1e-6 * reference, "f32 {dim}");
				let h =
					|v: &[f64]| v.iter().map(|v| half::f16::from_f64(*v)).collect::<Array1<_>>();
				let (hx, hy) = (h(&x), h(&y));
				let d = Distance::Euclidean
					.calculate(&Vector::F16(hx.clone()), &Vector::F16(hy.clone()));
				assert_eq!(d, collected(&hx, &hy), "f16 {dim}");
				let q = |v: &[f64], scale: f64| {
					v.iter().map(|v| (v * scale).clamp(-127.0, 127.0) as i8).collect::<Array1<_>>()
				};
				let (qx, qy) = (q(&x, 40.0), q(&y, 40.0));
				let d =
					Distance::Euclidean.calculate(&Vector::I8(qx.clone()), &Vector::I8(qy.clone()));
				assert_eq!(d, collected(&qx, &qy), "i8 {dim}");
			}
		}
	}

	#[test]
	fn test_shared_vector_norm_is_cached_and_correct() {
		// 3-4-5 right triangle: the L2 norm of [3, 4] is exactly 5.
		let nums: Vec<Number> = vec![3.0.into(), 4.0.into()];
		let v: SharedVector = Vector::try_from_vector(VectorType::F64, &nums).unwrap().into();
		// The lazily-cached norm matches the direct computation...
		assert_eq!(v.l2_norm(), 5.0);
		assert_eq!(v.norm(), 5.0);
		// ...and stays stable when served from the cache on subsequent calls.
		assert_eq!(v.norm(), 5.0);
	}

	#[test]
	fn test_distance_cosine_normalized() {
		test_distance_collection(&Distance::CosineNormalized, 100, 1536);
		test_distance(&Distance::CosineNormalized, &[1.0, 0.0, 0.0], &[0.5, 0.5, 0.0], 0.5);
	}

	#[test]
	fn test_distance_euclidean() {
		test_distance_collection(&Distance::Euclidean, 100, 1536);
		test_distance(&Distance::Euclidean, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 1.7320508075688772);
	}

	#[test]
	fn test_distance_hamming() {
		test_distance_collection(&Distance::Hamming, 100, 1536);
		test_distance(&Distance::Hamming, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 3.0);
	}

	#[test]
	fn test_distance_inner_product() {
		test_distance_collection(&Distance::InnerProduct, 100, 1536);
		test_distance(&Distance::InnerProduct, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], -20.0);
	}

	#[test]
	fn test_distance_jaccard() {
		test_distance_collection(&Distance::Jaccard, 100, 768);
		// Σ min = 1 + 2 + 3 = 6, Σ max = 2 + 3 + 4 = 9.
		test_distance(&Distance::Jaccard, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 1.0 - 6.0 / 9.0);
	}
	#[test]
	fn test_distance_manhattan() {
		test_distance_collection(&Distance::Manhattan, 100, 1536);
		test_distance(&Distance::Manhattan, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 3.0);
	}
	#[test]
	fn test_distance_minkowski() {
		test_distance_collection(&Distance::Minkowski(3.into()), 100, 1536);
		test_distance(
			&Distance::Minkowski(3.into()),
			&[1.0, 2.0, 3.0],
			&[2.0, 3.0, 4.0],
			1.4422495703074083,
		);
	}

	/// A record's raw vector is scored whether it is flat or nested, and
	/// refused when it holds the wrong number of numbers or anything else.
	#[test]
	fn score_raw_values_reads_flat_and_nested_vectors() {
		let n = |v: f64| Value::Number(Number::Float(v));
		let a = |v: Vec<Value>| Value::Array(Array(v));
		let query: SharedVector = Vector::F32(Array1::from_vec(vec![0.0; 4])).into();
		let score = |raw: Vec<Value>| {
			score_raw_values(&Distance::Euclidean, VectorType::F32, 4, &query, &raw)
		};
		assert_eq!(score(vec![n(0.0), n(0.0), n(0.0), n(1.0)]), Some(1.0));
		assert_eq!(score(vec![a(vec![n(0.0), n(0.0)]), a(vec![n(0.0), n(1.0)])]), Some(1.0));
		assert_eq!(score(vec![n(0.0), a(vec![n(0.0), n(0.0)]), n(1.0)]), Some(1.0));
		// Too few, too many (stopped at the limit), and not numbers.
		assert_eq!(score(vec![n(0.0), n(0.0), n(1.0)]), None);
		assert_eq!(score(vec![a((0..100_000).map(|_| n(1.0)).collect())]), None);
		assert_eq!(score(vec![n(0.0), n(0.0), n(0.0), Value::None]), None);
		assert_eq!(score(vec![a(vec![n(0.0), Value::None]), n(0.0), n(1.0)]), None);
		// A component the vector type cannot hold fails flat or nested alike.
		let i = |v: i64| Value::Number(Number::Int(v));
		let query: SharedVector = Vector::I8(Array1::from_vec(vec![0; 2])).into();
		let score = |raw: Vec<Value>| {
			score_raw_values(&Distance::Euclidean, VectorType::I8, 2, &query, &raw)
		};
		assert_eq!(score(vec![i(0), i(300)]), None);
		assert_eq!(score(vec![a(vec![i(0)]), a(vec![i(300)])]), None);
		assert_eq!(score(vec![a(vec![i(0)]), a(vec![i(3)])]), Some(3.0));
	}

	/// I64 components are subtracted exactly before the difference becomes an
	/// `f64`, by the indexed and the brute-force paths alike: integers above
	/// 2^53 that differ by one stay one apart, rather than both rounding to
	/// the same `f64`, and the extremes of `i64` do not overflow.
	#[test]
	fn i64_differences_are_exact_above_two_to_the_53() {
		let big = 1i64 << 53;
		let (a, b) = (vec![Number::Int(big)], vec![Number::Int(big + 1)]);
		let (va, vb) = (
			Vector::try_from_vector(VectorType::I64, &a).unwrap(),
			Vector::try_from_vector(VectorType::I64, &b).unwrap(),
		);
		for d in [
			Distance::Euclidean,
			Distance::Manhattan,
			Distance::Chebyshev,
			Distance::Minkowski(Number::Int(1)),
			Distance::Minkowski(Number::Int(2)),
		] {
			assert_eq!(d.calculate(&va, &vb), 1.0, "{d:?}");
			assert_eq!(distance_compute(&d, &a, &b).unwrap().to_float(), 1.0, "{d:?}");
		}
		let (min, max) = (vec![Number::Int(i64::MIN)], vec![Number::Int(i64::MAX)]);
		let span = u64::MAX as f64;
		for d in [
			Distance::Euclidean,
			Distance::Manhattan,
			Distance::Chebyshev,
			Distance::Minkowski(Number::Int(1)),
		] {
			assert_eq!(distance_compute(&d, &min, &max).unwrap().to_float(), span, "{d:?}");
		}
		// Integer Manhattan sums stay integers while they fit, and widen
		// rather than overflow once they do not.
		let (a, b) = (vec![Number::Int(1), Number::Int(2)], vec![Number::Int(4), Number::Int(-2)]);
		assert_eq!(distance_compute(&Distance::Manhattan, &a, &b).unwrap(), Number::Int(7));
		let (a, b) = (vec![Number::Int(i64::MAX); 2], vec![Number::Int(0); 2]);
		assert_eq!(
			distance_compute(&Distance::Manhattan, &a, &b).unwrap(),
			Number::Float(2.0 * i64::MAX as f64)
		);
	}

	/// An F32 inner product too large for an `f32` accumulator is computed in
	/// `f64`, so finite vectors keep finite, ordered distances.
	#[test]
	fn f32_inner_product_does_not_overflow_its_accumulator() {
		let v = |x: f32| Vector::F32(Array1::from_vec(vec![x]));
		let (q, near, far) = (v(1e20), v(1e20), v(2e20));
		let (d_near, d_far) = (
			Distance::InnerProduct.calculate(&q, &near),
			Distance::InnerProduct.calculate(&q, &far),
		);
		assert!(d_near.is_finite() && d_far.is_finite(), "{d_near} {d_far}");
		assert!(d_far < d_near, "a larger inner product ranks nearer: {d_near} {d_far}");
	}

	/// F32 norms and distances too large for an `f32` accumulator are
	/// computed in `f64`: identical and parallel vectors stay at cosine
	/// distance zero, and Euclidean, Manhattan and Chebyshev stay finite.
	#[test]
	fn f32_norms_and_distances_do_not_overflow_their_accumulator() {
		let v = |x: Vec<f32>| Vector::F32(Array1::from_vec(x));
		let (q, same, double) = (v(vec![1e20]), v(vec![1e20]), v(vec![2e20]));
		assert_eq!(q.l2_norm(), 1e20_f32 as f64);
		assert_eq!(Distance::Cosine.calculate(&q, &same), 0.0);
		assert_eq!(Distance::Cosine.calculate(&q, &double), 0.0);
		let (far, wide) = (v(vec![3e38, 0.0]), v(vec![-3e38, 0.0]));
		for d in [Distance::Euclidean, Distance::Manhattan, Distance::Chebyshev] {
			let dist = d.calculate(&far, &wide);
			assert!(dist.is_finite(), "{d:?}: {dist}");
			assert_eq!(dist, 2.0 * (3e38_f32 as f64), "{d:?}");
		}
	}

	/// Weighted Jaccard keeps finite components' similarity when their sums
	/// overflow: identical vectors stay at distance zero.
	#[test]
	fn weighted_jaccard_survives_overflowing_sums() {
		let v = |x: Vec<f64>| Vector::F64(Array1::from_vec(x));
		let same = v(vec![1e308, 1e308]);
		assert_eq!(Distance::Jaccard.calculate(&same, &same), 0.0);
		let other = v(vec![1e308, 0.0]);
		assert_eq!(Distance::Jaccard.calculate(&same, &other), 0.5);
		// Infinite components dominate every finite one: the similarity is the
		// share of infinite maxima that are infinite minima too.
		let inf = f64::INFINITY;
		assert_eq!(Distance::Jaccard.calculate(&v(vec![inf]), &v(vec![inf])), 0.0);
		assert_eq!(Distance::Jaccard.calculate(&v(vec![inf, 1.0]), &v(vec![inf, 5.0])), 0.0);
		assert_eq!(Distance::Jaccard.calculate(&v(vec![inf]), &v(vec![5.0])), 1.0);
		assert_eq!(Distance::Jaccard.calculate(&v(vec![inf, inf]), &v(vec![inf, 1.0])), 0.5);
	}

	/// Integer vectors are measured in `f64`: components far apart, which
	/// overflow a difference or its square taken in the element type, are as
	/// far apart as `distance_compute` measures them.
	#[test]
	fn integer_distances_do_not_overflow_their_element_type() {
		let cases: [(VectorType, i64, i64); 6] = [
			(VectorType::I8, -120, 120),
			(VectorType::I16, -30_000, 30_000),
			(VectorType::I32, 0, 50_000),
			(VectorType::I32, -2_000_000_000, 2_000_000_000),
			(VectorType::I64, -9_000_000_000_000_000, 9_000_000_000_000_000),
			(VectorType::I64, i64::MIN / 2, i64::MAX / 2),
		];
		for (t, x, y) in cases {
			let (a, b) =
				(vec![Number::Int(x), Number::Int(0)], vec![Number::Int(y), Number::Int(0)]);
			let (va, vb) =
				(Vector::try_from_vector(t, &a).unwrap(), Vector::try_from_vector(t, &b).unwrap());
			for d in [Distance::Euclidean, Distance::Manhattan, Distance::Chebyshev] {
				let want = distance_compute(&d, &a, &b).unwrap().to_float();
				assert_eq!(d.calculate(&va, &vb), want, "{t:?} {d:?} ({x}, {y})");
			}
		}
	}

	/// Vectors of different element types or dimensions are infinitely far
	/// apart under every distance, so the result builder never ranks them as
	/// `NaN`.
	#[test]
	fn mismatched_vectors_are_infinitely_far() {
		let f32_3 = Vector::F32(Array1::from_vec(vec![1.0, 2.0, 3.0]));
		let f32_2 = Vector::F32(Array1::from_vec(vec![1.0, 2.0]));
		let f64_3 = Vector::F64(Array1::from_vec(vec![1.0, 2.0, 3.0]));
		let f64_2 = Vector::F64(Array1::from_vec(vec![1.0, 2.0]));
		let i16_3 = Vector::I16(Array1::from_vec(vec![1, 2, 3]));
		let i16_2 = Vector::I16(Array1::from_vec(vec![1, 2]));
		let distances = [
			Distance::Chebyshev,
			Distance::Cosine,
			Distance::CosineNormalized,
			Distance::Euclidean,
			Distance::Hamming,
			Distance::InnerProduct,
			Distance::Jaccard,
			Distance::Manhattan,
			Distance::Minkowski(3.into()),
			Distance::Pearson,
		];
		let pairs = [
			(&f32_3, &f32_2),
			(&f64_3, &f64_2),
			(&i16_3, &i16_2),
			(&f32_3, &f64_3),
			(&f64_3, &i16_3),
		];
		for d in &distances {
			for (a, b) in pairs {
				for (a, b) in [(a, b), (b, a)] {
					let (sa, sb): (SharedVector, SharedVector) =
						(a.clone().into(), b.clone().into());
					assert_eq!(d.calculate(a, b), f64::INFINITY, "{d:?} {a:?} {b:?}");
					assert_eq!(d.calculate_shared(&sa, &sb), f64::INFINITY, "{d:?} {a:?} {b:?}");
					assert_eq!(d.calculate_against(&sa, b), f64::INFINITY, "{d:?} {a:?} {b:?}");
				}
			}
		}
		assert_eq!(f32_3.jaccard_distance(&f32_2), f64::INFINITY);
	}

	/// `assign` leaves exactly what `Vector::from` builds, whether it copies
	/// in place (same type and length) or replaces the vector.
	#[test]
	fn assign_matches_a_fresh_vector() {
		let cases = [
			SerializedVector::F32(vec![1.5, -2.0, 3.25]),
			SerializedVector::F32(vec![4.0, 5.0, 6.0]),
			SerializedVector::F32(vec![7.0, 8.0]),
			SerializedVector::F64(vec![1.0, 2.0, 3.0]),
			SerializedVector::F16(vec![
				half::f16::from_f32(0.5).to_bits(),
				half::f16::from_f32(-1.5).to_bits(),
			]),
			SerializedVector::F16(vec![
				half::f16::from_f32(2.0).to_bits(),
				half::f16::from_f32(3.0).to_bits(),
			]),
			SerializedVector::F16(vec![half::f16::from_f32(4.0).to_bits()]),
			SerializedVector::I8(vec![-3, 0, 7]),
			SerializedVector::U8(vec![1, 2, 3]),
		];
		let mut scratch = Vector::from(cases[0].clone());
		for case in &cases {
			scratch.assign(case);
			assert_eq!(scratch, Vector::from(case.clone()), "{case:?}");
		}
	}

	#[test]
	fn test_distance_pearson() {
		test_distance_collection(&Distance::Pearson, 100, 1536);
		// Perfectly correlated: r = 1.
		test_distance(&Distance::Pearson, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 0.0);
		// A vector of zero variance correlates with nothing: distance 1, on the
		// brute-force path as on the index's.
		test_distance(&Distance::Pearson, &[1.0, 1.0, 1.0], &[1.0, 2.0, 3.0], 1.0);
		test_distance(&Distance::Pearson, &[2.0, 2.0, 2.0], &[5.0, 5.0, 5.0], 1.0);
		// Finite components whose sums overflow: the deviations' squares, or
		// the sum the mean is taken from. The correlation is the one of the
		// same vectors scaled down, on both paths.
		let big = [1e308, -1e308];
		let neg = [-1e308, 1e308];
		test_distance(&Distance::Pearson, &big, &big, 0.0);
		test_distance(&Distance::Pearson, &big, &neg, 2.0);
		let wide = [1e308, 1e308, -1e308, -1e308];
		let wide_neg = [-1e308, -1e308, 1e308, 1e308];
		test_distance(&Distance::Pearson, &wide, &wide, 0.0);
		test_distance(&Distance::Pearson, &wide, &wide_neg, 2.0);
		// Overflowing, but one side has zero variance once scaled: uncorrelated.
		test_distance(&Distance::Pearson, &[1e308, 1e308], &big, 1.0);
	}

	#[test]
	fn test_serialized_vector_f16_roundtrip() {
		let vector = serialized_vector_from_value(
			VectorType::F16,
			2,
			value_array(vec![
				Value::Number(Number::Float(1.5)),
				Value::Number(Number::Float(-2.25)),
			]),
		)
		.unwrap();
		let SerializedVector::F16(bits) = &vector else {
			panic!("expected F16 serialized vector");
		};
		assert_eq!(bits.len(), 2);
		let Vector::F16(roundtrip) = Vector::from(vector) else {
			panic!("expected F16 vector");
		};
		assert_eq!(roundtrip[0].to_f32(), 1.5);
		assert_eq!(roundtrip[1].to_f32(), -2.25);
	}

	#[test]
	fn test_serialized_vector_i8_u8_range_validation() {
		assert!(
			serialized_vector_from_value(
				VectorType::U8,
				1,
				value_array(vec![Value::Number(Number::Int(-1))])
			)
			.is_err()
		);
		assert!(
			serialized_vector_from_value(
				VectorType::I8,
				1,
				value_array(vec![Value::Number(Number::Int(128))])
			)
			.is_err()
		);
		assert!(
			serialized_vector_from_value(
				VectorType::U8,
				1,
				value_array(vec![Value::Number(Number::Int(255))])
			)
			.is_ok()
		);
		assert!(
			serialized_vector_from_value(
				VectorType::I8,
				1,
				value_array(vec![Value::Number(Number::Int(-128))])
			)
			.is_ok()
		);
	}
}
