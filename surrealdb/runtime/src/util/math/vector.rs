use anyhow::{Result, ensure};
use surrealdb_expr::expr::Error;
use surrealdb_expr::val::Number;

use crate::util::math::ToFloat;

pub fn deviation<T>(v: &[T], mean: f64, sample: bool) -> f64
where
	T: ToFloat,
{
	match v.len() {
		0 => f64::NAN,
		1 => 0.0,
		len => {
			let len = (len - sample as usize) as f64;
			(v.iter().map(|x| (x.to_float() - mean).powi(2)).sum::<f64>() / len).sqrt()
		}
	}
}

pub fn check_same_dimension<T>(fnc: &str, a: &[T], b: &[T]) -> Result<()> {
	ensure!(
		a.len() == b.len(),
		Error::InvalidFunctionArguments {
			name: String::from(fnc),
			message: String::from("The two vectors must be of the same dimension."),
		}
	);
	Ok(())
}

/// Add `other` into `acc` elementwise, in place.
///
/// `acc` must already hold the running total and must have the same dimension
/// as `other`; a mismatch is reported against `fnc`. Accumulating in place keeps
/// a running vector total allocation-free per input, so callers folding over
/// many vectors hold O(dimension) state rather than O(inputs × dimension).
pub fn add_assign(fnc: &str, acc: &mut [Number], other: &[Number]) -> Result<()> {
	check_same_dimension(fnc, acc, other)?;
	for (a, b) in acc.iter_mut().zip(other.iter()) {
		*a = *a + *b;
	}
	Ok(())
}

pub trait Add {
	/// Addition of two vectors
	fn add(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Add for Vec<Number> {
	fn add(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::add", self, other)?;
		Ok(self.iter().zip(other.iter()).map(|(a, b)| a + b).collect())
	}
}

pub trait Angle {
	/// Compute the angle between two vectors
	fn angle(&self, other: &Self) -> Result<Number>;
}

impl Angle for Vec<Number> {
	fn angle(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::angle", self, other)?;
		let m1 = self.magnitude();
		let m2 = other.magnitude();
		ensure!(
			!m1.is_zero() && !m2.is_zero(),
			Error::InvalidFunctionArguments {
				name: String::from("vector::angle"),
				message: String::from("The two vectors must not have a magnitude of zero."),
			}
		);
		let dp = dot(self, other);
		Ok((dp / (m1 * m2)).acos())
	}
}

pub trait CosineDistance {
	fn cosine_distance(&self, other: &Self) -> Result<Number>;
}

impl CosineDistance for [Number] {
	fn cosine_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::cosine", self, other)?;
		let d = dot(self, other);
		Ok(Number::from(1) - d / (self.magnitude() * other.magnitude()))
	}
}

pub trait CosineSimilarity {
	fn cosine_similarity(&self, other: &Self) -> Result<Number>;
}

impl CosineSimilarity for [Number] {
	fn cosine_similarity(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::similarity::cosine", self, other)?;
		let d = dot(self, other);
		Ok(d / (self.magnitude() * other.magnitude()))
	}
}

pub trait Divide {
	/// Division of two vectors
	fn divide(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Divide for Vec<Number> {
	fn divide(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::divide", self, other)?;
		ensure!(
			!other.iter().any(Number::is_zero),
			Error::InvalidFunctionArguments {
				name: String::from("vector::divide"),
				message: String::from("The second vector must not contain a value of zero."),
			}
		);
		Ok(self.iter().zip(other.iter()).map(|(a, b)| a / b).collect())
	}
}

pub trait HammingDistance {
	fn hamming_distance(&self, other: &Self) -> Result<Number>;
}

impl HammingDistance for [Number] {
	fn hamming_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::hamming", self, other)?;
		Ok(self.iter().zip(other.iter()).filter(|(a, b)| a != b).count().into())
	}
}

pub trait JaccardSimilarity {
	fn jaccard_similarity(&self, other: &Self) -> Result<Number>;
}

impl JaccardSimilarity for [Number] {
	fn jaccard_similarity(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::similarity::jaccard", self, other)?;
		Ok(weighted_jaccard_similarity(
			self.iter().zip(other.iter()).map(|(a, b)| (a.to_float(), b.to_float())),
		)
		.into())
	}
}

/// Weighted (Ruzicka) Jaccard similarity over pairs of components,
/// `Σ min(aᵢ, bᵢ) / Σ max(aᵢ, bᵢ)`: the Jaccard measure of vector search,
/// equal on 0/1 vectors to the binary `|a ∧ b| / |a ∨ b|`, and the
/// complement of the Jaccard distance a vector index ranks by.
///
/// Positional: component `i` of one vector is only compared with component
/// `i` of the other. The measure is defined for non-negative weights, so a
/// negative (or NaN) component counts as `0`, and two vectors with no positive
/// component are identical (similarity `1`). The result lies in `[0, 1]`.
///
/// Finite components whose sums overflow are summed again scaled by the
/// largest of them, which leaves the ratio unchanged. Infinite components
/// dominate every finite one, so the ratio is then their limit: the share of
/// the infinite maxima that are infinite minima too.
pub fn weighted_jaccard_similarity(pairs: impl Iterator<Item = (f64, f64)> + Clone) -> f64 {
	let (mut shared, mut total) = jaccard_sums(pairs.clone(), 1.0);
	if total.is_infinite() {
		let largest = pairs.clone().fold(0.0_f64, |m, (a, b)| m.max(a).max(b));
		(shared, total) = if largest.is_finite() {
			jaccard_sums(pairs, 1.0 / largest)
		} else {
			infinite_jaccard_counts(pairs)
		};
	}
	if total == 0.0 {
		1.0
	} else {
		shared / total
	}
}

/// `(Σ min(aᵢ, bᵢ), Σ max(aᵢ, bᵢ))` over the components scaled by `scale`,
/// a negative (or NaN) component counting as `0`.
fn jaccard_sums(pairs: impl Iterator<Item = (f64, f64)>, scale: f64) -> (f64, f64) {
	let (mut shared, mut total) = (0.0, 0.0);
	for (a, b) in pairs {
		let (a, b) = ((a * scale).max(0.0), (b * scale).max(0.0));
		shared += a.min(b);
		total += a.max(b);
	}
	(shared, total)
}

/// `(n, m)`: of the components whose maximum is infinite (`m`), those whose
/// minimum is infinite too (`n`), a negative (or NaN) component counting as
/// `0`.
fn infinite_jaccard_counts(pairs: impl Iterator<Item = (f64, f64)>) -> (f64, f64) {
	let (mut shared, mut total) = (0.0, 0.0);
	for (a, b) in pairs {
		let (a, b) = (a.max(0.0), b.max(0.0));
		if a.max(b).is_infinite() {
			total += 1.0;
			if a.min(b).is_infinite() {
				shared += 1.0;
			}
		}
	}
	(shared, total)
}

pub trait PearsonSimilarity {
	fn pearson_similarity(&self, other: &Self) -> Result<Number>;
}

impl PearsonSimilarity for [Number] {
	fn pearson_similarity(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::similarity::pearson", self, other)?;
		let m1 = crate::fuel::now(super::mean(self))?.to_float();
		let m2 = crate::fuel::now(super::mean(other))?.to_float();
		let covar: f64 = self
			.iter()
			.zip(other.iter())
			.map(|(x, y)| (x.to_float() - m1) * (y.to_float() - m2))
			.sum();
		let covar = covar / self.len() as f64;
		let std_dev1 = deviation(self, m1, false);
		let std_dev2 = deviation(other, m2, false);
		if !(m1.is_finite()
			&& m2.is_finite()
			&& covar.is_finite()
			&& (std_dev1 * std_dev2).is_finite())
		{
			return Ok(pearson_rescaled(
				self.iter().zip(other.iter()).map(|(a, b)| (a.to_float(), b.to_float())),
			)
			.into());
		}
		Ok((covar / (std_dev1 * std_dev2)).into())
	}
}

/// Pearson's correlation coefficient of paired components whose plain sums
/// overflow `f64`: the correlation is unchanged by scaling either side by a
/// positive factor, so each side is divided by its largest magnitude first,
/// which keeps every deviation within `[-2, 2]` and every sum finite.
///
/// `NaN` where no correlation is defined: an empty input, a side with zero
/// variance, or a non-finite component.
pub fn pearson_rescaled(pairs: impl Iterator<Item = (f64, f64)> + Clone) -> f64 {
	let (sx, sy) = pairs
		.clone()
		.fold((0.0_f64, 0.0_f64), |(sx, sy), (x, y)| (sx.max(x.abs()), sy.max(y.abs())));
	if !(sx.is_finite() && sy.is_finite() && sx > 0.0 && sy > 0.0) {
		return f64::NAN;
	}
	let (mut n, mut tx, mut ty) = (0_usize, 0.0, 0.0);
	for (x, y) in pairs.clone() {
		n += 1;
		tx += x / sx;
		ty += y / sy;
	}
	let (mx, my) = (tx / n as f64, ty / n as f64);
	let (mut sxy, mut sxx, mut syy) = (0.0, 0.0, 0.0);
	for (x, y) in pairs {
		let (dx, dy) = (x / sx - mx, y / sy - my);
		sxy += dx * dy;
		sxx += dx * dx;
		syy += dy * dy;
	}
	sxy / (sxx * syy).sqrt()
}

pub trait ManhattanDistance {
	fn manhattan_distance(&self, other: &Self) -> Result<Number>;
}

impl ManhattanDistance for [Number] {
	fn manhattan_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::manhattan", self, other)?;
		Ok(self.iter().zip(other.iter()).fold(Number::Int(0), |acc, (a, b)| {
			match (acc, int_abs_diff(a, b)) {
				// An integer sum stays an integer while it fits in an `i64`.
				(Number::Int(acc), Some(d)) => {
					match i64::try_from(d).ok().and_then(|d| acc.checked_add(d)) {
						Some(sum) => Number::Int(sum),
						None => Number::Float(acc as f64 + d as f64),
					}
				}
				(acc, Some(d)) => acc + Number::Float(d as f64),
				(acc, None) => acc + (a - b).abs(),
			}
		}))
	}
}

/// The exact absolute difference of two integers, which an `i64`
/// subtraction overflows and an `f64` subtraction rounds above 2^53; `None`
/// unless both are integers.
fn int_abs_diff(a: &Number, b: &Number) -> Option<u64> {
	match (a, b) {
		(Number::Int(a), Number::Int(b)) => Some(a.abs_diff(*b)),
		_ => None,
	}
}

pub trait MinkowskiDistance {
	fn minkowski_distance(&self, other: &Self, order: &Number) -> Result<Number>;
}

impl MinkowskiDistance for [Number] {
	fn minkowski_distance(&self, other: &Self, order: &Number) -> Result<Number> {
		check_same_dimension("vector::distance::minkowski", self, other)?;
		let p = order.to_float();
		let dist: f64 = self
			.iter()
			.zip(other.iter())
			.map(|(a, b)| {
				int_abs_diff(a, b)
					.map_or_else(|| (a.to_float() - b.to_float()).abs(), |d| d as f64)
					.powf(p)
			})
			.sum();
		Ok(dist.powf(1.0 / p).into())
	}
}

pub trait Multiply {
	/// Multiplication of two vectors
	fn multiply(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Multiply for Vec<Number> {
	fn multiply(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::multiply", self, other)?;
		Ok(self.iter().zip(other.iter()).map(|(a, b)| a * b).collect())
	}
}

pub trait Scale {
	fn scale(&self, other: &Number) -> Result<Vec<Number>>;
}

impl Scale for Vec<Number> {
	fn scale(&self, other: &Number) -> Result<Vec<Number>> {
		Ok(self.iter().map(|a| a * other).collect())
	}
}

pub trait Project {
	/// Projection of two vectors
	fn project(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Project for Vec<Number> {
	fn project(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::project", self, other)?;
		let m: Number = magnitude_squared(other).into();
		ensure!(
			!m.is_zero(),
			Error::InvalidFunctionArguments {
				name: String::from("vector::project"),
				message: String::from("The second vector must not have a magnitude of zero."),
			}
		);
		let d = dot(self, other);
		let s = d / m;
		Ok(other.iter().map(|x| &s * x).collect())
	}
}

pub trait ChebyshevDistance {
	fn chebyshev_distance(&self, other: &Self) -> Result<Number>;
}

impl ChebyshevDistance for [Number] {
	fn chebyshev_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::chebyshev", self, other)?;
		Ok(self
			.iter()
			.zip(other.iter())
			.map(|(a, b)| {
				int_abs_diff(a, b).map_or_else(|| (a.to_float() - b.to_float()).abs(), |d| d as f64)
			})
			.fold(0.0, f64::max)
			.into())
	}
}

pub trait Subtract {
	/// Subtraction of two vectors
	fn subtract(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Subtract for Vec<Number> {
	fn subtract(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::subtract", self, other)?;
		Ok(self.iter().zip(other.iter()).map(|(a, b)| a - b).collect())
	}
}

pub trait CrossProduct {
	/// Cross product of two vectors
	fn cross(&self, other: &Self) -> Result<Vec<Number>>;
}

impl CrossProduct for Vec<Number> {
	fn cross(&self, other: &Self) -> Result<Vec<Number>> {
		ensure!(
			self.len() == 3 && other.len() == 3,
			Error::InvalidFunctionArguments {
				name: "vector::cross".to_string(),
				message: String::from("Both vectors must have a dimension of 3."),
			}
		);
		let a0 = &self[0];
		let a1 = &self[1];
		let a2 = &self[2];
		let b0 = &other[0];
		let b1 = &other[1];
		let b2 = &other[2];
		let v = vec![a1 * b2 - a2 * b1, a2 * b0 - a0 * b2, a0 * b1 - a1 * b0];
		Ok(v)
	}
}

pub trait DotProduct {
	/// Dot Product of two vectors
	fn dot(&self, other: &Self) -> Result<Number>;
}

impl DotProduct for Vec<Number> {
	fn dot(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::dot", self, other)?;
		Ok(dot(self, other))
	}
}

fn dot(a: &[Number], b: &[Number]) -> Number {
	a.iter().zip(b.iter()).map(|(a, b)| a * b).sum()
}

pub trait EuclideanDistance {
	/// Euclidean Distance between two vectors (L2 Norm)
	fn euclidean_distance(&self, other: &Self) -> Result<Number>;
}

impl EuclideanDistance for [Number] {
	fn euclidean_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::euclidean", self, other)?;
		Ok(self
			.iter()
			.zip(other.iter())
			.map(|(a, b)| {
				int_abs_diff(a, b).map_or_else(|| (a - b).to_float(), |d| d as f64).powi(2)
			})
			.sum::<f64>()
			.sqrt()
			.into())
	}
}

fn magnitude_squared(v: &[Number]) -> f64 {
	v.iter().map(|a| a.to_float().powi(2)).sum::<f64>()
}

pub trait Magnitude {
	/// Calculate the magnitude of a vector
	fn magnitude(&self) -> Number;
}

impl Magnitude for [Number] {
	fn magnitude(&self) -> Number {
		magnitude_squared(self).sqrt().into()
	}
}

pub trait Normalize {
	/// Normalize a vector
	fn normalize(&self) -> Result<Vec<Number>>;
}

impl Normalize for Vec<Number> {
	fn normalize(&self) -> Result<Vec<Number>> {
		let m = self.magnitude();
		ensure!(
			!m.is_zero(),
			Error::InvalidFunctionArguments {
				name: String::from("vector::normalize"),
				message: String::from("The vector must not have a magnitude of zero."),
			}
		);
		Ok(self.iter().map(|a| a / &m).collect())
	}
}
