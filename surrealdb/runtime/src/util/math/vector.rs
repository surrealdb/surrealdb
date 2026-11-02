use anyhow::{Result, ensure};
use surrealdb_expr::expr::Error;
use surrealdb_expr::val::{Number, TryAdd, TryMul, TrySub, Value, Wide};
use surrealdb_types::BigDecimal;

use crate::util::math::ToFloat;

/// An element of a vector the distance and similarity measures read as a
/// [`Number`]: a number itself, or a value that holds one.
///
/// A [`Value`] that is not a number reads as `0`. A caller hands the
/// measures only arrays whose every element is a number
/// ([`NumberArray`](crate::vector::NumberArray) checks it), so the fallback
/// is never what a measure answers.
pub trait AsNumber {
	fn as_number(&self) -> &Number;
}

impl AsNumber for Number {
	#[inline(always)]
	fn as_number(&self) -> &Number {
		self
	}
}

/// The number a non-number [`Value`] reads as.
static NOT_A_NUMBER: Number = Number::Int(0);

impl AsNumber for Value {
	#[inline(always)]
	fn as_number(&self) -> &Number {
		if let Value::Number(n) = self {
			n
		} else {
			&NOT_A_NUMBER
		}
	}
}

/// The lanes [`lane_sum`] accumulates in.
const LANES: usize = 8;

/// The sum of the partial sums `p`, combined pairwise:
/// `((p₀ + p₄) + (p₁ + p₅)) + ((p₂ + p₆) + (p₃ + p₇))`.
#[inline(always)]
fn fold_lanes(p: [f64; LANES]) -> f64 {
	let [p0, p1, p2, p3, p4, p5, p6, p7] = p;
	((p0 + p4) + (p1 + p5)) + ((p2 + p6) + (p3 + p7))
}

/// `Σ term(aᵢ, bᵢ)` over two slices of equal length, from `-0.0` as a sum of
/// `f64`s ([`Iterator::sum`]) starts.
///
/// The term of index `i` is accumulated in partial sum `i mod LANES`, over the
/// whole chunks of [`LANES`] terms; the partial sums are combined by
/// [`fold_lanes`] and followed by the terms of the tail in order. The partial
/// sums carry no dependency on one another, which is what lets the loop run at
/// the throughput of the additions rather than their latency, and vectorise
/// over contiguous floats. The order is fixed, so a pair of slices always sums
/// to the same value.
///
/// A slice shorter than [`LANES`] is summed in order, so its answer is the
/// sequential sum's, to the last bit, an empty slice's included. A longer one is
/// summed in another order than the sequential sum, and the two differ by
/// rounding error of the order of `n · ε · Σ|termᵢ|`. That is small next to the
/// sum where its terms share a sign; where they cancel, the two can differ by
/// the whole of the sum, sign included.
///
/// A sum the lanes leave not finite is taken again in order from `-0.0`, and
/// that sum is the answer: finite terms that cancel across lanes can carry one
/// lane past the largest float while the sum in order stays finite. A sum in
/// order that passes the largest float partway, where the lanes stay finite, is
/// answered from the lanes.
#[inline]
pub fn lane_sum<T>(a: &[T], b: &[T], term: impl Fn(&T, &T) -> f64) -> f64 {
	match try_lane_sum(a, b, |x, y| Some(term(x, y))) {
		Some(sum) => sum,
		None => unreachable!("every term of a lane sum exists"),
	}
}

/// [`lane_sum`] over terms that may not exist: `None` as soon as `term` answers `None` for a
/// pair, so a caller that sums only pairs of one kind reads each pair once.
#[inline]
pub fn try_lane_sum<T>(a: &[T], b: &[T], term: impl Fn(&T, &T) -> Option<f64>) -> Option<f64> {
	let mut p = [-0.0f64; LANES];
	let (chunks_a, chunks_b) = (a.chunks_exact(LANES), b.chunks_exact(LANES));
	let (rest_a, rest_b) = (chunks_a.remainder(), chunks_b.remainder());
	for (x, y) in chunks_a.zip(chunks_b) {
		for lane in 0..LANES {
			p[lane] += term(&x[lane], &y[lane])?;
		}
	}
	let mut acc = fold_lanes(p);
	for (x, y) in rest_a.iter().zip(rest_b) {
		acc += term(x, y)?;
	}
	if acc.is_finite() {
		Some(acc)
	} else {
		in_order_sum(a, b, &term)
	}
}

/// `Σ term(aᵢ, bᵢ)` over two slices of equal length, summed in order from `-0.0`; `None` as
/// soon as `term` answers `None` for a pair.
fn in_order_sum<T>(a: &[T], b: &[T], term: impl Fn(&T, &T) -> Option<f64>) -> Option<f64> {
	a.iter().zip(b).try_fold(-0.0, |acc, (x, y)| Some(acc + term(x, y)?))
}

/// `(Σ aᵢbᵢ, Σ aᵢ², Σ bᵢ²)` over two slices of equal length, in one pass; `None` as soon as
/// `float` reads no float from an element.
///
/// The dot product is the one [`float_dot`] answers: accumulated as [`lane_sum`] accumulates its
/// terms, taken again in order where that leaves it not finite, and from the integer `0`
/// [`Number`] arithmetic sums from. Each norm is accumulated as [`lane_sum`] accumulates its
/// terms. A norm is a sum of squares, whose terms never cancel, so a lane past the largest float
/// leaves the sum in order past it too.
#[inline]
pub fn dot_and_norms<T>(
	a: &[T],
	b: &[T],
	float: impl Fn(&T) -> Option<f64>,
) -> Option<(f64, f64, f64)> {
	let mut dot = [-0.0f64; LANES];
	let mut na = [-0.0f64; LANES];
	let mut nb = [-0.0f64; LANES];
	let (chunks_a, chunks_b) = (a.chunks_exact(LANES), b.chunks_exact(LANES));
	let (rest_a, rest_b) = (chunks_a.remainder(), chunks_b.remainder());
	for (x, y) in chunks_a.zip(chunks_b) {
		for lane in 0..LANES {
			let (x, y) = (float(&x[lane])?, float(&y[lane])?);
			dot[lane] += x * y;
			na[lane] += x * x;
			nb[lane] += y * y;
		}
	}
	let (mut d, mut sa, mut sb) = (fold_lanes(dot), fold_lanes(na), fold_lanes(nb));
	for (x, y) in rest_a.iter().zip(rest_b) {
		let (x, y) = (float(x)?, float(y)?);
		d += x * y;
		sa += x * x;
		sb += y * y;
	}
	if !d.is_finite() {
		d = in_order_sum(a, b, |x, y| Some(float(x)? * float(y)?))?;
	}
	Some((from_integer_zero(d), sa, sb))
}

/// The sum `sum`, taken from `-0.0`, as a sum of the same terms from the integer `0` answers
/// it: the two differ only where every term is `-0.0`, which sums to `-0.0` from `-0.0` and to
/// `0.0` from `0`.
#[inline(always)]
fn from_integer_zero(sum: f64) -> f64 {
	sum + 0.0
}

/// The float a [`Number::Float`] holds; `None` for any other number.
#[inline(always)]
fn float_of<T: AsNumber>(n: &T) -> Option<f64> {
	if let Number::Float(f) = n.as_number() {
		Some(*f)
	} else {
		None
	}
}

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
		*a = a.clone().try_add(b.clone())?;
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
		self.iter().zip(other.iter()).map(|(a, b)| a.clone().try_add(b.clone())).collect()
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

impl<T: AsNumber> CosineDistance for [T] {
	fn cosine_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::cosine", self, other)?;
		Ok(Number::from(1) - cosine(self, other))
	}
}

/// The cosine similarity of two vectors of the same dimension.
///
/// Vectors of floats are read in one pass ([`dot_and_norms`]). A vector
/// holding any other number takes the dot product in [`Number`] arithmetic,
/// which keeps a product of integers exact.
fn cosine<T: AsNumber>(a: &[T], b: &[T]) -> Number {
	if !a.is_empty()
		&& let Some((d, na, nb)) = dot_and_norms(a, b, float_of)
	{
		return Number::Float(d / (na.sqrt() * nb.sqrt()));
	}
	let d = dot(a, b);
	d / (Number::from(magnitude_squared(a).sqrt()) * Number::from(magnitude_squared(b).sqrt()))
}

pub trait CosineSimilarity {
	fn cosine_similarity(&self, other: &Self) -> Result<Number>;
}

impl<T: AsNumber> CosineSimilarity for [T] {
	fn cosine_similarity(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::similarity::cosine", self, other)?;
		Ok(cosine(self, other))
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

impl<T: AsNumber> HammingDistance for [T] {
	fn hamming_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::hamming", self, other)?;
		Ok(self
			.iter()
			.zip(other.iter())
			.filter(|(a, b)| a.as_number() != b.as_number())
			.count()
			.into())
	}
}

pub trait JaccardSimilarity {
	fn jaccard_similarity(&self, other: &Self) -> Result<Number>;
}

impl<T: AsNumber> JaccardSimilarity for [T] {
	fn jaccard_similarity(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::similarity::jaccard", self, other)?;
		Ok(weighted_jaccard_similarity(
			self.iter()
				.zip(other.iter())
				.map(|(a, b)| (a.as_number().to_float(), b.as_number().to_float())),
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

impl<T: AsNumber> ManhattanDistance for [T] {
	fn manhattan_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::manhattan", self, other)?;
		// A sum of floats is a float whatever its terms' order; any other
		// number keeps the integer sum exact while it fits, and an empty
		// vector answers the empty integer sum.
		if !self.is_empty()
			&& let Some(sum) =
				try_lane_sum(self, other, |a, b| Some((float_of(a)? - float_of(b)?).abs()))
		{
			return Ok(sum.into());
		}
		Ok(self.iter().zip(other.iter()).fold(Number::Int(0), |acc, (a, b)| {
			let (a, b) = (a.as_number(), b.as_number());
			if let Some(d) = int_abs_diff(a, b) {
				match acc {
					Number::Int(acc) => {
						match i64::try_from(d).ok().and_then(|d| acc.checked_add(d)) {
							Some(sum) => Number::Int(sum),
							None => Number::Float(acc as f64 + d as f64),
						}
					}
					Number::Decimal(acc) => Number::Decimal(acc + rust_decimal::Decimal::from(d)),
					Number::BigDecimal(acc) => match acc.checked_add(&BigDecimal::from(d)) {
						Some(sum) => Number::BigDecimal(Box::new(sum)),
						None => Number::Float(acc.to_f64() + d as f64),
					},
					Number::Float(_)
					| Number::I8(_)
					| Number::U8(_)
					| Number::I16(_)
					| Number::U16(_)
					| Number::I32(_)
					| Number::U32(_)
					| Number::U64(_)
					| Number::F16(_)
					| Number::F32(_) => acc + Number::Float(d as f64),
				}
			} else {
				acc + (a.widened() - b.widened()).abs()
			}
		}))
	}
}

/// The exact absolute difference of two integers, which an `i64`
/// subtraction overflows and an `f64` subtraction rounds above 2^53; `None`
/// unless both are integers.
fn int_abs_diff(a: &Number, b: &Number) -> Option<u64> {
	match (a.wide(), b.wide()) {
		(Wide::Int(a), Wide::Int(b)) => Some(a.abs_diff(b)),
		(Wide::Int(_), Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_))
		| (
			Wide::Float(_),
			Wide::Int(_) | Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_),
		)
		| (
			Wide::Decimal(_) | Wide::BigDecimal(_),
			Wide::Int(_) | Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_),
		) => None,
	}
}

/// `|a − b|` as a float: exact for two integers, taken in floats otherwise.
#[inline(always)]
fn abs_diff(a: &Number, b: &Number) -> f64 {
	// A pair of `Float`s is answered before the per-element widening.
	if let (Number::Float(a), Number::Float(b)) = (a, b) {
		return (a - b).abs();
	}
	match (a.wide(), b.wide()) {
		(Wide::Float(a), Wide::Float(b)) => (a - b).abs(),
		(Wide::Int(a), Wide::Int(b)) => a.abs_diff(b) as f64,
		(Wide::Int(_), Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_))
		| (Wide::Float(_), Wide::Int(_) | Wide::Decimal(_) | Wide::BigDecimal(_))
		| (
			Wide::Decimal(_) | Wide::BigDecimal(_),
			Wide::Int(_) | Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_),
		) => (a.to_float() - b.to_float()).abs(),
	}
}

pub trait MinkowskiDistance {
	fn minkowski_distance(&self, other: &Self, order: &Number) -> Result<Number>;
}

impl<T: AsNumber> MinkowskiDistance for [T] {
	fn minkowski_distance(&self, other: &Self, order: &Number) -> Result<Number> {
		check_same_dimension("vector::distance::minkowski", self, other)?;
		let p = order.to_float();
		let dist = lane_sum(self, other, |a, b| abs_diff(a.as_number(), b.as_number()).powf(p));
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
		self.iter().zip(other.iter()).map(|(a, b)| a.clone().try_mul(b.clone())).collect()
	}
}

pub trait Scale {
	fn scale(&self, other: &Number) -> Result<Vec<Number>>;
}

impl Scale for Vec<Number> {
	fn scale(&self, other: &Number) -> Result<Vec<Number>> {
		self.iter().map(|a| a.clone().try_mul(other.clone())).collect()
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

impl<T: AsNumber> ChebyshevDistance for [T] {
	fn chebyshev_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::chebyshev", self, other)?;
		Ok(self
			.iter()
			.zip(other.iter())
			.map(|(a, b)| abs_diff(a.as_number(), b.as_number()))
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
		self.iter().zip(other.iter()).map(|(a, b)| a.clone().try_sub(b.clone())).collect()
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

impl<T: AsNumber> DotProduct for [T] {
	fn dot(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::dot", self, other)?;
		if let Some(d) = float_dot(self, other) {
			return Ok(Number::Float(d));
		}
		Ok(dot(self, other))
	}
}

/// The dot product of two vectors of floats of the same dimension, summed as
/// [`lane_sum`] sums and from the integer `0` that [`Number`] arithmetic sums
/// from; `None` where either holds any other number, and for empty vectors,
/// which [`Number`] arithmetic answers with the integer `0`.
pub fn float_dot<T: AsNumber>(a: &[T], b: &[T]) -> Option<f64> {
	if a.is_empty() {
		return None;
	}
	try_lane_sum(a, b, |x, y| Some(float_of(x)? * float_of(y)?)).map(from_integer_zero)
}

/// The dot product in [`Number`] arithmetic, which keeps a product of
/// integers exact.
fn dot<T: AsNumber>(a: &[T], b: &[T]) -> Number {
	a.iter().zip(b.iter()).map(|(a, b)| a.as_number() * b.as_number()).sum()
}

pub trait EuclideanDistance {
	/// Euclidean Distance between two vectors (L2 Norm)
	fn euclidean_distance(&self, other: &Self) -> Result<Number>;
}

impl<T: AsNumber> EuclideanDistance for [T] {
	fn euclidean_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::euclidean", self, other)?;
		Ok(lane_sum(self, other, |a, b| {
			let (a, b) = (a.as_number(), b.as_number());
			// A pair of `Float`s, the shape of every stored float vector, is answered before
			// widening: `wide()` is a match over every number kind, run for each element.
			if let (Number::Float(a), Number::Float(b)) = (a, b) {
				return (a - b).powi(2);
			}
			match (a.wide(), b.wide()) {
				(Wide::Float(a), Wide::Float(b)) => (a - b).powi(2),
				(Wide::Int(a), Wide::Int(b)) => (a.abs_diff(b) as f64).powi(2),
				(Wide::Int(_), Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_))
				| (Wide::Float(_), Wide::Int(_) | Wide::Decimal(_) | Wide::BigDecimal(_))
				| (
					Wide::Decimal(_) | Wide::BigDecimal(_),
					Wide::Int(_) | Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_),
				) => (a.widened() - b.widened()).to_float().powi(2),
			}
		})
		.sqrt()
		.into())
	}
}

fn magnitude_squared<T: AsNumber>(v: &[T]) -> f64 {
	v.iter().map(|a| a.as_number().to_float().powi(2)).sum::<f64>()
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

#[cfg(test)]
mod tests {
	use rust_decimal::Decimal;

	use super::*;

	#[test]
	fn manhattan_distance_preserves_decimal_precision_for_large_integer_diffs() {
		let a = [Number::Decimal(Decimal::new(5, 1)), Number::U64(u64::MAX)];
		let b = [Number::Decimal(Decimal::ZERO), Number::U64(0)];
		let dist = a.manhattan_distance(&b).unwrap();
		assert_eq!(dist, Number::Decimal(Decimal::new(5, 1) + Decimal::from(u64::MAX)));

		let a = [Number::U64(1)];
		let b = [Number::U64(u64::MAX)];
		assert_eq!(a.manhattan_distance(&b).unwrap(), Number::Decimal(Decimal::from(u64::MAX - 1)));
	}

	#[test]
	fn euclidean_distance_does_not_wrap_on_u64_underflow() {
		let a = [Number::U64(1)];
		let b = [Number::U64(2)];
		assert_eq!(a.euclidean_distance(&b).unwrap(), Number::Float(1.0));

		let a = [Number::U64(1)];
		let b = [Number::U64(u64::MAX)];
		assert_eq!(a.euclidean_distance(&b).unwrap(), Number::Float((u64::MAX - 1) as f64));
	}

	#[test]
	fn vector_elementwise_arithmetic_checks_narrow_overflow() {
		let max_u8 = vec![Number::U8(255)];
		let one_u8 = vec![Number::U8(1)];
		let two_u8 = vec![Number::U8(2)];
		let zero_u8 = vec![Number::U8(0)];

		assert!(max_u8.add(&one_u8).is_err());
		assert!(max_u8.multiply(&two_u8).is_err());
		assert!(max_u8.scale(&Number::U8(2)).is_err());
		assert!(zero_u8.subtract(&one_u8).is_err());

		let mut acc = vec![Number::U8(255)];
		assert!(add_assign("vector::add", &mut acc, &one_u8).is_err());
	}
}
