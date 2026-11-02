//! Vector kernels: a query vector in an index's vector type, the score of a
//! stored vector against it, and the distance between two vectors of numbers.
//!
//! A vector index's exact tier reads the records it ranks and hands each
//! one's raw vector to [`score`], which converts it through the index's vector
//! type exactly as the graph search scores stored vectors, so reduced-precision
//! types (F32, F16, I8, …) rank identically across tiers. Nested arrays are
//! read flattened, in order, as the index flattens them when it stores them.

use anyhow::Result;
use surrealdb_runtime::util::math::vector::{
	ChebyshevDistance, CosineDistance, EuclideanDistance, HammingDistance, JaccardSimilarity,
	ManhattanDistance, MinkowskiDistance, PearsonSimilarity, check_same_dimension, lane_sum,
};

use crate::catalog::{Distance, VectorType};
use crate::trees::vector::{DistanceExt as _, SharedVector, Vector};
use crate::val::{Number, Value};

/// `pt` as a query vector of type `vector_type`, which fails unless it has
/// `dimension` elements: the conversion a graph search applies to its query.
pub fn query(vector_type: VectorType, dimension: usize, pt: &[Number]) -> Result<SharedVector> {
	let vector = Vector::try_from_vector(vector_type, pt)?;
	vector.check_dimension(dimension)?;
	Ok(vector.into())
}

/// The distance of a record's raw vector from the (already converted) query vector,
/// converting it through the index's vector type as the graph search does, so
/// reduced-precision index types (F32/F16/I8/...) rank identically across
/// tiers. A flat array of numbers is read in place; nested arrays are
/// flattened in order, as the index flattens them when it stores the vector.
/// `None` unless `raw` holds exactly `dimension` numbers representable in the
/// vector type: such a record could not have been indexed as it is either.
pub fn score(
	distance: &Distance,
	vector_type: VectorType,
	dimension: usize,
	query: &SharedVector,
	raw: &[Value],
) -> Option<f64> {
	if raw.len() == dimension {
		// A non-number stops the sequence short of the dimension; only an
		// array among them sends the record to be read as nested arrays below.
		let mut nested = false;
		let numbers = raw.iter().map_while(|v| {
			if let Value::Number(n) = v {
				Some(n.clone())
			} else if let Value::Array(_) = v {
				nested = true;
				None
			} else {
				None
			}
		});
		let record = Vector::try_from_numbers(vector_type, numbers);
		if !nested {
			// Flattening a record with no array reads the same numbers, so it
			// could only fail the same way.
			let record = record.ok().filter(|r| r.len() == dimension)?;
			return Some(distance.calculate_against(query, &record));
		}
	} else if !raw.iter().any(|v| matches!(v, Value::Array(_))) {
		// Without an array, flattening cannot change the length.
		return None;
	}
	let mut flat = Vec::with_capacity(dimension);
	if !flatten_numbers(raw, &mut flat, dimension) || flat.len() != dimension {
		return None;
	}
	let record = Vector::try_from_vector(vector_type, &flat).ok()?;
	Some(distance.calculate_against(query, &record))
}

/// Appends every number of `values` to `out`, descending into nested arrays in
/// order. `false` on any value that is neither a number nor an array, and as
/// soon as `out` would hold more than `limit` numbers.
fn flatten_numbers(values: &[Value], out: &mut Vec<Number>, limit: usize) -> bool {
	values.iter().all(|v| {
		if let Value::Number(n) = v {
			if out.len() < limit {
				out.push(n.clone());
				true
			} else {
				false
			}
		} else if let Value::Array(a) = v {
			flatten_numbers(&a.0, out, limit)
		} else {
			false
		}
	})
}

/// The `d` distance between two vectors of numbers, as a KNN ranks by it:
/// `JACCARD` and `PEARSON` answer `1 −` their similarity.
pub fn distance_compute(d: &Distance, v1: &[Number], v2: &[Number]) -> Result<Number> {
	match d {
		Distance::Cosine => v1.cosine_distance(v2),
		Distance::CosineNormalized => {
			check_same_dimension("vector::distance::cosine_normalized", v1, v2)?;
			Ok((1.0 - float_products(v1, v2)).into())
		}
		Distance::Chebyshev => v1.chebyshev_distance(v2),
		Distance::Euclidean => v1.euclidean_distance(v2),
		Distance::Hamming => v1.hamming_distance(v2),
		Distance::InnerProduct => {
			check_same_dimension("vector::distance::inner_product", v1, v2)?;
			Ok((-float_products(v1, v2)).into())
		}
		Distance::Jaccard => Ok((1.0 - v1.jaccard_similarity(v2)?.to_float()).into()),
		Distance::Manhattan => v1.manhattan_distance(v2),
		Distance::Minkowski(r) => v1.minkowski_distance(v2, r),
		Distance::Pearson => {
			// A vector of zero variance has no correlation with anything, so it
			// is at distance 1, as the index's own Pearson distance puts it,
			// rather than at the `NaN` the correlation's `0 / 0` would give.
			let r = v1.pearson_similarity(v2)?.to_float();
			Ok((1.0
				- if r.is_finite() {
					r
				} else {
					0.0
				})
			.into())
		}
	}
}

/// `Σ aᵢbᵢ` over two vectors of the same dimension, each product taken in
/// floats, summed as [`lane_sum`] sums.
fn float_products(v1: &[Number], v2: &[Number]) -> f64 {
	lane_sum(v1, v2, |a, b| a.to_float() * b.to_float())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn nums(values: &[i64]) -> Vec<Number> {
		values.iter().map(|&v| Number::Int(v)).collect()
	}

	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; this takes its operand by value like the arithmetic it feeds"
	)]
	fn float(n: Number) -> f64 {
		n.to_float()
	}

	#[test]
	fn distances_answer_their_metric() {
		let (a, b) = (nums(&[0, 0]), nums(&[3, 4]));
		assert_eq!(float(distance_compute(&Distance::Euclidean, &a, &b).unwrap()), 5.0);
		assert_eq!(float(distance_compute(&Distance::Manhattan, &a, &b).unwrap()), 7.0);
		assert_eq!(float(distance_compute(&Distance::Chebyshev, &a, &b).unwrap()), 4.0);
		let (x, y) = (nums(&[1, 0]), nums(&[0, 1]));
		assert_eq!(float(distance_compute(&Distance::Cosine, &x, &y).unwrap()), 1.0);
		assert_eq!(float(distance_compute(&Distance::Cosine, &x, &nums(&[2, 0])).unwrap()), 0.0);
		assert_eq!(float(distance_compute(&Distance::CosineNormalized, &x, &x).unwrap()), 0.0);
		assert_eq!(float(distance_compute(&Distance::InnerProduct, &a, &b).unwrap()), 0.0);
		assert_eq!(float(distance_compute(&Distance::Hamming, &x, &y).unwrap()), 2.0);
	}

	#[test]
	fn vectors_of_different_dimensions_are_refused() {
		let (a, b) = (nums(&[1, 2]), nums(&[1, 2, 3]));
		for d in [
			Distance::Euclidean,
			Distance::Manhattan,
			Distance::Chebyshev,
			Distance::Cosine,
			Distance::CosineNormalized,
			Distance::InnerProduct,
			Distance::Hamming,
		] {
			assert!(distance_compute(&d, &a, &b).is_err(), "{d:?}");
		}
	}

	#[test]
	fn zero_length_and_zero_magnitude_vectors_do_not_fail() {
		let empty = nums(&[]);
		assert_eq!(float(distance_compute(&Distance::Euclidean, &empty, &empty).unwrap()), 0.0);
		assert_eq!(float(distance_compute(&Distance::Manhattan, &empty, &empty).unwrap()), 0.0);
		// A cosine over a zero vector divides by a zero magnitude; it answers a
		// number rather than an error.
		assert!(distance_compute(&Distance::Cosine, &nums(&[0, 0]), &nums(&[1, 0])).is_ok());
	}

	#[test]
	fn a_query_has_the_index_dimension() {
		assert!(query(VectorType::F32, 2, &nums(&[1, 2])).is_ok());
		assert!(query(VectorType::F32, 3, &nums(&[1, 2])).is_err());
	}

	#[test]
	fn a_record_scores_only_when_it_could_have_been_indexed() {
		let values = |v: &[i64]| -> Vec<Value> { nums(v).into_iter().map(Value::Number).collect() };
		let q = query(VectorType::F64, 2, &nums(&[0, 0])).unwrap();
		let scored = score(&Distance::Euclidean, VectorType::F64, 2, &q, &values(&[3, 4]));
		assert_eq!(scored, Some(5.0));
		// The wrong dimension, or a value the vector type cannot hold.
		assert_eq!(score(&Distance::Euclidean, VectorType::F64, 2, &q, &values(&[3])), None);
		assert_eq!(score(&Distance::Euclidean, VectorType::U8, 2, &q, &values(&[-1, 300])), None);
	}
}
