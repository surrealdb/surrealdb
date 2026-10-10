//! Vector kernels: a query vector in an index's vector type, the score of a
//! stored vector against it, and the distance between two vectors of numbers.
//!
//! A vector index's exact tier reads the records it ranks and hands each
//! one's raw vector to [`score`], which converts it through the index's vector
//! type exactly as the graph search scores stored vectors, so reduced-precision
//! types (F32, F16, I8, …) rank identically across tiers.

use anyhow::Result;
use surrealdb_runtime::util::math::ToFloat;
use surrealdb_runtime::util::math::vector::{
	ChebyshevDistance, CosineDistance, EuclideanDistance, HammingDistance, JaccardSimilarity,
	ManhattanDistance, MinkowskiDistance, PearsonSimilarity, check_same_dimension,
};

use crate::catalog::{Distance, VectorType};
use crate::trees::vector::{DistanceExt as _, Vector};
use crate::val::Number;

/// `pt` as a query vector of type `vector_type`, which fails unless it has
/// `dimension` elements: the conversion a graph search applies to its query.
pub fn query(vector_type: VectorType, dimension: usize, pt: &[Number]) -> Result<Vector> {
	Vector::check_expected_dimension(pt.len(), dimension)?;
	Vector::try_from_vector(vector_type, pt)
}

/// The `distance` of a record's `raw` vector, converted through
/// `vector_type`, from `query`. `None` when the raw values cannot be
/// represented in the vector type or the dimension is not `dimension`: such a
/// record could not have been indexed as it is either.
pub fn score(
	distance: &Distance,
	vector_type: VectorType,
	dimension: usize,
	query: &Vector,
	raw: &[Number],
) -> Option<f64> {
	// The dimension is the slice's length, so a record of another dimension is
	// refused before it is converted.
	if raw.len() != dimension {
		return None;
	}
	let record = Vector::try_from_vector(vector_type, raw).ok()?;
	Some(distance.calculate(&record, query))
}

/// The `d` distance between two vectors of numbers, as the
/// `vector::distance::*` functions compute it.
pub fn distance_compute(d: &Distance, v1: &[Number], v2: &[Number]) -> Result<Number> {
	match d {
		Distance::Cosine => v1.cosine_distance(v2),
		Distance::CosineNormalized => {
			check_same_dimension("vector::distance::cosine_normalized", v1, v2)?;
			Ok((1.0
				- v1.iter().zip(v2.iter()).map(|(a, b)| a.to_float() * b.to_float()).sum::<f64>())
			.into())
		}
		Distance::Chebyshev => v1.chebyshev_distance(v2),
		Distance::Euclidean => v1.euclidean_distance(v2),
		Distance::Hamming => v1.hamming_distance(v2),
		Distance::InnerProduct => {
			check_same_dimension("vector::distance::inner_product", v1, v2)?;
			Ok((-v1.iter().zip(v2.iter()).map(|(a, b)| a.to_float() * b.to_float()).sum::<f64>())
				.into())
		}
		Distance::Jaccard => v1.jaccard_similarity(v2),
		Distance::Manhattan => v1.manhattan_distance(v2),
		Distance::Minkowski(r) => v1.minkowski_distance(v2, r),
		Distance::Pearson => v1.pearson_similarity(v2),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn nums(values: &[i64]) -> Vec<Number> {
		values.iter().map(|&v| Number::Int(v)).collect()
	}

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
		let q = query(VectorType::F64, 2, &nums(&[0, 0])).unwrap();
		let scored = score(&Distance::Euclidean, VectorType::F64, 2, &q, &nums(&[3, 4]));
		assert_eq!(scored, Some(5.0));
		// The wrong dimension, or a value the vector type cannot hold.
		assert_eq!(score(&Distance::Euclidean, VectorType::F64, 2, &q, &nums(&[3])), None);
		assert_eq!(score(&Distance::Euclidean, VectorType::U8, 2, &q, &nums(&[-1, 300])), None);
	}
}
