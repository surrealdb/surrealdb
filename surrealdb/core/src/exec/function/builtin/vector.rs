//! Vector functions
//!
//! Note: vector::distance::knn is not included as it requires execution context.

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// Single vector argument functions
define_pure_function!(VectorMagnitude, "vector::magnitude", (vector: Any) -> Float, crate::fnc::vector::magnitude);
define_pure_function!(VectorNormalize, "vector::normalize", (vector: Any) -> Any, crate::fnc::vector::normalize);

// Two vector argument functions
define_pure_function!(VectorAdd, "vector::add", (a: Any, b: Any) -> Any, crate::fnc::vector::add);
define_pure_function!(VectorAngle, "vector::angle", (a: Any, b: Any) -> Float, crate::fnc::vector::angle);
define_pure_function!(VectorCross, "vector::cross", (a: Any, b: Any) -> Any, crate::fnc::vector::cross);
define_pure_function!(VectorDivide, "vector::divide", (a: Any, b: Any) -> Any, crate::fnc::vector::divide);
define_pure_function!(VectorDot, "vector::dot", (a: Any, b: Any) -> Float, crate::fnc::vector::dot);
define_pure_function!(VectorMultiply, "vector::multiply", (a: Any, b: Any) -> Any, crate::fnc::vector::multiply);
define_pure_function!(VectorProject, "vector::project", (a: Any, b: Any) -> Any, crate::fnc::vector::project);
define_pure_function!(VectorSubtract, "vector::subtract", (a: Any, b: Any) -> Any, crate::fnc::vector::subtract);

// Vector with scalar
define_pure_function!(VectorScale, "vector::scale", (vector: Any, scalar: Number) -> Any, crate::fnc::vector::scale);

// Distance functions
define_pure_function!(VectorDistanceChebyshev, "vector::distance::chebyshev", (a: Any, b: Any) -> Float, crate::fnc::vector::distance::chebyshev);
define_pure_function!(VectorDistanceEuclidean, "vector::distance::euclidean", (a: Any, b: Any) -> Float, crate::fnc::vector::distance::euclidean);
define_pure_function!(VectorDistanceHamming, "vector::distance::hamming", (a: Any, b: Any) -> Float, crate::fnc::vector::distance::hamming);
define_pure_function!(VectorDistanceMahalanobis, "vector::distance::mahalanobis", (a: Any, b: Any, cov: Any) -> Float, crate::fnc::vector::distance::mahalanobis);
define_pure_function!(VectorDistanceManhattan, "vector::distance::manhattan", (a: Any, b: Any) -> Float, crate::fnc::vector::distance::manhattan);
define_pure_function!(VectorDistanceMinkowski, "vector::distance::minkowski", (a: Any, b: Any, p: Number) -> Float, crate::fnc::vector::distance::minkowski);

// Similarity functions
define_pure_function!(VectorSimilarityCosine, "vector::similarity::cosine", (a: Any, b: Any) -> Float, crate::fnc::vector::similarity::cosine);
define_pure_function!(VectorSimilarityJaccard, "vector::similarity::jaccard", (a: Any, b: Any) -> Float, crate::fnc::vector::similarity::jaccard);
define_pure_function!(VectorSimilarityPearson, "vector::similarity::pearson", (a: Any, b: Any) -> Float, crate::fnc::vector::similarity::pearson);
define_pure_function!(VectorSimilaritySpearman, "vector::similarity::spearman", (a: Any, b: Any) -> Float, crate::fnc::vector::similarity::spearman);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		VectorAdd,
		VectorAngle,
		VectorCross,
		VectorDistanceChebyshev,
		VectorDistanceEuclidean,
		VectorDistanceHamming,
		VectorDistanceMahalanobis,
		VectorDistanceManhattan,
		VectorDistanceMinkowski,
		VectorDivide,
		VectorDot,
		VectorMagnitude,
		VectorMultiply,
		VectorNormalize,
		VectorProject,
		VectorScale,
		VectorSimilarityCosine,
		VectorSimilarityJaccard,
		VectorSimilarityPearson,
		VectorSimilaritySpearman,
		VectorSubtract,
	);
}
