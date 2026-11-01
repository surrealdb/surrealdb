//! Vector functions

use anyhow::Result;

use crate::define_pure_function;
use crate::exec::function::index::{IndexContext, IndexContextKind, IndexFunction};
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::exec::{ContextLevel, EvalContext};
use crate::expr::Kind;
use crate::val::Value;

// Single vector argument functions
define_pure_function!(VectorMagnitude, "vector::magnitude", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (vector: Any) -> Float, crate::fnc::vector::magnitude);
define_pure_function!(VectorNormalize, "vector::normalize", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (vector: Any) -> Any, crate::fnc::vector::normalize);

// Collection-of-vectors functions.
//
// `vector::sum` is also registered as an aggregate, so a GROUP BY query folds it
// across rows while a direct call folds the array it is given.
define_pure_function!(VectorSum, "vector::sum", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (vectors: Any) -> Any, crate::fnc::vector::sum);

// Two vector argument functions
define_pure_function!(VectorAdd, "vector::add", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Any, crate::fnc::vector::add);
define_pure_function!(VectorAngle, "vector::angle", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::angle);
define_pure_function!(VectorCross, "vector::cross", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, rule: ReturnRule::SameAsArg(0), (a: Any, b: Any) -> Any, crate::fnc::vector::cross);
define_pure_function!(VectorDivide, "vector::divide", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Any, crate::fnc::vector::divide);
define_pure_function!(VectorDot, "vector::dot", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::dot);
define_pure_function!(VectorMultiply, "vector::multiply", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Any, crate::fnc::vector::multiply);
define_pure_function!(VectorProject, "vector::project", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Any, crate::fnc::vector::project);
define_pure_function!(VectorSubtract, "vector::subtract", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Any, crate::fnc::vector::subtract);

// Vector with scalar
define_pure_function!(VectorScale, "vector::scale", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (vector: Any, scalar: Number) -> Any, crate::fnc::vector::scale);

// Distance functions
define_pure_function!(VectorDistanceChebyshev, "vector::distance::chebyshev", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::distance::chebyshev);
define_pure_function!(VectorDistanceCosine, "vector::distance::cosine", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::distance::cosine);
define_pure_function!(VectorDistanceEuclidean, "vector::distance::euclidean", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::distance::euclidean);
define_pure_function!(VectorDistanceHamming, "vector::distance::hamming", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::distance::hamming);
define_pure_function!(VectorDistanceMahalanobis, "vector::distance::mahalanobis", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any, cov: Any) -> Float, crate::fnc::vector::distance::mahalanobis);
define_pure_function!(VectorDistanceManhattan, "vector::distance::manhattan", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::distance::manhattan);
define_pure_function!(VectorDistanceMinkowski, "vector::distance::minkowski", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any, p: Number) -> Float, crate::fnc::vector::distance::minkowski);

// Similarity functions
define_pure_function!(VectorSimilarityCosine, "vector::similarity::cosine", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::similarity::cosine);
define_pure_function!(VectorSimilarityJaccard, "vector::similarity::jaccard", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::similarity::jaccard);
define_pure_function!(VectorSimilarityPearson, "vector::similarity::pearson", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::similarity::pearson);
define_pure_function!(VectorSimilaritySpearman, "vector::similarity::spearman", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::vector::similarity::spearman);

// =========================================================================
// vector::distance::knn - IndexFunction (reads distance from KNN context)
// =========================================================================

/// Returns the KNN distance for the current row, as computed by the KNN scan
/// operator (e.g., HNSW index search).
///
/// Usage: `SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt`
///
/// This is an [`IndexFunction`] with [`IndexContextKind::Knn`]. The distance
/// is populated by the KNN scan operator at execution time and looked up by
/// RecordId from the shared [`KnnContext`].
#[derive(Debug, Clone, Copy, Default)]
pub struct VectorDistanceKnn;

impl IndexFunction for VectorDistanceKnn {
	fn name(&self) -> &'static str {
		"vector::distance::knn"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Index, "vector::distance::knn", Fallibility::DATA)
	}

	fn signature(&self) -> Signature {
		Signature::new().optional("knn_ref", Kind::Number).returns(Kind::Any)
	}

	fn index_context_kind(&self) -> IndexContextKind {
		IndexContextKind::Knn
	}

	fn index_ref_arg_index(&self) -> Option<usize> {
		// First argument is the optional KNN reference number, extracted at plan time
		Some(0)
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		index_ctx: &'a IndexContext,
		_args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let knn_ctx = match index_ctx {
				IndexContext::Knn(ctx) => ctx,
				_ => {
					return Err(anyhow::anyhow!(
						"vector::distance::knn requires a KNN index context"
					));
				}
			};

			// Extract RecordId from the current row
			let rid = extract_record_id(ctx)?;

			// Look up the precomputed distance for this record
			match knn_ctx.get(&rid).await {
				Some(dist) => Ok(Value::Number(dist)),
				None => Ok(Value::None),
			}
		})
	}
}

/// Extract the RecordId from the current row value.
fn extract_record_id(ctx: &EvalContext<'_>) -> Result<crate::val::RecordId> {
	let current = ctx.value_in_scope().read()?.ok_or_else(|| {
		anyhow::anyhow!(
			"vector::distance::knn requires a current document (must be used in SELECT)"
		)
	})?;

	if let Value::Object(obj) = current {
		match obj.get("id") {
			Some(Value::RecordId(rid)) => Ok(rid.clone()),
			Some(_) => Err(anyhow::anyhow!("Current document 'id' field is not a RecordId")),
			None => Err(anyhow::anyhow!("Current document has no 'id' field")),
		}
	} else if let Value::RecordId(rid) = current {
		Ok(rid.clone())
	} else {
		Err(anyhow::anyhow!(
			"Expected current document to be an Object, got: {}",
			current.kind_of()
		))
	}
}

/// The `vector::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&VectorAdd,
	&VectorAngle,
	&VectorCross,
	&VectorDistanceChebyshev,
	&VectorDistanceCosine,
	&VectorDistanceEuclidean,
	&VectorDistanceHamming,
	&VectorDistanceMahalanobis,
	&VectorDistanceManhattan,
	&VectorDistanceMinkowski,
	&VectorDivide,
	&VectorDot,
	&VectorMagnitude,
	&VectorMultiply,
	&VectorNormalize,
	&VectorProject,
	&VectorScale,
	&VectorSimilarityCosine,
	&VectorSimilarityJaccard,
	&VectorSimilarityPearson,
	&VectorSimilaritySpearman,
	&VectorSubtract,
	&VectorSum,
];

pub fn register(registry: &mut FunctionRegistry) {
	// Index function (reads distance from KNN context)
	registry.register_index_function(VectorDistanceKnn);
}
