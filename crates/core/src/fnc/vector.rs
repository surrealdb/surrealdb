use anyhow::Result;

use crate::fnc::util::math::vector::{
	Add, Angle, CrossProduct, Divide, DotProduct, Magnitude, Multiply, Normalize, Project, Scale,
	Subtract,
};
use crate::val::{Number, Value};

pub fn add((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.add(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn angle((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.angle(&b)?.into())
}

pub fn divide((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.divide(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn cross((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.cross(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn dot((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.dot(&b)?.into())
}

pub fn magnitude((a,): (Vec<Number>,)) -> Result<Value> {
	Ok(a.magnitude().into())
}

pub fn multiply((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.multiply(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn normalize((a,): (Vec<Number>,)) -> Result<Value> {
	Ok(a.normalize().into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn project((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.project(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn subtract((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
	Ok(a.subtract(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub fn scale((a, b): (Vec<Number>, Number)) -> Result<Value> {
	Ok(a.scale(&b)?.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub mod distance {
	use anyhow::Result;

	use crate::ctx::Context;
	use crate::doc::CursorDoc;
	use crate::err::Error;
	use crate::fnc::args::Optional;
	use crate::fnc::get_execution_context;
	use crate::fnc::util::math::vector::{
		ChebyshevDistance, EuclideanDistance, HammingDistance, ManhattanDistance, MinkowskiDistance,
	};
	use crate::idx::planner::IterationStage;
	use crate::val::{Number, Value};

	pub fn chebyshev((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.chebyshev_distance(&b)?.into())
	}

	pub fn euclidean((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.euclidean_distance(&b)?.into())
	}

	pub fn hamming((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.hamming_distance(&b)?.into())
	}

	pub fn knn(
		(ctx, doc): (&Context, Option<&CursorDoc>),
		(Optional(knn_ref),): (Optional<Value>,),
	) -> Result<Value> {
		if let Some((_exe, doc, thg)) = get_execution_context(ctx, doc) {
			if let Some(ir) = &doc.ir {
				if let Some(d) = ir.dist() {
					return Ok(d.into());
				}
			}
			if let Some(IterationStage::Iterate(Some(results))) = ctx.get_iteration_stage() {
				let n = if let Some(Value::Number(n)) = knn_ref {
					n.as_usize()
				} else {
					0
				};
				if let Some(d) = results.get_dist(n, thg) {
					return Ok(d.into());
				}
			}
		}
		Ok(Value::None)
	}

	pub fn mahalanobis((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Err(anyhow::Error::new(Error::Unimplemented(
			"vector::distance::mahalanobis() function".to_string(),
		)))
	}

	pub fn manhattan((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.manhattan_distance(&b)?.into())
	}

	pub fn minkowski((a, b, o): (Vec<Number>, Vec<Number>, Number)) -> Result<Value> {
		Ok(a.minkowski_distance(&b, &o)?.into())
	}
}

pub mod similarity {

	use anyhow::Result;

	use crate::err::Error;
	use crate::fnc::util::math::vector::{CosineSimilarity, JaccardSimilarity, PearsonSimilarity};
	use crate::val::{Number, Value};

	pub fn cosine((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.cosine_similarity(&b)?.into())
	}

	pub fn jaccard((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.jaccard_similarity(&b)?.into())
	}

	pub fn pearson((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Ok(a.pearson_similarity(&b)?.into())
	}

	pub fn spearman((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value> {
		Err(anyhow::Error::new(Error::Unimplemented(
			"vector::similarity::spearman() function".to_string(),
		)))
	}
}

#[cfg(test)]
mod tests {
	use rust_decimal::Decimal;

	use super::*;
	use crate::val::Number;

	#[test]
	fn vector_scale_int() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_int = Number::Int(2);

		let result: Result<Value> = scale((input_vector.clone(), scalar_int));

		let expected_output: Vec<_> =
			vec![2, 4, 6, 8].into_iter().map(Number::Int).map(Value::from).collect();

		assert_eq!(result.unwrap(), Value::from(expected_output));
	}

	#[test]
	fn vector_scale_float() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_float = Number::Float(1.51);

		let result: Result<Value> = scale((input_vector.clone(), scalar_float));
		let expected_output = vec![1.51, 3.02, 4.53, 6.04]
			.into_iter()
			.map(Number::Float)
			.map(Value::from)
			.collect::<Value>();
		assert_eq!(result.unwrap(), expected_output);
	}

	#[test]
	fn vector_scale_decimal() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_decimal = Number::Decimal(Decimal::new(3141, 3));

		let result: Result<Value> = scale((input_vector.clone(), scalar_decimal));
		let expected_output: Vec<_> = vec![
			Value::Number(Number::Decimal(Decimal::new(3141, 3))), // 3.141 * 1
			Value::Number(Number::Decimal(Decimal::new(6282, 3))), // 3.141 * 2
			Value::Number(Number::Decimal(Decimal::new(9423, 3))), // 3.141 * 3
			Value::Number(Number::Decimal(Decimal::new(12564, 3))), // 3.141 * 4
		];
		assert_eq!(result.unwrap(), Value::from(expected_output));
	}
}
