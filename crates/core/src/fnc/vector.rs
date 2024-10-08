use crate::err::Error;
use crate::fnc::util::math::vector::{
	Add, Angle, CrossProduct, Divide, DotProduct, Magnitude, Multiply, Normalize, Project, Scale,
	Subtract,
};
use crate::sql::{Number, Value};

pub fn add((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Ok(a.add(&b)?.into())
}

pub fn angle((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Ok(a.angle(&b)?.into())
}

pub fn divide((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Ok(a.divide(&b)?.into())
}

pub fn cross((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Ok(a.cross(&b)?.into())
}

pub fn dot((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Ok(a.dot(&b)?.into())
}

pub fn magnitude((a,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(a.magnitude().into())
}

pub fn multiply((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Ok(a.multiply(&b)?.into())
}

pub fn normalize((a,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(a.normalize().into())
}

pub fn project((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Ok(a.project(&b)?.into())
}

pub fn subtract((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Ok(a.subtract(&b)?.into())
}

pub fn scale((a, b): (Vec<Number>, Number)) -> Result<Value, Error> {
	Ok(a.scale(&b)?.into())
}

pub mod distance {
	use crate::ctx::Context;
	use crate::doc::CursorDoc;
	use crate::err::Error;
	use crate::fnc::get_execution_context;
	use crate::fnc::util::math::vector::{
		ChebyshevDistance, EuclideanDistance, HammingDistance, ManhattanDistance, MinkowskiDistance,
	};
	use crate::idx::planner::IterationStage;
	use crate::sql::{Number, Value};

	pub fn chebyshev((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Ok(a.chebyshev_distance(&b)?.into())
	}

	pub fn euclidean((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Ok(a.euclidean_distance(&b)?.into())
	}

	pub fn hamming((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Ok(a.hamming_distance(&b)?.into())
	}

	pub fn knn(
		(ctx, doc): (&Context, Option<&CursorDoc>),
		(knn_ref,): (Option<Value>,),
	) -> Result<Value, Error> {
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

	pub fn mahalanobis((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::distance::mahalanobis() function".to_string(),
		})
	}

	pub fn manhattan((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Ok(a.manhattan_distance(&b)?.into())
	}

	pub fn minkowski((a, b, o): (Vec<Number>, Vec<Number>, Number)) -> Result<Value, Error> {
		Ok(a.minkowski_distance(&b, &o)?.into())
	}
}

pub mod similarity {

	use crate::err::Error;
	use crate::fnc::util::math::vector::{CosineSimilarity, JaccardSimilarity, PearsonSimilarity};
	use crate::sql::{Number, Value};

	pub fn cosine((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Ok(a.cosine_similarity(&b)?.into())
	}

	pub fn jaccard((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Ok(a.jaccard_similarity(&b)?.into())
	}

	pub fn pearson((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Ok(a.pearson_similarity(&b)?.into())
	}

	pub fn spearman((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::similarity::spearman() function".to_string(),
		})
	}
}

impl TryFrom<&Value> for Vec<Number> {
	type Error = Error;

	fn try_from(val: &Value) -> Result<Self, Self::Error> {
		if let Value::Array(a) = val {
			a.iter()
				.map(|v| v.try_into())
				.collect::<Result<Self, Error>>()
				.map_err(|e| Error::InvalidVectorValue(e.to_string()))
		} else {
			Err(Error::InvalidVectorValue(val.to_string()))
		}
	}
}

impl TryFrom<Value> for Vec<Number> {
	type Error = Error;

	fn try_from(val: Value) -> Result<Self, Self::Error> {
		if let Value::Array(a) = val {
			a.into_iter()
				.map(Value::try_into)
				.collect::<Result<Self, Error>>()
				.map_err(|e| Error::InvalidVectorValue(e.to_string()))
		} else {
			Err(Error::InvalidVectorValue(val.to_string()))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::Number;
	use rust_decimal::Decimal;

	#[test]
	fn vector_scale_int() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_int = Number::Int(2);

		let result: Result<Value, Error> = scale((input_vector.clone(), scalar_int));

		let expected_output: Vec<Number> = vec![2, 4, 6, 8].into_iter().map(Number::Int).collect();

		assert!(result.is_ok());
		assert_eq!(result.unwrap(), expected_output.into());
	}

	#[test]
	fn vector_scale_float() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_float = Number::Float(1.51);

		let result: Result<Value, Error> = scale((input_vector.clone(), scalar_float));
		let expected_output: Vec<Number> =
			vec![1.51, 3.02, 4.53, 6.04].into_iter().map(Number::Float).collect();
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), expected_output.into());
	}

	#[test]
	fn vector_scale_decimal() {
		let input_vector: Vec<Number> = vec![1, 2, 3, 4].into_iter().map(Number::Int).collect();
		let scalar_decimal = Number::Decimal(Decimal::new(3141, 3));

		let result: Result<Value, Error> = scale((input_vector.clone(), scalar_decimal));
		let expected_output: Vec<Number> = vec![
			Number::Decimal(Decimal::new(3141, 3)),  // 3.141 * 1
			Number::Decimal(Decimal::new(6282, 3)),  // 3.141 * 2
			Number::Decimal(Decimal::new(9423, 3)),  // 3.141 * 3
			Number::Decimal(Decimal::new(12564, 3)), // 3.141 * 4
		];
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), expected_output.into());
	}
}
