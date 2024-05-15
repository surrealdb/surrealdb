use crate::err::Error;
use crate::fnc::util::math::vector::{
	Add, Angle, CrossProduct, Divide, DotProduct, Magnitude, Multiply, Normalize, Project, Subtract,
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

pub mod distance {
	use crate::ctx::Context;
	use crate::dbs::Transaction;
	use crate::doc::CursorDoc;
	use crate::err::Error;
	use crate::fnc::get_execution_context;
	use crate::fnc::util::math::vector::{
		ChebyshevDistance, EuclideanDistance, HammingDistance, ManhattanDistance, MinkowskiDistance,
	};
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

	pub async fn knn(
		(ctx, txn, doc): (&Context<'_>, Option<&Transaction>, Option<&CursorDoc<'_>>),
		_: (),
	) -> Result<Value, Error> {
		if let Some((_txn, _exe, _doc, _thg)) = get_execution_context(ctx, txn, doc) {
			todo!()
		} else {
			Ok(Value::None)
		}
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
