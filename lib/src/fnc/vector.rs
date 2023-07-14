use crate::err::Error;
use crate::fnc::util::math::dotproduct::DotProduct;
use crate::fnc::util::math::magnitude::Magnitude;
use crate::sql::{Number, Value};

pub fn dotproduct((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.dotproduct(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::dot"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(dot) => Ok(dot.into()),
	}
}

pub fn magnitude((a,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(a.magnitude().into())
}

pub mod distance {

	use crate::err::Error;
	use crate::fnc::util::math::euclideandistance::EuclideanDistance;
	use crate::sql::{Number, Value};

	pub fn chebyshev((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::distance::chebyshev() function",
		})
	}

	pub fn euclidean((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		match a.euclidean_distance(&b) {
			None => Err(Error::InvalidArguments {
				name: String::from("vector::euclidean_distance"),
				message: String::from("The two vectors must be of the same length."),
			}),
			Some(distance) => Ok(distance.into()),
		}
	}

	pub fn hamming((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::distance::hamming() function",
		})
	}

	pub fn mahalanobis((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::distance::mahalanobis() function",
		})
	}

	pub fn manhattan((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::distance::manhattan() function",
		})
	}

	pub fn minkowski((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::distance::minkowski() function",
		})
	}
}

pub mod similarity {

	use crate::err::Error;
	use crate::fnc::util::math::dotproduct::DotProduct;
	use crate::fnc::util::math::magnitude::Magnitude;
	use crate::sql::{Number, Value};

	pub fn cosine((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		match a.dotproduct(&b) {
			None => Err(Error::InvalidArguments {
				name: String::from("vector::similarity::cosine"),
				message: String::from("The two vectors must be of the same length."),
			}),
			Some(dot) => Ok((dot / (a.magnitude() * b.magnitude())).into()),
		}
	}

	pub fn jaccard((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::similarity::jaccard() function",
		})
	}

	pub fn pearson((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::similarity::pearson() function",
		})
	}

	pub fn spearman((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::similarity::spearman() function",
		})
	}
}
