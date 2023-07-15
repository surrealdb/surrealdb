use crate::err::Error;
use crate::fnc::util::math::vector::{Add, Divide, DotProduct, Magnitude, Multiply, Subtract};
use crate::sql::{Number, Value};

pub fn add((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.add(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::add"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(v) => Ok(v.into()),
	}
}

pub fn divide((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.divide(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::divide"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(v) => Ok(v.into()),
	}
}

pub fn dotproduct((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.dotproduct(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::dotproduct"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(dot) => Ok(dot.into()),
	}
}

pub fn magnitude((a,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(a.magnitude().into())
}

pub fn multiply((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.multiply(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::multiply"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(v) => Ok(v.into()),
	}
}

pub fn subtract((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.subtract(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::subtract"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(v) => Ok(v.into()),
	}
}

pub fn normalize((_,): (Vec<Number>,)) -> Result<Value, Error> {
	Err(Error::FeatureNotYetImplemented {
		feature: "vector::normalize() function",
	})
}

pub fn project((_,): (Vec<Number>,)) -> Result<Value, Error> {
	Err(Error::FeatureNotYetImplemented {
		feature: "vector::project() function",
	})
}

pub fn crossproduct((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Err(Error::FeatureNotYetImplemented {
		feature: "vector::crossproduct() function",
	})
}

pub fn angle((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	Err(Error::FeatureNotYetImplemented {
		feature: "vector::angle() function",
	})
}

pub mod distance {

	use crate::err::Error;
	use crate::fnc::util::math::vector::EuclideanDistance;
	use crate::sql::{Number, Value};

	pub fn chebyshev((_, _): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "vector::distance::chebyshev() function",
		})
	}

	pub fn euclidean((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
		match a.euclidean_distance(&b) {
			None => Err(Error::InvalidArguments {
				name: String::from("vector::distance::euclidean"),
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
	use crate::fnc::util::math::vector::{DotProduct, Magnitude};
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
