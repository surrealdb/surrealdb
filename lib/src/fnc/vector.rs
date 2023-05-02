use crate::err::Error;
use crate::fnc::util::math::dotproduct::DotProduct;
use crate::fnc::util::math::euclideandistance::EuclideanDistance;
use crate::fnc::util::math::magnitude::Magnitude;
use crate::sql::{Number, Value};

pub fn cosine_similarity((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.dot(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::cosine_similarity"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(dot) => Ok((dot / (a.magnitude() * b.magnitude())).into()),
	}
}

pub fn dot((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.dot(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::dot"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(dot) => Ok(dot.into()),
	}
}

pub fn euclidean_distance((a, b): (Vec<Number>, Vec<Number>)) -> Result<Value, Error> {
	match a.euclidean_distance(&b) {
		None => Err(Error::InvalidArguments {
			name: String::from("vector::euclidean_distance"),
			message: String::from("The two vectors must be of the same length."),
		}),
		Some(distance) => Ok(distance.into()),
	}
}

pub fn magnitude((a,): (Vec<Number>,)) -> Result<Value, Error> {
	Ok(a.magnitude().into())
}
