use std::collections::HashSet;

use anyhow::{Result, ensure};

use crate::err::Error;
use crate::fnc::util::math::deviation::deviation;
use crate::fnc::util::math::mean::Mean;
use crate::val::Number;

pub(crate) fn check_same_dimension<T>(fnc: &str, a: &[T], b: &[T]) -> Result<()> {
	ensure!(
		a.len() == b.len(),
		Error::InvalidArguments {
			name: String::from(fnc),
			message: String::from("The two vectors must be of the same dimension."),
		}
	);
	Ok(())
}

pub trait Add {
	/// Addition of two vectors
	fn add(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Add for Vec<Number> {
	fn add(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::add", self, other)?;
		Ok(self.iter().zip(other.iter()).map(|(a, b)| a + b).collect())
	}
}

pub trait Angle {
	/// Compute the angle between two vectors
	fn angle(&self, other: &Self) -> Result<Number>;
}

impl Angle for Vec<Number> {
	fn angle(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::angle", self, other)?;
		let dp = dot(self, other);
		let m = self.magnitude() * other.magnitude();
		let d = vector_div(&dp, &m);
		Ok(d.acos())
	}
}

pub trait CosineDistance {
	fn cosine_distance(&self, other: &Self) -> Result<Number>;
}

impl CosineDistance for Vec<Number> {
	fn cosine_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::cosine", self, other)?;
		let d = dot(self, other);
		Ok(Number::from(1) - d / (self.magnitude() * other.magnitude()))
	}
}

pub trait CosineSimilarity {
	fn cosine_similarity(&self, other: &Self) -> Result<Number>;
}

impl CosineSimilarity for Vec<Number> {
	fn cosine_similarity(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::similarity::cosine", self, other)?;
		let d = dot(self, other);
		Ok(d / (self.magnitude() * other.magnitude()))
	}
}

pub trait Divide {
	/// Division of two vectors
	fn divide(&self, other: &Self) -> Result<Vec<Number>>;
}

fn vector_div(a: &Number, b: &Number) -> Number {
	if a.is_nan() || b.is_nan() || b.is_zero() {
		Number::NAN
	} else {
		a / b
	}
}

impl Divide for Vec<Number> {
	fn divide(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::divide", self, other)?;
		Ok(self.iter().zip(other.iter()).map(|(a, b)| vector_div(a, b)).collect())
	}
}

pub trait HammingDistance {
	fn hamming_distance(&self, other: &Self) -> Result<Number>;
}

impl HammingDistance for Vec<Number> {
	fn hamming_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::hamming", self, other)?;
		Ok(self.iter().zip(other.iter()).filter(|(a, b)| a != b).count().into())
	}
}

pub trait JaccardSimilarity {
	fn jaccard_similarity(&self, other: &Self) -> Result<Number>;
}

impl JaccardSimilarity for Vec<Number> {
	fn jaccard_similarity(&self, other: &Self) -> Result<Number> {
		let mut union: HashSet<&Number> = HashSet::from_iter(self.iter());
		let intersection_size = other.iter().filter(|n| !union.insert(n)).count() as f64;
		Ok((intersection_size / union.len() as f64).into())
	}
}

pub trait PearsonSimilarity {
	fn pearson_similarity(&self, other: &Self) -> Result<Number>;
}

impl PearsonSimilarity for Vec<Number> {
	fn pearson_similarity(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::similarity::pearson", self, other)?;
		let m1 = self.mean();
		let m2 = other.mean();
		let covar: f64 = self
			.iter()
			.zip(other.iter())
			.map(|(x, y)| (x.to_float() - m1) * (y.to_float() - m2))
			.sum();
		let covar = covar / self.len() as f64;
		let std_dev1 = deviation(self, m1, false);
		let std_dev2 = deviation(other, m2, false);
		Ok((covar / (std_dev1 * std_dev2)).into())
	}
}

pub trait ManhattanDistance {
	fn manhattan_distance(&self, other: &Self) -> Result<Number>;
}

impl ManhattanDistance for Vec<Number> {
	fn manhattan_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::manhattan", self, other)?;
		Ok(self.iter().zip(other.iter()).map(|(a, b)| (a - b).abs()).sum())
	}
}

pub trait MinkowskiDistance {
	fn minkowski_distance(&self, other: &Self, order: &Number) -> Result<Number>;
}

impl MinkowskiDistance for Vec<Number> {
	fn minkowski_distance(&self, other: &Self, order: &Number) -> Result<Number> {
		check_same_dimension("vector::distance::minkowski", self, other)?;
		let p = order.to_float();
		let dist: f64 = self
			.iter()
			.zip(other.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).abs().powf(p))
			.sum();
		Ok(dist.powf(1.0 / p).into())
	}
}

pub trait Multiply {
	/// Multiplication of two vectors
	fn multiply(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Multiply for Vec<Number> {
	fn multiply(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::multiply", self, other)?;
		Ok(self.iter().zip(other.iter()).map(|(a, b)| a * b).collect())
	}
}

pub trait Scale {
	fn scale(&self, other: &Number) -> Result<Vec<Number>>;
}

impl Scale for Vec<Number> {
	fn scale(&self, other: &Number) -> Result<Vec<Number>> {
		Ok(self.iter().map(|a| a * other).collect())
	}
}

pub trait Project {
	/// Projection of two vectors
	fn project(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Project for Vec<Number> {
	fn project(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::project", self, other)?;
		let d = dot(self, other);
		let m = magnitude_squared(other).into();
		let s = vector_div(&d, &m);
		Ok(other.iter().map(|x| &s * x).collect())
	}
}

pub trait ChebyshevDistance {
	fn chebyshev_distance(&self, other: &Self) -> Result<Number>;
}

impl ChebyshevDistance for Vec<Number> {
	fn chebyshev_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::chebyshev", self, other)?;
		Ok(self
			.iter()
			.zip(other.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).abs())
			.fold(f64::MIN, f64::max)
			.into())
	}
}

pub trait Subtract {
	/// Subtraction of two vectors
	fn subtract(&self, other: &Self) -> Result<Vec<Number>>;
}

impl Subtract for Vec<Number> {
	fn subtract(&self, other: &Self) -> Result<Vec<Number>> {
		check_same_dimension("vector::subtract", self, other)?;
		Ok(self.iter().zip(other.iter()).map(|(a, b)| a - b).collect())
	}
}

pub trait CrossProduct {
	/// Cross product of two vectors
	fn cross(&self, other: &Self) -> Result<Vec<Number>>;
}

impl CrossProduct for Vec<Number> {
	fn cross(&self, other: &Self) -> Result<Vec<Number>> {
		ensure!(
			self.len() == 3 && other.len() == 3,
			Error::InvalidArguments {
				name: "vector::cross".to_string(),
				message: String::from("Both vectors must have a dimension of 3."),
			}
		);
		let a0 = &self[0];
		let a1 = &self[1];
		let a2 = &self[2];
		let b0 = &other[0];
		let b1 = &other[1];
		let b2 = &other[2];
		let v = vec![a1 * b2 - a2 * b1, a2 * b0 - a0 * b2, a0 * b1 - a1 * b0];
		Ok(v)
	}
}

pub trait DotProduct {
	/// Dot Product of two vectors
	fn dot(&self, other: &Self) -> Result<Number>;
}

impl DotProduct for Vec<Number> {
	fn dot(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::dot", self, other)?;
		Ok(dot(self, other))
	}
}

fn dot(a: &[Number], b: &[Number]) -> Number {
	a.iter().zip(b.iter()).map(|(a, b)| a * b).sum()
}

pub trait EuclideanDistance {
	/// Euclidean Distance between two vectors (L2 Norm)
	fn euclidean_distance(&self, other: &Self) -> Result<Number>;
}

impl EuclideanDistance for Vec<Number> {
	fn euclidean_distance(&self, other: &Self) -> Result<Number> {
		check_same_dimension("vector::distance::euclidean", self, other)?;
		Ok(self
			.iter()
			.zip(other.iter())
			.map(|(a, b)| (a - b).to_float().powi(2))
			.sum::<f64>()
			.sqrt()
			.into())
	}
}

fn magnitude_squared(v: &[Number]) -> f64 {
	v.iter().map(|a| a.to_float().powi(2)).sum::<f64>()
}

pub trait Magnitude {
	/// Calculate the magnitude of a vector
	fn magnitude(&self) -> Number;
}

impl Magnitude for Vec<Number> {
	fn magnitude(&self) -> Number {
		magnitude_squared(self).sqrt().into()
	}
}

pub trait Normalize {
	/// Normalize a vector
	fn normalize(&self) -> Vec<Number>;
}

impl Normalize for Vec<Number> {
	fn normalize(&self) -> Vec<Number> {
		let m = self.magnitude();
		// TODO: What about zero magnitude vectors?
		self.iter().map(|a| vector_div(a, &m)).collect()
	}
}
