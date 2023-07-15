use crate::sql::Number;

pub trait Add {
	/// Addition of two vectors
	fn add(&self, other: &Self) -> Option<Vec<Number>>;
}

impl Add for Vec<Number> {
	fn add(&self, other: &Self) -> Option<Vec<Number>> {
		if self.len() != other.len() {
			return None;
		}
		Some(self.iter().zip(other.iter()).map(|(a, b)| a + b).collect())
	}
}

pub trait Angle {
	/// Compute the angle between two vectors
	fn angle(&self, other: &Self) -> Option<Number>;
}

impl Angle for Vec<Number> {
	fn angle(&self, other: &Self) -> Option<Number> {
		self.dotproduct(other).map(|dp| {
			let m = self.magnitude() * other.magnitude();
			let d = vector_div(&dp, &m);
			d.acos()
		})
	}
}

pub trait Divide {
	/// Division of two vectors
	fn divide(&self, other: &Self) -> Option<Vec<Number>>;
}

fn vector_div(a: &Number, b: &Number) -> Number {
	if a.is_nan() || b.is_nan() || b.is_zero() {
		Number::NAN
	} else {
		a / b
	}
}

impl Divide for Vec<Number> {
	fn divide(&self, other: &Self) -> Option<Vec<Number>> {
		if self.len() != other.len() {
			return None;
		}
		Some(self.iter().zip(other.iter()).map(|(a, b)| vector_div(a, b)).collect())
	}
}

pub trait Multiply {
	/// Multiplication of two vectors
	fn multiply(&self, other: &Self) -> Option<Vec<Number>>;
}

impl Multiply for Vec<Number> {
	fn multiply(&self, other: &Self) -> Option<Vec<Number>> {
		if self.len() != other.len() {
			return None;
		}
		Some(self.iter().zip(other.iter()).map(|(a, b)| a * b).collect())
	}
}

pub trait Project {
	/// Projection of two vectors
	fn project(&self, other: &Self) -> Option<Vec<Number>>;
}

impl Project for Vec<Number> {
	fn project(&self, other: &Self) -> Option<Vec<Number>> {
		self.dotproduct(other).map(|d| {
			let m = magnitude_squared(other).into();
			let s = vector_div(&d, &m);
			other.iter().map(|x| &s * x).collect()
		})
	}
}

pub trait Subtract {
	/// Subtraction of two vectors
	fn subtract(&self, other: &Self) -> Option<Vec<Number>>;
}

impl Subtract for Vec<Number> {
	fn subtract(&self, other: &Self) -> Option<Vec<Number>> {
		if self.len() != other.len() {
			return None;
		}
		Some(self.iter().zip(other.iter()).map(|(a, b)| a - b).collect())
	}
}

pub trait CrossProduct {
	/// Cross product of two vectors
	fn crossproduct(&self, other: &Self) -> Option<Vec<Number>>;
}

impl CrossProduct for Vec<Number> {
	fn crossproduct(&self, other: &Self) -> Option<Vec<Number>> {
		if self.len() != 3 || other.len() != 3 {
			return None;
		}
		let a0 = &self[0];
		let a1 = &self[1];
		let a2 = &self[2];
		let b0 = &other[0];
		let b1 = &other[1];
		let b2 = &other[2];
		let v = vec![a1 * b2 - a2 * b1, a2 * b0 - a0 * b2, a0 * b1 - a1 * b0];
		Some(v)
	}
}

pub trait DotProduct {
	/// Dot Product of two vectors
	fn dotproduct(&self, other: &Self) -> Option<Number>;
}

impl DotProduct for Vec<Number> {
	fn dotproduct(&self, other: &Self) -> Option<Number> {
		if self.len() != other.len() {
			return None;
		}
		Some(self.iter().zip(other.iter()).map(|(a, b)| a * b).sum())
	}
}

pub trait EuclideanDistance {
	/// Euclidean Distance between two vectors (L2 Norm)
	fn euclidean_distance(&self, other: &Self) -> Option<Number>;
}

impl EuclideanDistance for Vec<Number> {
	fn euclidean_distance(&self, other: &Self) -> Option<Number> {
		if self.len() != other.len() {
			return None;
		}
		Some(
			self.iter()
				.zip(other.iter())
				.map(|(a, b)| (a - b).to_float().powi(2))
				.sum::<f64>()
				.sqrt()
				.into(),
		)
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
		self.iter().map(|a| vector_div(a, &m)).collect()
	}
}
