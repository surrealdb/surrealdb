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

pub trait Divide {
	/// Division of two vectors
	fn divide(&self, other: &Self) -> Option<Vec<Number>>;
}

impl Divide for Vec<Number> {
	fn divide(&self, other: &Self) -> Option<Vec<Number>> {
		if self.len() != other.len() {
			return None;
		}
		Some(
			self.iter()
				.zip(other.iter())
				.map(|(a, b)| {
					if a.is_nan() || b.is_nan() || b.is_zero() {
						Number::NAN
					} else {
						a / b
					}
				})
				.collect(),
		)
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
				.map(|(a, b)| (a - b).pow(Number::Int(2)))
				.sum::<Number>()
				.sqrt(),
		)
	}
}

pub trait Magnitude {
	/// Calculate the magnitude of a vector
	fn magnitude(&self) -> Number;
}

impl Magnitude for Vec<Number> {
	fn magnitude(&self) -> Number {
		self.iter().map(|a| a.clone().pow(Number::Int(2))).sum::<Number>().sqrt()
	}
}
