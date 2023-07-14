use crate::sql::Number;

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
