use crate::sql::Number;

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
