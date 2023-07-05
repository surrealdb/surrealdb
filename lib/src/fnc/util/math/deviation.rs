use super::variance::Variance;
use crate::sql::number::Number;

pub trait Deviation {
	/// Population Standard Deviation
	fn deviation(self, sample: bool) -> f64;
}

impl Deviation for Vec<Number> {
	fn deviation(self, sample: bool) -> f64 {
		self.variance(sample).sqrt()
	}
}
