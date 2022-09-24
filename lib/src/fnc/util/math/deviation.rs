use super::variance::Variance;
use crate::sql::number::Number;

pub trait Deviation {
	/// Population Standard Deviation
	fn deviation(self, sample: bool) -> Number;
}

impl Deviation for Vec<Number> {
	fn deviation(self, sample: bool) -> Number {
		self.variance(sample).sqrt()
	}
}
