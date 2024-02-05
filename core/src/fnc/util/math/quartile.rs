use super::percentile::Percentile;
use crate::sql::number::{Number, Sorted};

pub trait Quartile {
	/// Divides the set of numbers into Q_0 (min), Q_1, Q_2, Q_3, and Q_4 (max)
	fn quartile(self) -> (f64, f64, f64, f64, f64);
}

impl Quartile for Sorted<&Vec<Number>> {
	fn quartile(self) -> (f64, f64, f64, f64, f64) {
		(
			self.percentile(Number::from(0)),
			self.percentile(Number::from(25)),
			self.percentile(Number::from(50)),
			self.percentile(Number::from(75)),
			self.percentile(Number::from(100)),
		)
	}
}
