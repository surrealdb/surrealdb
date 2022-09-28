use super::percentile::Percentile;
use crate::sql::number::{Number, Sorted};

pub trait Interquartile {
	/// Interquartile Range - the difference between the upper and lower quartiles
	/// Q_3 - Q_1 [ or P_75 - P-25 ]
	fn interquartile(self) -> Number;
}

impl Interquartile for Sorted<&Vec<Number>> {
	fn interquartile(self) -> Number {
		self.percentile(Number::from(75)) - self.percentile(Number::from(25))
	}
}
