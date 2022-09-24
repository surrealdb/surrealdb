use crate::sql::number::{Number,Sorted};
use super::percentile::Percentile;

pub trait Interquartile {
	/// the interquartile Range: Q_3 - Q_1 [ or P_75 - P-25 ]
	fn interquartile(self) -> Number;
}

impl Interquartile for Sorted<&Vec<Number>> {
	fn interquartile(self) -> Number {
		self.percentile(Number::from(75)) - self.percentile(Number::from(25))
	}
}
