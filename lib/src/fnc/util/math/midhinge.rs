use super::percentile::Percentile;
use crate::sql::number::{Number, Sorted};

pub trait Midhinge {
	/// Tukey Midhinge - the average of the 1st and 3rd Quartiles
	fn midhinge(&self) -> Number;
}

impl Midhinge for Sorted<&Vec<Number>> {
	fn midhinge(&self) -> Number {
		(self.percentile(Number::from(75)) + self.percentile(Number::from(25))) / Number::from(2)
	}
}
