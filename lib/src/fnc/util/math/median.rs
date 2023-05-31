use crate::sql::number::{Number, Sorted};

pub trait Median {
	fn median(self) -> f64;
}

impl Median for Sorted<&Vec<Number>> {
	fn median(self) -> f64 {
		if self.0.is_empty() {
			f64::NAN
		} else if self.0.len() % 2 == 1 {
			// return the middle: _ _ X _ _
			self.0[self.0.len() / 2].to_float()
		} else {
			// return the average of the middles: _ _ X Y _ _
			(self.0[self.0.len() / 2].to_float() + self.0[self.0.len() / 2 + 1].to_float()) / 2.0
		}
	}
}
