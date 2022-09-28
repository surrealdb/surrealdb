use crate::sql::number::{Number, Sorted};

pub trait Median {
	fn median(self) -> Number;
}

impl Median for Sorted<&Vec<Number>> {
	fn median(self) -> Number {
		self.0.get(self.0.len() / 2).unwrap_or(&Number::NAN).clone()
	}
}
