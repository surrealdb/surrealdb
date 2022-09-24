use crate::sql::number::Number;

pub trait Deviation {
	/// Population Standard Deviation
	fn deviation(self) -> Number;
}

impl Deviation for Vec<Number> {
	fn deviation(self) -> Number {
		super::variance::Variance::variance(self).sqrt()
	}
}
