use crate::sql::Number;

pub trait Magnitude {
	/// Calculate the magnitude of a vector
	fn magnitude(&self) -> Number;
}

impl Magnitude for Vec<Number> {
	fn magnitude(&self) -> Number {
		self.iter().map(|a| a.clone().pow(Number::Int(2))).sum::<Number>().sqrt()
	}
}
