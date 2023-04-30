use crate::sql::number::Number;

pub trait Mean {
	fn mean(&self) -> Number;
}

impl Mean for Vec<Number> {
	fn mean(&self) -> Number {
		match self.len() {
			0 => Number::NAN,
			_ => {
				let len = Number::from(self.len());
				let sum = self.iter().sum::<Number>();
				sum / len
			}
		}
	}
}
