use crate::sql::number::Number;

pub trait Mean {
	fn mean(&self) -> Number;
}

impl Mean for Vec<Number> {
	fn mean(&self) -> Number {
		if self.is_empty() {
			Number::NAN
		} else {
			let len = Number::from(self.len());
			let sum = self.iter().sum::<Number>();
			sum / len
		}
	}
}
