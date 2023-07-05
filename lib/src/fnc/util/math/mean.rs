use crate::sql::number::Number;

pub trait Mean {
	fn mean(&self) -> f64;
}

impl Mean for Vec<Number> {
	fn mean(&self) -> f64 {
		let len = self.len() as f64;
		let sum = self.iter().map(|n| n.to_float()).sum::<f64>();

		// Will be NaN if len is 0
		sum / len
	}
}
