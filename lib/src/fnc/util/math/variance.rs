use crate::sql::number::Number;

pub trait Variance {
	/// Population Variance of Data
	/// O(n) time complex
	fn variance(self) -> Number;
}

impl Variance for Vec<Number> {
	fn variance(self) -> Number {
		let mean = super::mean::Mean::mean(&self);
		let len = Number::from(self.len());
		let out = self.iter().map(|x| (x - &mean) * (x - &mean)).sum::<Number>() / len;

		out
	}
}
