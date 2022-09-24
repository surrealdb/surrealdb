use super::mean::Mean;
use crate::sql::number::Number;

pub trait Variance {
	/// Population Variance of Data
	/// O(n) time complex
	fn variance(self, sample: bool) -> Number;
}

impl Variance for Vec<Number> {
	fn variance(self, sample: bool) -> Number {
		let mean = self.mean();
		let len = Number::from(self.len() - sample as usize);
		let out = self.iter().map(|x| (x - &mean) * (x - &mean)).sum::<Number>() / len;

		out
	}
}
