use super::mean::Mean;
use crate::sql::number::Number;

pub trait Variance {
	/// Population Variance of Data
	/// O(n) time complex
	fn variance(self, sample: bool) -> f64;
}

impl Variance for Vec<Number> {
	fn variance(self, sample: bool) -> f64 {
		match self.len() {
			0 => f64::NAN,
			1 => 0.0,
			len => {
				let mean = self.mean();
				let len = (len - sample as usize) as f64;
				let out = self.iter().map(|x| (x.to_float() - mean).powi(2)).sum::<f64>() / len;
				out
			}
		}
	}
}
