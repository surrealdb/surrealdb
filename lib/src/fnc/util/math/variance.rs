use super::mean::Mean;
use crate::sql::number::Number;

pub trait Variance {
	/// Population Variance of Data
	/// O(n) time complex
	fn variance(self, sample: bool) -> f64;
}

impl Variance for Vec<Number> {
	fn variance(self, sample: bool) -> f64 {
		variance(&self, self.mean(), sample)
	}
}

pub(super) fn variance(v: &[Number], mean: f64, sample: bool) -> f64 {
	match v.len() {
		0 => f64::NAN,
		1 => 0.0,
		len => {
			let len = (len - sample as usize) as f64;
			let out = v.iter().map(|x| (x.to_float() - mean).powi(2)).sum::<f64>() / len;
			out
		}
	}
}
