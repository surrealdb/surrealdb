use super::mean::Mean;
use crate::fnc::util::math::ToFloat;
use crate::val::Number;

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

// This function is exposed to optimise the pearson distance calculation.
// As the mean of the vector is already calculated, we pass it as a parameter
// rather than recalculating it.
pub(super) fn variance<T>(v: &[T], mean: f64, sample: bool) -> f64
where
	T: ToFloat,
{
	match v.len() {
		0 => f64::NAN,
		1 => 0.0,
		len => {
			let len = (len - sample as usize) as f64;
			v.iter().map(|x| (x.to_float() - mean).powi(2)).sum::<f64>() / len
		}
	}
}
