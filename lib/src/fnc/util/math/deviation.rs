use crate::fnc::util::math::mean::Mean;
use crate::fnc::util::math::variance::variance;
use crate::sql::number::Number;

pub trait Deviation {
	/// Population Standard Deviation
	fn deviation(self, sample: bool) -> f64;
}

impl Deviation for Vec<Number> {
	fn deviation(self, sample: bool) -> f64 {
		deviation(&self, self.mean(), sample)
	}
}

pub(super) fn deviation(v: &[Number], mean: f64, sample: bool) -> f64 {
	variance(v, mean, sample).sqrt()
}
