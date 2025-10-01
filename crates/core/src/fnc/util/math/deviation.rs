use crate::fnc::util::math::ToFloat;
use crate::fnc::util::math::mean::Mean;
use crate::fnc::util::math::variance::variance;
use crate::val::Number;

pub trait Deviation {
	/// Population Standard Deviation
	fn deviation(self, sample: bool) -> f64;
}

impl Deviation for Vec<Number> {
	fn deviation(self, sample: bool) -> f64 {
		deviation(&self, self.mean(), sample)
	}
}

// This function is exposed to optimise the pearson distance calculation.
// As the mean of the vector is already calculated, we pass it as a parameter
// rather than recalculating it.
pub(crate) fn deviation<T>(v: &[T], mean: f64, sample: bool) -> f64
where
	T: ToFloat,
{
	variance(v, mean, sample).sqrt()
}
