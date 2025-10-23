use crate::fnc::util::math::ToFloat;

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
