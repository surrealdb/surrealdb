use crate::val::Number;
use crate::val::number::Sorted;

pub trait Percentile {
	/// Gets the N percentile, averaging neighboring records if non-exact
	fn percentile(&self, perc: Number) -> f64;
}

impl Percentile for Sorted<&Vec<Number>> {
	fn percentile(&self, perc: Number) -> f64 {
		// If an empty set, then return NaN
		if self.0.is_empty() {
			return f64::NAN;
		}
		// If an invalid percentile, then return NaN
		let perc = perc.to_float();
		if !(0.0..=100.0).contains(&perc) {
			return f64::NAN;
		}
		// Get the index of the specified percentile
		let fract_index = (self.0.len() - 1) as f64 * perc * (1.0 / 100.0);
		let floor = self.0[fract_index.floor() as usize].to_float();
		let fract = fract_index.fract();

		if fract.abs() <= f64::EPSILON {
			floor
		} else {
			let ceil = self.0[fract_index.ceil() as usize].to_float();
			floor + (ceil - floor) * fract
		}
	}
}
